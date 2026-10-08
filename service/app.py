"""
Model serving API.

The model is baked into the image at /opt/model during the build, so this pod
has no runtime dependency on MLflow - it starts even if MLflow is down.

Endpoints:
  GET  /health   liveness/readiness
  GET  /metrics  Prometheus scrape target
  GET  /info     which model and version this pod is serving
  POST /predict  single or batch inference
"""

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from typing import Any

import mlflow.pyfunc
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, Field

MODEL_PATH = os.getenv("MODEL_PATH", "/opt/model")
MODEL_NAME = os.getenv("MODEL_NAME", "unknown")
MODEL_VERSION = os.getenv("MODEL_VERSION", "unknown")

# --------------------------------------------------------------------------
# Prometheus metrics - scraped by the ServiceMonitor the chart installs
# --------------------------------------------------------------------------
PREDICTIONS = Counter(
    "model_predictions_total",
    "Predictions served",
    ["model_name", "model_version", "predicted_class"],
)
LATENCY = Histogram(
    "model_prediction_latency_seconds",
    "Time spent in predict()",
    ["model_name", "model_version"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)
ERRORS = Counter(
    "model_prediction_errors_total",
    "Failed prediction requests",
    ["model_name", "model_version", "reason"],
)
MODEL_INFO = Gauge(
    "model_info",
    "Always 1; the labels carry which model this pod serves",
    ["model_name", "model_version"],
)
ROWS = Histogram(
    "model_request_rows",
    "Rows per request",
    ["model_name", "model_version"],
    buckets=(1, 2, 5, 10, 50, 100, 500, 1000),
)

_model: Any = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Load the model once at startup, not per request."""
    global _model
    print(f"Loading {MODEL_NAME} v{MODEL_VERSION} from {MODEL_PATH}")
    _model = mlflow.pyfunc.load_model(MODEL_PATH)
    MODEL_INFO.labels(MODEL_NAME, MODEL_VERSION).set(1)
    print("Model ready")
    yield
    print("Shutting down")


app = FastAPI(
    title="Model Service",
    description=f"Serving {MODEL_NAME} version {MODEL_VERSION}",
    version=MODEL_VERSION,
    lifespan=lifespan,
)


class PredictRequest(BaseModel):
    """One or more rows of features. Column names must match the training data."""

    rows: list[dict[str, Any]] = Field(
        ...,
        min_length=1,
        examples=[
            [
                {
                    "tenure_months": 5,
                    "monthly_charges": 89.5,
                    "support_calls": 3,
                    "contract": "month-to-month",
                    "internet_service": "fiber",
                    "payment_method": "e-check",
                }
            ]
        ],
    )


class PredictResponse(BaseModel):
    model_name: str
    model_version: str
    predictions: list[Any]
    n_rows: int
    latency_ms: float


@app.get("/health")
def health():
    if _model is None:
        raise HTTPException(status_code=503, detail="model not loaded")
    return {"status": "ok", "model": MODEL_NAME, "version": MODEL_VERSION}


@app.get("/info")
def info():
    return {
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "model_path": MODEL_PATH,
        "loaded": _model is not None,
    }


@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest):
    if _model is None:
        ERRORS.labels(MODEL_NAME, MODEL_VERSION, "model_not_loaded").inc()
        raise HTTPException(status_code=503, detail="model not loaded")

    started = time.perf_counter()
    try:
        frame = pd.DataFrame(request.rows)
        ROWS.labels(MODEL_NAME, MODEL_VERSION).observe(len(frame))

        with LATENCY.labels(MODEL_NAME, MODEL_VERSION).time():
            raw = _model.predict(frame)

        predictions = raw.tolist() if hasattr(raw, "tolist") else list(raw)
        for value in predictions:
            PREDICTIONS.labels(MODEL_NAME, MODEL_VERSION, str(value)).inc()

    except Exception as exc:
        ERRORS.labels(MODEL_NAME, MODEL_VERSION, type(exc).__name__).inc()
        # The message can expose schema details, so keep it short.
        raise HTTPException(status_code=400, detail=f"prediction failed: {exc}") from exc

    return PredictResponse(
        model_name=MODEL_NAME,
        model_version=MODEL_VERSION,
        predictions=predictions,
        n_rows=len(frame),
        latency_ms=round((time.perf_counter() - started) * 1000, 2),
    )
