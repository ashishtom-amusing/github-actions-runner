"""
Model inference service.

The model is baked into the image at MODEL_PATH by the CI pipeline, so the
container has no runtime dependency on MLflow.

Roles
-----
ROLE=champion    serves live traffic. If a challenger is deployed beside it,
                 every request is also mirrored to the challenger in the
                 background (shadow deployment). The client only ever sees the
                 champion's answer.
ROLE=challenger  receives mirrored traffic only. Logs its predictions under the
                 same request id so the two can be compared row for row.

Endpoints
---------
GET  /health    liveness and readiness
GET  /info      what is loaded, logging and shadow status
POST /predict   inference
GET  /metrics   Prometheus
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any

import pandas as pd
from fastapi import FastAPI, Header, HTTPException
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, Field
from starlette.responses import Response

from prediction_logger import PredictionLogger, utc_now_iso

MODEL_PATH = os.getenv("MODEL_PATH", "/opt/model")
MODEL_NAME = os.getenv("MODEL_NAME", "unknown")
MODEL_VERSION = os.getenv("MODEL_VERSION", "unknown")
ROLE = os.getenv("ROLE", "champion")
NAMESPACE = os.getenv("POD_NAMESPACE", "ml-apps")

SHADOW_ENABLED = os.getenv("SHADOW_ENABLED", "true").lower() == "true" and ROLE == "champion"
SHADOW_URL = os.getenv(
    "SHADOW_TARGET_URL",
    f"http://{MODEL_NAME}-challenger.{NAMESPACE}.svc.cluster.local:8000",
)
SHADOW_PROBE_SECONDS = float(os.getenv("SHADOW_PROBE_SECONDS", "30"))
SHADOW_TIMEOUT = float(os.getenv("SHADOW_TIMEOUT_SECONDS", "2"))
SHADOW_MAX_INFLIGHT = int(os.getenv("SHADOW_MAX_INFLIGHT", "32"))

LABELS = {"model": MODEL_NAME, "version": MODEL_VERSION, "role": ROLE}

# ---------------------------------------------------------------------------
# Metrics. Names match the Grafana dashboard.
# ---------------------------------------------------------------------------
MODEL_INFO = Gauge("model_info", "Loaded model, as labels", ["model", "version", "role"])
PREDICTIONS = Counter(
    "model_predictions_total", "Rows scored", ["model", "version", "role", "outcome"]
)
ERRORS = Counter("model_prediction_errors_total", "Failed requests", ["model", "version", "role"])
LATENCY = Histogram(
    "model_prediction_latency_seconds",
    "Inference latency per request",
    ["model", "version", "role"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)
POSITIVE_RATE = Gauge(
    "model_positive_prediction_rate",
    "Share of positive predictions in the most recent request",
    ["model", "version", "role"],
)
SHADOW_REQUESTS = Counter(
    "shadow_requests_total", "Requests mirrored to the challenger", ["model", "result"]
)
SHADOW_ACTIVE = Gauge(
    "shadow_target_active", "1 when a healthy challenger is receiving mirrored traffic", ["model"]
)


# ---------------------------------------------------------------------------
# Model loading
# ---------------------------------------------------------------------------
class LoadedModel:
    """Prefers the native sklearn flavour so probabilities are available."""

    def __init__(self, path: str) -> None:
        self.flavor = "unknown"
        self.native = None
        self.pyfunc = None
        try:
            import mlflow.sklearn

            self.native = mlflow.sklearn.load_model(path)
            self.flavor = "sklearn"
        except Exception as exc:  # noqa: BLE001
            print(f"sklearn flavour unavailable ({exc}); falling back to pyfunc")
            import mlflow.pyfunc

            self.pyfunc = mlflow.pyfunc.load_model(path)
            self.flavor = "pyfunc"

    def predict(self, frame: pd.DataFrame) -> tuple[list[Any], list[float] | None]:
        if self.native is not None:
            predictions = self.native.predict(frame).tolist()
            probabilities = None
            if hasattr(self.native, "predict_proba"):
                try:
                    probabilities = self.native.predict_proba(frame)[:, 1].round(6).tolist()
                except Exception:  # noqa: BLE001 - probabilities are optional
                    probabilities = None
            return predictions, probabilities
        raw = self.pyfunc.predict(frame)
        predictions = raw.tolist() if hasattr(raw, "tolist") else list(raw)
        return predictions, None


_state: dict[str, Any] = {}


# ---------------------------------------------------------------------------
# Shadow traffic
# ---------------------------------------------------------------------------
class ShadowMirror:
    """
    Fire-and-forget mirroring to the challenger.

    A background probe checks the challenger's /health every 30s, so deploying
    or removing a challenger takes effect without touching the champion.
    In-flight mirrors are capped: under load, shadow requests are skipped rather
    than queued, so the champion's latency is never affected.
    """

    def __init__(self) -> None:
        self.active = False
        self.target_version: str | None = None
        self._stop = threading.Event()
        self._slots = threading.Semaphore(SHADOW_MAX_INFLIGHT)
        self._pool = ThreadPoolExecutor(max_workers=SHADOW_MAX_INFLIGHT, thread_name_prefix="shadow")
        self._probe = threading.Thread(target=self._probe_loop, name="shadow-probe", daemon=True)

    def start(self) -> None:
        if SHADOW_ENABLED:
            self._probe.start()
            print(f"Shadow mirroring enabled, probing {SHADOW_URL}")

    def stop(self) -> None:
        self._stop.set()
        self._pool.shutdown(wait=False, cancel_futures=True)

    def _probe_loop(self) -> None:
        import requests

        while not self._stop.is_set():
            try:
                r = requests.get(f"{SHADOW_URL}/health", timeout=SHADOW_TIMEOUT)
                body = r.json() if r.ok else {}
                healthy = r.ok and body.get("status") == "ok"
                self.target_version = body.get("version") if healthy else None
                if healthy != self.active:
                    print(f"Shadow target {'up' if healthy else 'down'}: "
                          f"{SHADOW_URL} version={self.target_version}")
                self.active = healthy
            except Exception:  # noqa: BLE001 - no challenger is the normal state
                if self.active:
                    print(f"Shadow target down: {SHADOW_URL}")
                self.active = False
                self.target_version = None
            SHADOW_ACTIVE.labels(model=MODEL_NAME).set(1 if self.active else 0)
            self._stop.wait(SHADOW_PROBE_SECONDS)

    def mirror(self, request_id: str, rows: list[dict[str, Any]]) -> None:
        if not self.active:
            return
        if not self._slots.acquire(blocking=False):
            SHADOW_REQUESTS.labels(model=MODEL_NAME, result="skipped").inc()
            return
        self._pool.submit(self._send, request_id, rows)

    def _send(self, request_id: str, rows: list[dict[str, Any]]) -> None:
        import requests

        try:
            r = requests.post(
                f"{SHADOW_URL}/predict",
                json={"rows": rows},
                headers={"X-Request-ID": request_id, "X-Shadow-Request": "1"},
                timeout=SHADOW_TIMEOUT,
            )
            SHADOW_REQUESTS.labels(model=MODEL_NAME, result="ok" if r.ok else "error").inc()
        except Exception:  # noqa: BLE001
            SHADOW_REQUESTS.labels(model=MODEL_NAME, result="error").inc()
        finally:
            self._slots.release()


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(_: FastAPI):
    print(f"Loading {MODEL_NAME} v{MODEL_VERSION} ({ROLE}) from {MODEL_PATH}")
    _state["model"] = LoadedModel(MODEL_PATH)
    _state["loaded_at"] = time.time()
    MODEL_INFO.labels(**LABELS).set(1)

    logger = PredictionLogger(
        model=MODEL_NAME,
        role=ROLE,
        bucket=os.getenv("PREDICTION_LOG_BUCKET") or None,
        endpoint_url=os.getenv("S3_ENDPOINT_URL") or None,
        batch_size=int(os.getenv("PREDICTION_LOG_BATCH_SIZE", "500")),
        flush_seconds=float(os.getenv("PREDICTION_LOG_FLUSH_SECONDS", "30")),
    )
    logger.start()
    _state["logger"] = logger

    shadow = ShadowMirror()
    shadow.start()
    _state["shadow"] = shadow

    print(f"Ready: {MODEL_NAME} v{MODEL_VERSION} as {ROLE} (flavour {_state['model'].flavor})")
    yield

    print("Shutting down - flushing prediction log")
    shadow.stop()
    logger.close()
    _state.clear()


app = FastAPI(
    title="Model inference service",
    description=f"Serving {MODEL_NAME} version {MODEL_VERSION} as {ROLE}",
    lifespan=lifespan,
)


class PredictRequest(BaseModel):
    """One or more rows of features. Column names must match the training data."""

    rows: list[dict[str, Any]] = Field(
        ...,
        min_length=1,
        examples=[[{
            "tenure_months": 5,
            "monthly_charges": 89.5,
            "support_calls": 3,
            "contract": "month-to-month",
            "internet_service": "fiber",
            "payment_method": "e-check",
        }]],
    )


class PredictResponse(BaseModel):
    model_name: str
    model_version: str
    role: str
    request_id: str
    predictions: list[Any]
    probabilities: list[float] | None = None
    n_rows: int
    latency_ms: float


@app.get("/health")
def health() -> dict[str, Any]:
    if "model" not in _state:
        raise HTTPException(status_code=503, detail="model not loaded")
    return {"status": "ok", "model": MODEL_NAME, "version": MODEL_VERSION, "role": ROLE}


@app.get("/info")
def info() -> dict[str, Any]:
    shadow: ShadowMirror | None = _state.get("shadow")
    logger: PredictionLogger | None = _state.get("logger")
    return {
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "role": ROLE,
        "model_path": MODEL_PATH,
        "loaded": "model" in _state,
        "flavor": _state["model"].flavor if "model" in _state else None,
        "prediction_log": logger.stats() if logger else None,
        "shadow": {
            "enabled": SHADOW_ENABLED,
            "target": SHADOW_URL if SHADOW_ENABLED else None,
            "active": shadow.active if shadow else False,
            "target_version": shadow.target_version if shadow else None,
        },
    }


@app.post("/predict", response_model=PredictResponse)
def predict(
    request: PredictRequest,
    x_request_id: str | None = Header(default=None),
    x_shadow_request: str | None = Header(default=None),
) -> PredictResponse:
    model: LoadedModel | None = _state.get("model")
    if model is None:
        raise HTTPException(status_code=503, detail="model not loaded")

    # The champion mints the id; a challenger reuses it so rows line up.
    request_id = x_request_id or uuid.uuid4().hex
    start = time.perf_counter()
    try:
        predictions, probabilities = model.predict(pd.DataFrame(request.rows))
    except Exception as exc:  # noqa: BLE001
        ERRORS.labels(**LABELS).inc()
        print(f"Prediction failed for {request_id}: {exc}")
        raise HTTPException(status_code=400, detail=f"prediction failed: {exc}") from exc
    elapsed = time.perf_counter() - start

    LATENCY.labels(**LABELS).observe(elapsed)
    positives = 0
    for value in predictions:
        positive = value in (1, True, "1")
        positives += positive
        PREDICTIONS.labels(**LABELS, outcome="positive" if positive else "negative").inc()
    POSITIVE_RATE.labels(**LABELS).set(positives / len(predictions))

    # One record per row, so labels can be joined on row_id later.
    ts = utc_now_iso()
    latency_ms = round(elapsed * 1000, 3)
    _state["logger"].log([
        {
            "request_id": request_id,
            "row_id": f"{request_id}-{i}",
            "ts": ts,
            "model": MODEL_NAME,
            "version": MODEL_VERSION,
            "role": ROLE,
            "shadow": bool(x_shadow_request),
            "features": row,
            "prediction": predictions[i],
            "probability": probabilities[i] if probabilities else None,
            "latency_ms": latency_ms,
        }
        for i, row in enumerate(request.rows)
    ])

    if ROLE == "champion" and not x_shadow_request:
        _state["shadow"].mirror(request_id, request.rows)

    return PredictResponse(
        model_name=MODEL_NAME,
        model_version=MODEL_VERSION,
        role=ROLE,
        request_id=request_id,
        predictions=predictions,
        probabilities=probabilities,
        n_rows=len(predictions),
        latency_ms=latency_ms,
    )


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
