# Serving image. CI downloads the model from the MLflow registry into ./model
# just before this build, so the image is immutable: tag == model version.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MODEL_PATH=/opt/model

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install exactly what the model was trained with. MLflow writes this file
# when the model is logged, so scikit-learn and skops versions always match.
COPY model /opt/model
RUN pip install --no-cache-dir -r /opt/model/requirements.txt

COPY app.py prediction_logger.py ./

RUN useradd -u 10001 -m appuser && chown -R appuser /opt/model /app
USER appuser

EXPOSE 8000
# Single worker: one in-process prediction logger per pod. Scale with replicas.
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--timeout-graceful-shutdown", "20"]
