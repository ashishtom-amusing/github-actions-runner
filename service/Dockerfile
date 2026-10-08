# Serving image. The model is COPYed in from ./model, which the CI pipeline
# downloads from the MLflow registry just before the build. The image is
# therefore immutable and self-contained: tag == model version.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MODEL_PATH=/opt/model

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Downloaded by the workflow's "fetch model" step
COPY model /opt/model
COPY app.py .

# Don't run as root
RUN useradd -u 10001 -m appuser && chown -R appuser /opt/model /app
USER appuser

EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
