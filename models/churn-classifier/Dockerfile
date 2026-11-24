# Trainer image. Built by CI on every release tag; run by the Airflow
# training DAG as a Kubernetes pod.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY train.py .
RUN useradd -u 10001 -m trainer && chown -R trainer /app
USER trainer

ENTRYPOINT ["python", "train.py"]
