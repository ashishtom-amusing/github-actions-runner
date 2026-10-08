# Phase 2 — build and deploy pipeline

Repo contents:

```
service/          FastAPI inference app + Dockerfile
charts/           one generic Helm chart, reused by every model
values/           one small file per model
.github/workflows/deploy.yml
scripts/07-prepare-build-host.sh
```

## One-time setup

**1. Prepare the build host** (installs Docker and Helm for the runner):

```bash
sudo ./scripts/07-prepare-build-host.sh
```

**2. Repo → Settings → Secrets and variables → Actions**

*Variables* tab:

| Name | Value |
|---|---|
| `MLFLOW_URL` | `http://mlflow.192.168.10.180.nip.io` |
| `NODE_IP` | `192.168.10.180` |

*Secrets* tab:

| Name | Value |
|---|---|
| `MLFLOW_PASSWORD` | the `MLFLOW_UI_PASSWORD` from `.secrets/credentials.env` |

**3. Settings → Actions → General → Workflow permissions**
Set to **Read and write**, so the workflow can push to GHCR.

**4. After the first successful push**, the GHCR package is private by default.
Open `https://github.com/users/<owner>/packages`, pick the package, and set its
visibility to public — otherwise k3s cannot pull it. Alternatively keep it
private and add an imagePullSecret to `values/<model>.yaml`.

## Running it

Manually: **Actions → Build and deploy model → Run workflow**, then enter the
model name and version.

From Airflow: DAG 2 dispatches it automatically once the approval is granted.

## What the workflow does

1. Downloads the model from the MLflow registry by name and version
2. Builds an image with the model baked in, tagged `v<model_version>`
3. Pushes to `ghcr.io/<owner>/<repo>/<model>`
4. `helm upgrade --install` into the `ml-apps` namespace
5. Smoke-tests `/health`, `/info` and `/predict`, and fails if the pod reports a
   different version than the one requested

Result: `http://churn-classifier.192.168.10.180.nip.io/predict`

## Why everything runs on the self-hosted runner

MLflow is on a private network, so GitHub-hosted runners cannot reach it to
download the model. Since the model is baked into the image, the build has to
happen where MLflow is reachable — your node.

## Adding a second model

```bash
cp values/churn-classifier.yaml values/<new-model>.yaml
```

Then run the workflow with that model name. Same chart, same image recipe, its
own release, ingress host and Grafana metrics.

## Rolling back

```bash
sudo helm rollback <model-name> -n ml-apps
```

Or re-run the workflow with the previous version number — the image for it is
still in GHCR.

## Grafana

The chart ships a ServiceMonitor, so Prometheus discovers the pod automatically.
Metrics exposed: `model_predictions_total`, `model_prediction_latency_seconds`,
`model_prediction_errors_total`, `model_positive_prediction_rate`.

The last one is the drift signal — a sudden shift in the share of positive
predictions is the first sign something changed upstream.
