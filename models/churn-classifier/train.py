"""
Trainer for churn-classifier.

Runs as a container launched by the Airflow training DAG. Owned by the model
team: change the candidates or features here, tag a release, and CI builds a
new trainer image. Nothing in Airflow needs to change.

    python train.py --model-name churn-classifier --batch-id <id> \
        --train-uri s3://datasets/snapshots/churn-classifier/<id>/train.parquet \
        --validation-uri s3://datasets/snapshots/churn-classifier/<id>/validation.parquet \
        --manifest-uri s3://datasets/snapshots/churn-classifier/<id>/manifest.json

Every candidate becomes one MLflow run tagged with the batch id, so the
promotion DAG can pick the best run from exactly this training batch. Each run
also stores the training features as a reference dataset, which the drift
monitor compares live traffic against.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sys
import tempfile

import pandas as pd

TARGET = "churn"
NUMERIC = ["tenure_months", "monthly_charges", "support_calls"]
CATEGORICAL = ["contract", "internet_service", "payment_method"]
RANDOM_STATE = 42
# Separate floors: an 80/20 split of production feedback gives a much smaller
# validation set than training set.
MIN_TRAIN_ROWS = 1000
MIN_VALIDATION_ROWS = 200


# --------------------------------------------------------------------------- data
def s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}),
    )


def read_bytes(uri: str) -> bytes:
    bucket, key = uri.removeprefix("s3://").split("/", 1)
    return s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()


def read_parquet(uri: str) -> tuple[pd.DataFrame, str]:
    """Returns the frame and the sha256 of the exact bytes read, which identifies the data version."""
    body = read_bytes(uri)
    return pd.read_parquet(io.BytesIO(body)), hashlib.sha256(body).hexdigest()


def check_snapshot(manifest: dict, train_sha: str, valid_sha: str) -> list[str]:
    """The files must be byte-for-byte the ones the snapshot recorded."""
    problems = []
    for name, sha in (("train", train_sha), ("validation", valid_sha)):
        expected = manifest.get(name, {}).get("sha256")
        if expected and expected != sha:
            problems.append(f"{name}: sha256 {sha[:12]} does not match the snapshot manifest {expected[:12]}")
    return problems


def validate(df: pd.DataFrame, name: str, min_rows: int = MIN_TRAIN_ROWS) -> list[str]:
    problems = []
    missing = set(NUMERIC + CATEGORICAL + [TARGET]) - set(df.columns)
    if missing:
        return [f"{name}: missing columns {sorted(missing)}"]
    if len(df) < min_rows:
        problems.append(f"{name}: {len(df)} rows, need at least {min_rows}")
    nulls = df[NUMERIC + CATEGORICAL + [TARGET]].isnull().sum()
    if nulls.any():
        problems.append(f"{name}: nulls in {nulls[nulls > 0].to_dict()}")
    if not set(df[TARGET].unique()) <= {0, 1}:
        problems.append(f"{name}: target must be 0/1, got {sorted(df[TARGET].unique())}")
    rate = df[TARGET].mean()
    if not 0.02 < rate < 0.98:
        problems.append(f"{name}: positive rate {rate:.1%} is implausible")
    if (df[["tenure_months", "monthly_charges", "support_calls"]] < 0).any().any():
        problems.append(f"{name}: negative values in numeric features")
    return problems


# ---------------------------------------------------------------------- modelling
def build_pipeline(estimator):
    """Preprocessing travels inside the model, so serving cannot drift from training."""
    from sklearn.compose import ColumnTransformer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    pre = ColumnTransformer([
        ("num", StandardScaler(), NUMERIC),
        ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL),
    ])
    return Pipeline([("prep", pre), ("clf", estimator)])


def candidates():
    from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
    from sklearn.linear_model import LogisticRegression

    return {
        "logreg": (LogisticRegression(max_iter=1000, random_state=RANDOM_STATE),
                   {"model_type": "logistic_regression", "max_iter": 1000}),
        "rf_200": (RandomForestClassifier(n_estimators=200, max_depth=10, random_state=RANDOM_STATE),
                   {"model_type": "random_forest", "n_estimators": 200, "max_depth": 10}),
        "gbm": (GradientBoostingClassifier(n_estimators=200, learning_rate=0.08, random_state=RANDOM_STATE),
                {"model_type": "gradient_boosting", "n_estimators": 200, "learning_rate": 0.08}),
    }


def evaluate(pipe, X, y) -> dict[str, float]:
    from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score

    pred = pipe.predict(X)
    proba = pipe.predict_proba(X)[:, 1]
    return {
        "accuracy": accuracy_score(y, pred),
        "f1": f1_score(y, pred),
        "precision": precision_score(y, pred),
        "recall": recall_score(y, pred),
        "roc_auc": roc_auc_score(y, proba),
    }


# --------------------------------------------------------------------------- main
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--train-uri", required=True)
    parser.add_argument("--validation-uri", required=True)
    parser.add_argument("--experiment", default=None)
    parser.add_argument("--manifest-uri", default=None, help="Snapshot manifest written by the train DAG")
    parser.add_argument("--data-source", default="unknown")
    parser.add_argument("--trigger", default="manual")
    args = parser.parse_args(argv)

    import mlflow
    import mlflow.data
    import mlflow.sklearn
    from mlflow.models import infer_signature

    print(f"Loading {args.train_uri} and {args.validation_uri}")
    train, train_sha = read_parquet(args.train_uri)
    valid, valid_sha = read_parquet(args.validation_uri)
    manifest = json.loads(read_bytes(args.manifest_uri)) if args.manifest_uri else {}

    problems = (check_snapshot(manifest, train_sha, valid_sha)
                + validate(train, "train", MIN_TRAIN_ROWS) + validate(valid, "validation", MIN_VALIDATION_ROWS))
    if problems:
        for p in problems:
            print(f"DATA VALIDATION FAILED: {p}")
        return 2
    print(f"Data OK: train {len(train)} rows, validation {len(valid)} rows")

    X_train, y_train = train[NUMERIC + CATEGORICAL], train[TARGET]
    X_valid, y_valid = valid[NUMERIC + CATEGORICAL], valid[TARGET]

    mlflow.set_experiment(args.experiment or args.model_name)
    lineage = {
        "batch_id": args.batch_id,
        "trigger": args.trigger,
        "trainer_image": os.getenv("TRAINER_IMAGE", "local"),
        "git_sha": os.getenv("GIT_SHA", "unknown"),
        "train_uri": args.train_uri,
        "validation_uri": args.validation_uri,
        "train_sha256": train_sha,
        "validation_sha256": valid_sha,
        "data_source": args.data_source,
        "data_snapshot": args.manifest_uri or "none",
        "data_split": manifest.get("split", "-"),
    }

    with tempfile.TemporaryDirectory() as tmp:
        reference_path = os.path.join(tmp, "reference.parquet")
        X_train.to_parquet(reference_path, index=False)

        results = []
        for name, (estimator, params) in candidates().items():
            with mlflow.start_run(run_name=f"{name}-{args.batch_id[-6:]}") as run:
                pipe = build_pipeline(estimator).fit(X_train, y_train)
                valid_metrics = evaluate(pipe, X_valid, y_valid)
                train_metrics = evaluate(pipe, X_train, y_train)

                mlflow.set_tags(lineage)
                # Dataset lineage in the MLflow UI (Run -> Inputs): what, where, which digest.
                mlflow.log_input(mlflow.data.from_pandas(train, source=args.train_uri, targets=TARGET,
                                                         name=f"{args.model_name}-train", digest=train_sha[:10]),
                                 context="training")
                mlflow.log_input(mlflow.data.from_pandas(valid, source=args.validation_uri, targets=TARGET,
                                                         name=f"{args.model_name}-validation",
                                                         digest=valid_sha[:10]),
                                 context="validation")
                mlflow.log_params({**params, "n_train_rows": len(train), "n_validation_rows": len(valid)})
                # Unprefixed names are validation metrics - those decide promotion.
                mlflow.log_metrics(valid_metrics)
                mlflow.log_metrics({f"train_{k}": v for k, v in train_metrics.items()})
                mlflow.log_artifact(reference_path, artifact_path="reference")

                mlflow.sklearn.log_model(
                    pipe,
                    name="model",
                    signature=infer_signature(X_train, pipe.predict(X_train)),
                    input_example=X_train.head(3),
                    # MLflow 3 serialises with skops, which refuses tree internals unless trusted.
                    skops_trusted_types=["sklearn.tree._tree.Tree"],
                )
                results.append((name, run.info.run_id, valid_metrics))
                print(f"  {name:<7} f1={valid_metrics['f1']:.4f} acc={valid_metrics['accuracy']:.4f} "
                      f"recall={valid_metrics['recall']:.4f} auc={valid_metrics['roc_auc']:.4f}")

    best = max(results, key=lambda r: r[2]["f1"])
    print(f"\n{len(results)} candidates logged for batch {args.batch_id}. "
          f"Best by validation F1: {best[0]} ({best[2]['f1']:.4f})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
