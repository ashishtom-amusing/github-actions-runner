"""Fast tests that run in CI without S3 or MLflow."""

import numpy as np
import pandas as pd
import pytest

import train


def make_frame(n=1500, seed=0):
    rng = np.random.default_rng(seed)
    df = pd.DataFrame({
        "tenure_months": rng.integers(1, 73, n),
        "monthly_charges": np.round(rng.uniform(18, 120, n), 2),
        "support_calls": rng.poisson(1.2, n),
        "contract": rng.choice(["month-to-month", "one-year", "two-year"], n),
        "internet_service": rng.choice(["dsl", "fiber", "none"], n),
        "payment_method": rng.choice(["card", "bank-transfer", "e-check"], n),
    })
    df["churn"] = ((df.monthly_charges > 70) & (df.tenure_months < 24)).astype(int)
    return df


def test_valid_data_passes():
    assert train.validate(make_frame(), "train") == []


def test_missing_column_is_reported():
    problems = train.validate(make_frame().drop(columns="contract"), "train")
    assert problems and "missing columns" in problems[0]


def test_too_few_rows_is_reported():
    assert any("rows" in p for p in train.validate(make_frame(n=100), "train"))


def test_validation_split_has_its_own_floor():
    # 300 rows: too few to train on, enough to validate with.
    df = make_frame(n=300)
    assert any("rows" in p for p in train.validate(df, "train", train.MIN_TRAIN_ROWS))
    assert train.validate(df, "validation", train.MIN_VALIDATION_ROWS) == []


def test_negative_values_are_reported():
    df = make_frame()
    df.loc[0, "tenure_months"] = -5
    assert any("negative" in p for p in train.validate(df, "train"))


def test_non_binary_target_is_reported():
    df = make_frame()
    df.loc[0, "churn"] = 2
    assert any("target" in p for p in train.validate(df, "train"))


@pytest.mark.parametrize("name", list(train.candidates()))
def test_every_candidate_trains_and_scores(name):
    df = make_frame()
    estimator, _ = train.candidates()[name]
    pipe = train.build_pipeline(estimator).fit(df[train.NUMERIC + train.CATEGORICAL], df["churn"])
    metrics = train.evaluate(pipe, df[train.NUMERIC + train.CATEGORICAL], df["churn"])
    assert set(metrics) == {"accuracy", "f1", "precision", "recall", "roc_auc"}
    assert all(0.0 <= v <= 1.0 for v in metrics.values())


def test_pipeline_tolerates_unseen_category():
    df = make_frame()
    estimator, _ = train.candidates()["logreg"]
    pipe = train.build_pipeline(estimator).fit(df[train.NUMERIC + train.CATEGORICAL], df["churn"])
    unseen = df.head(1)[train.NUMERIC + train.CATEGORICAL].copy()
    unseen["contract"] = "monthly-new-plan"
    assert pipe.predict(unseen).shape == (1,)


def test_snapshot_mismatch_is_reported():
    manifest = {"train": {"sha256": "a" * 64}, "validation": {"sha256": "b" * 64}}
    assert train.check_snapshot(manifest, "a" * 64, "b" * 64) == []
    problems = train.check_snapshot(manifest, "c" * 64, "b" * 64)
    assert len(problems) == 1 and problems[0].startswith("train")


def test_no_manifest_means_no_snapshot_check():
    assert train.check_snapshot({}, "a" * 64, "b" * 64) == []
