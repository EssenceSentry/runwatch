from __future__ import annotations

import pytest

from runwatch._eta import CacheAwareBayesianETA, bayesian_eta_seconds


def test_single_worker_eta_matches_seamless_reference() -> None:
    estimate = bayesian_eta_seconds(10, 100, 10)

    assert estimate == pytest.approx(
        {
            "p10": 61.328954604763666,
            "p50": 92.5671212770502,
            "p90": 147.10523774814695,
        }
    )


def test_single_worker_eta_waits_for_progress_and_finishes_at_zero() -> None:
    assert bayesian_eta_seconds(0, 100, 10) == {}
    assert bayesian_eta_seconds(100, 100, 10) == {
        "p10": 0.0,
        "p50": 0.0,
        "p90": 0.0,
    }


def test_cache_aware_eta_rebases_after_a_stable_slowdown() -> None:
    estimator = CacheAwareBayesianETA()

    estimator.update(0, 100_000, 0)
    cached_eta, status, probability = estimator.update(50_000, 100_000, 5)
    assert status == "estimating"
    assert probability is None
    assert cached_eta["p50"] == pytest.approx(5, rel=0.02)

    eta, status, probability = estimator.update(50_003, 100_000, 6)
    assert eta == {}
    assert status == "recalibrating"
    assert probability == pytest.approx(1)
    estimator.update(50_006, 100_000, 7)
    eta, status, probability = estimator.update(50_009, 100_000, 8)

    assert status == "cache_adjusted"
    assert probability == pytest.approx(1)
    assert eta["p50"] == pytest.approx((100_000 - 50_009) / 3, rel=0.2)


def test_cache_aware_eta_keeps_the_original_regime_after_a_brief_stall() -> None:
    estimator = CacheAwareBayesianETA()

    estimator.update(0, 100_000, 0)
    estimator.update(50_000, 100_000, 5)
    eta, status, probability = estimator.update(50_000, 100_000, 6)
    assert eta == {}
    assert status == "recalibrating"
    assert probability == pytest.approx(1)

    eta, status, probability = estimator.update(60_000, 100_000, 7)
    assert status == "estimating"
    assert probability is None
    assert eta["p50"] < 10


def test_cache_aware_eta_does_not_flag_an_unchanged_rate() -> None:
    estimator = CacheAwareBayesianETA()

    estimator.update(0, 1_000, 0)
    estimator.update(500, 1_000, 5)
    _, status, probability = estimator.update(600, 1_000, 6)

    assert status == "estimating"
    assert probability is None


def test_cache_aware_eta_requires_evidence_beyond_a_small_initial_stall() -> None:
    estimator = CacheAwareBayesianETA()

    estimator.update(0, 100, 0)
    estimator.update(10, 100, 1)
    _, status, probability = estimator.update(10, 100, 2)

    assert status == "estimating"
    assert probability is None


@pytest.mark.parametrize("new_rate", [1.5, 2.4, 3.6, 6.0])
def test_cache_aware_eta_rebases_after_sustained_rate_drift(
    new_rate: float,
) -> None:
    estimator = CacheAwareBayesianETA()
    total = 100_000.0

    estimator.update(0, total, 0)
    estimator.update(50_000, total, 5)
    for second in range(6, 605, 5):
        estimator.update(50_000 + 3 * (second - 5), total, second)

    drift_start = 605
    drift_start_completed = 50_000 + 3 * (drift_start - 5)
    statuses: set[str] = set()
    completed = drift_start_completed
    eta: dict[str, float] = {}
    status = ""
    probability: float | None = None
    for second in range(drift_start, 4_205, 5):
        completed = drift_start_completed + new_rate * (second - drift_start)
        eta, status, probability = estimator.update(completed, total, second)
        statuses.add(status)
        if status == "drift_adjusted":
            break

    if new_rate != 6.0:
        assert "recalibrating_drift" in statuses
    assert status == "drift_adjusted"
    assert probability is not None and probability >= 0.999
    assert eta["p50"] == pytest.approx((total - completed) / new_rate, rel=0.2)


def test_cache_aware_eta_does_not_rebase_a_stable_long_running_rate() -> None:
    estimator = CacheAwareBayesianETA()

    estimator.update(0, 100_000, 0)
    estimator.update(50_000, 100_000, 5)
    statuses = {
        estimator.update(50_000 + 3 * (second - 5), 100_000, second)[1]
        for second in range(6, 400)
    }

    assert "recalibrating_drift" not in statuses
    assert "drift_adjusted" not in statuses


def test_cache_aware_eta_tracks_multiple_real_world_rate_regimes() -> None:
    estimator = CacheAwareBayesianETA()
    total = 1_000_000.0
    completed = 500_000.0
    elapsed = 5.0

    estimator.update(0, total, 0)
    estimator.update(completed, total, elapsed)

    regimes = [
        ("post-cache", 4.0, 600),
        ("throttled", 2.0, 1_200),
        ("partial recovery", 3.0, 2_000),
        ("connection loss", 0.1, 180),
        ("connection restored", 3.0, 900),
        ("small speedup", 3.6, 3_600),
        ("small slowdown", 3.0, 7_200),
    ]
    for label, rate, duration in regimes:
        regime_end = elapsed + duration
        eta: dict[str, float] = {}
        status = ""
        while elapsed < regime_end:
            increment = min(10.0, regime_end - elapsed)
            elapsed += increment
            completed += rate * increment
            eta, status, _ = estimator.update(completed, total, elapsed)

        assert status in {"cache_adjusted", "drift_adjusted"}, label
        inferred_rate = (total - completed) / eta["p50"]
        assert inferred_rate == pytest.approx(rate, rel=0.03), label
