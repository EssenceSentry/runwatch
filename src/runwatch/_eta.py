from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field

import numpy as np
from numpy.typing import NDArray

_DRAWS = 4_096
_RATE_PRIOR_SHAPE = 0.5
_RATE_PRIOR_RATE = 0.5
_CHANGE_PRIOR_PROBABILITY = 0.01
_CACHE_SLOWDOWN_RATIO = 0.2
_CANDIDATE_PROBABILITY = 0.9
_CHANGE_PROBABILITY = 0.99
_REJECTION_PROBABILITY = 0.5
_MIN_RECALIBRATION_SECONDS = 3.0
_DRIFT_WINDOWS_SECONDS = (
    30.0,
    60.0,
    120.0,
    300.0,
    600.0,
    1_200.0,
    1_800.0,
    3_600.0,
    7_200.0,
)
_DRIFT_CHECK_INTERVAL_SECONDS = 5.0
_DRIFT_CANDIDATE_PROBABILITY = 0.999
_DRIFT_CHANGE_PROBABILITY = 0.9999


def _linear_quantile(values: Sequence[float], quantile: float) -> float:
    position = (len(values) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    weight = position - lower
    return float(values[lower] * (1.0 - weight) + values[upper] * weight)


def bayesian_eta_seconds(
    completed: float,
    total: float | None,
    elapsed_seconds: float,
) -> dict[str, float]:
    """Estimate one progress stream with Seamless's exponential-rate model."""

    if total is None or completed <= 0 or elapsed_seconds <= 0 or completed > total:
        return {}
    remaining = total - completed
    if remaining == 0:
        return {"p10": 0.0, "p50": 0.0, "p90": 0.0}

    rng = np.random.default_rng(0)
    rate_draws: NDArray[np.float64] = rng.gamma(
        shape=completed,
        scale=1.0 / elapsed_seconds,
        size=_DRAWS,
    ).astype(np.float64, copy=False)
    remaining_draws: NDArray[np.float64] = rng.gamma(
        shape=remaining,
        scale=1.0 / rate_draws,
    ).astype(np.float64, copy=False)
    values = sorted(float(value) for value in remaining_draws)
    return {
        "p10": _linear_quantile(values, 0.1),
        "p50": _linear_quantile(values, 0.5),
        "p90": _linear_quantile(values, 0.9),
    }


def _rate_log_evidence(completed: float, elapsed: float) -> float:
    return (
        _RATE_PRIOR_SHAPE * math.log(_RATE_PRIOR_RATE)
        - math.lgamma(_RATE_PRIOR_SHAPE)
        + math.lgamma(_RATE_PRIOR_SHAPE + completed)
        - (_RATE_PRIOR_SHAPE + completed) * math.log(_RATE_PRIOR_RATE + elapsed)
    )


def _rate_change_probability(
    previous_completed: float,
    previous_elapsed: float,
    new_completed: float,
    new_elapsed: float,
    material_ratio: float | None = None,
) -> float:
    time_scale = previous_completed / previous_elapsed
    previous_exposure = previous_elapsed * time_scale
    new_exposure = new_elapsed * time_scale
    log_bayes_factor = (
        _rate_log_evidence(previous_completed, previous_exposure)
        + _rate_log_evidence(new_completed, new_exposure)
        - _rate_log_evidence(
            previous_completed + new_completed,
            previous_exposure + new_exposure,
        )
    )
    log_prior_odds = math.log(
        _CHANGE_PRIOR_PROBABILITY / (1.0 - _CHANGE_PRIOR_PROBABILITY)
    )
    log_odds = log_bayes_factor + log_prior_odds
    change_probability = (
        1.0 / (1.0 + math.exp(-log_odds))
        if log_odds >= 0
        else math.exp(log_odds) / (1.0 + math.exp(log_odds))
    )

    rng = np.random.default_rng(0)
    previous_rates = rng.gamma(
        shape=previous_completed + _RATE_PRIOR_SHAPE,
        scale=1.0 / (_RATE_PRIOR_RATE + previous_exposure),
        size=_DRAWS,
    )
    new_rates = rng.gamma(
        shape=new_completed + _RATE_PRIOR_SHAPE,
        scale=1.0 / (_RATE_PRIOR_RATE + new_exposure),
        size=_DRAWS,
    )
    if material_ratio is None:
        return change_probability
    materially_slower = new_rates <= material_ratio * previous_rates
    return change_probability * float(np.mean(materially_slower))


def _best_drift_candidate(
    points: Sequence[tuple[float, float]],
    anchor: tuple[float, float],
    current: tuple[float, float],
) -> tuple[float, tuple[float, float]] | None:
    anchor_completed, anchor_elapsed = anchor
    completed, elapsed = current
    best: tuple[float, tuple[float, float]] | None = None
    for window_seconds in _DRIFT_WINDOWS_SECONDS:
        window_start = next(
            (
                point
                for point in reversed(points)
                if point[1] <= elapsed - window_seconds
            ),
            None,
        )
        if window_start is None:
            continue
        start_completed, start_elapsed = window_start
        previous_completed = start_completed - anchor_completed
        previous_elapsed = start_elapsed - anchor_elapsed
        if previous_completed <= 0 or previous_elapsed <= 0:
            continue
        probability = _rate_change_probability(
            previous_completed,
            previous_elapsed,
            completed - start_completed,
            elapsed - start_elapsed,
        )
        if best is None or probability > best[0]:
            best = probability, window_start
    return best


@dataclass
class CacheAwareBayesianETA:
    """Estimate a tqdm stream across cache and sustained rate changes."""

    _total: float | None = None
    _previous: tuple[float, float] | None = None
    _anchor: tuple[float, float] = (0.0, 0.0)
    _candidate: list[tuple[float, float]] = field(
        default_factory=lambda: list[tuple[float, float]]()
    )
    _drift_window: list[tuple[float, float]] = field(
        default_factory=lambda: list[tuple[float, float]]()
    )
    _change_probability: float | None = None
    _drift_probability: float | None = None
    _last_drift_check_elapsed: float | None = None
    _cache_adjusted: bool = False
    _drift_adjusted: bool = False

    def update(
        self,
        completed: float,
        total: float | None,
        elapsed_seconds: float,
    ) -> tuple[dict[str, float], str, float | None]:
        if total is None or completed < 0 or elapsed_seconds < 0 or completed > total:
            return {}, "warming_up", None
        if self._should_reset(completed, total, elapsed_seconds):
            self._reset(total)

        previous = self._previous
        self._previous = (completed, elapsed_seconds)
        if completed == total:
            return (
                bayesian_eta_seconds(completed, total, elapsed_seconds),
                "complete",
                None,
            )
        current = (completed, elapsed_seconds)
        cache_detection_open = bool(self._candidate) or (
            elapsed_seconds <= _DRIFT_WINDOWS_SECONDS[2]
        )
        if (
            previous is not None
            and not self._cache_adjusted
            and not self._drift_adjusted
            and cache_detection_open
        ):
            self._observe_rate_change(previous, current)
        if not self._candidate:
            self._observe_drift(current)
        if self._candidate:
            return {}, "recalibrating", self._change_probability
        if self._drift_probability is not None:
            return {}, "recalibrating_drift", self._drift_probability

        anchor_completed, anchor_elapsed = self._anchor
        estimate = bayesian_eta_seconds(
            completed - anchor_completed,
            total - anchor_completed,
            elapsed_seconds - anchor_elapsed,
        )
        status = (
            "drift_adjusted"
            if self._drift_adjusted
            else "cache_adjusted" if self._cache_adjusted else "estimating"
        )
        return estimate, status, self._change_probability

    def _should_reset(
        self,
        completed: float,
        total: float,
        elapsed_seconds: float,
    ) -> bool:
        if self._total != total:
            return True
        if self._previous is None:
            return False
        previous_completed, previous_elapsed = self._previous
        return completed < previous_completed or elapsed_seconds < previous_elapsed

    def _reset(self, total: float) -> None:
        self._total = total
        self._previous = None
        self._anchor = (0.0, 0.0)
        self._candidate.clear()
        self._drift_window.clear()
        self._change_probability = None
        self._drift_probability = None
        self._last_drift_check_elapsed = None
        self._cache_adjusted = False
        self._drift_adjusted = False

    def _observe_rate_change(
        self,
        previous: tuple[float, float],
        current: tuple[float, float],
    ) -> None:
        previous_completed, previous_elapsed = previous
        completed, elapsed = current
        interval_elapsed = elapsed - previous_elapsed
        if interval_elapsed <= 0:
            return

        if self._candidate:
            self._candidate.append(current)
            self._update_candidate()
            return

        anchor_completed, anchor_elapsed = self._anchor
        observed = previous_completed - anchor_completed
        observed_elapsed = previous_elapsed - anchor_elapsed
        if observed <= 0 or observed_elapsed <= 0:
            return
        probability = _rate_change_probability(
            observed,
            observed_elapsed,
            completed - previous_completed,
            interval_elapsed,
            _CACHE_SLOWDOWN_RATIO,
        )
        if probability >= _CANDIDATE_PROBABILITY:
            self._candidate = [previous, current]
            self._change_probability = probability

    def _update_candidate(self) -> None:
        start_completed, start_elapsed = self._candidate[0]
        completed, elapsed = self._candidate[-1]
        duration = elapsed - start_elapsed
        anchor_completed, anchor_elapsed = self._anchor
        if duration <= 0:
            return
        probability = _rate_change_probability(
            start_completed - anchor_completed,
            start_elapsed - anchor_elapsed,
            completed - start_completed,
            duration,
            _CACHE_SLOWDOWN_RATIO,
        )
        self._change_probability = probability
        if probability < _REJECTION_PROBABILITY:
            self._candidate.clear()
            self._change_probability = None
            return
        if (
            duration >= _MIN_RECALIBRATION_SECONDS
            and probability >= _CHANGE_PROBABILITY
        ):
            self._anchor = self._candidate[0]
            self._candidate.clear()
            self._drift_window.clear()
            self._last_drift_check_elapsed = None
            self._cache_adjusted = True

    def _observe_drift(self, current: tuple[float, float]) -> None:
        self._drift_window.append(current)
        _, elapsed = current
        cutoff = elapsed - _DRIFT_WINDOWS_SECONDS[-1]
        while len(self._drift_window) > 1 and self._drift_window[1][1] <= cutoff:
            self._drift_window.pop(0)

        if (
            self._last_drift_check_elapsed is not None
            and elapsed - self._last_drift_check_elapsed < _DRIFT_CHECK_INTERVAL_SECONDS
        ):
            return
        self._last_drift_check_elapsed = elapsed

        best = _best_drift_candidate(self._drift_window, self._anchor, current)
        probability = best[0] if best is not None else 0.0
        if probability < _DRIFT_CANDIDATE_PROBABILITY:
            self._drift_probability = None
            return

        self._change_probability = probability
        if probability < _DRIFT_CHANGE_PROBABILITY:
            self._drift_probability = probability
            return

        assert best is not None
        self._anchor = best[1]
        self._drift_window = [current]
        self._drift_probability = None
        self._last_drift_check_elapsed = elapsed
        self._drift_adjusted = True


__all__ = ["CacheAwareBayesianETA", "bayesian_eta_seconds"]
