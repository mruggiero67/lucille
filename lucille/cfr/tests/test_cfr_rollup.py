"""
Tests for CFR rollup / aggregation.
"""

import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from lucille.cfr.logic.deployment_detector import DeploymentEvent
from lucille.cfr.logic.intervention_detector import InterventionResult
from lucille.cfr.logic.cfr_rollup import compute_cfr, DeploymentRecord, CFRResult


PERIOD_START = date(2026, 2, 1)
PERIOD_END = date(2026, 3, 1)

TS = datetime(2026, 2, 15, tzinfo=timezone.utc)


def _record(
    tag: str,
    category: str,
    detected: bool,
    repo: str = "analytics",
) -> DeploymentRecord:
    event = DeploymentEvent(
        deployment_id=tag,
        timestamp=TS,
        repo=repo,
        prs=[],
    )
    intervention = InterventionResult(
        detected=detected,
        reason="hotfix PR" if detected else None,
        confidence="high" if detected else "n/a",
    )
    return DeploymentRecord(event=event, category=category, intervention=intervention)


class TestComputeCFR:
    def test_zero_deployments_gives_zero_cfr(self):
        result = compute_cfr([], PERIOD_START, PERIOD_END)
        assert result.cfr == 0.0
        assert result.total_deployments == 0

    def test_no_failures_gives_zero_cfr(self):
        records = [
            _record("v1.0", "human", False),
            _record("v1.1", "human", False),
            _record("v1.2", "agent", False),
        ]
        result = compute_cfr(records, PERIOD_START, PERIOD_END)
        assert result.cfr == 0.0
        assert result.failed_deployments == 0

    def test_all_failures_gives_cfr_of_one(self):
        records = [
            _record("v1.0", "human", True),
            _record("v1.1", "agent", True),
        ]
        result = compute_cfr(records, PERIOD_START, PERIOD_END)
        assert result.cfr == 1.0

    def test_cfr_calculation_with_mixed_results(self):
        records = [
            _record("v1.0", "human", True),   # fail
            _record("v1.1", "human", False),  # ok
            _record("v1.2", "human", False),  # ok
            _record("v1.3", "agent", True),   # fail
            _record("v1.4", "agent", False),  # ok
            _record("v1.5", "hybrid", False), # ok
            _record("v1.6", "hybrid", False), # ok
            _record("v1.7", "hybrid", False), # ok
            _record("v1.8", "human", False),  # ok
            _record("v1.9", "human", False),  # ok
        ]
        result = compute_cfr(records, PERIOD_START, PERIOD_END)

        assert result.total_deployments == 10
        assert result.failed_deployments == 2
        assert abs(result.cfr - 0.2) < 1e-9

    def test_category_breakdown_is_correct(self):
        records = [
            _record("v1.0", "human", True),
            _record("v1.1", "human", False),
            _record("v1.2", "human", False),
            _record("v1.3", "human", False),
            _record("v1.4", "human", False),
            _record("v1.5", "agent", True),
            _record("v1.6", "agent", True),
            _record("v1.7", "agent", False),
            _record("v1.8", "agent", False),
            _record("v1.9", "agent", False),
        ]
        result = compute_cfr(records, PERIOD_START, PERIOD_END)

        assert result.by_category["human"].cfr == pytest.approx(0.2)
        assert result.by_category["agent"].cfr == pytest.approx(0.4)
        assert result.by_category["hybrid"].total_deployments == 0

    def test_low_confidence_flagged_when_n_below_threshold(self):
        records = [_record("v1.0", "agent", False)]  # n=1 < 5
        result = compute_cfr(records, PERIOD_START, PERIOD_END)
        assert result.low_confidence is True
        assert result.by_category["agent"].low_confidence is True

    def test_cfr_pct_property(self):
        records = [
            _record("v1.0", "human", True),
            _record("v1.1", "human", False),
            _record("v1.2", "human", False),
            _record("v1.3", "human", False),
        ]
        result = compute_cfr(records, PERIOD_START, PERIOD_END)
        assert result.cfr_pct == "25.0%"

    def test_cfr_fraction_property(self):
        records = [
            _record("v1.0", "human", True),
            _record("v1.1", "human", False),
            _record("v1.2", "human", False),
        ]
        result = compute_cfr(records, PERIOD_START, PERIOD_END)
        assert result.cfr_fraction == "1/3"
