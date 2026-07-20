"""Tests for lucille.opsgenie.noise."""

from datetime import datetime, timezone

import pytest

from context import lucille  # noqa: F401
from lucille.opsgenie.io import Alert
from lucille.opsgenie.noise import (
    coarse_alias,
    compute_noise_rows,
    filter_by_min_fires,
    group_by_alias,
    summarize,
    top_n,
)


def _a(
    *,
    alias="a",
    message="msg",
    status="closed",
    ack=False,
    when=None,
    team="DIP",
    owner="",
    count=1,
    alert_id="id",
) -> Alert:
    """Compact Alert factory for tests."""
    if when is None:
        when = datetime(2026, 7, 10, 12, tzinfo=timezone.utc)
    return Alert(
        alert_id=alert_id,
        alias=alias,
        message=message,
        status=status,
        acknowledged=ack,
        created_at=when,
        owner=owner,
        team=team,
        count=count,
    )


# ---------------------------------------------------------------------------
# group_by_alias
# ---------------------------------------------------------------------------


class TestGroupByAlias:
    def test_groups_alerts_sharing_alias(self):
        alerts = [_a(alias="x"), _a(alias="x"), _a(alias="y")]
        groups = group_by_alias(alerts)
        assert len(groups["x"]) == 2
        assert len(groups["y"]) == 1

    def test_empty_input_returns_empty_dict(self):
        assert group_by_alias([]) == {}

    def test_empty_alias_becomes_its_own_bucket(self):
        alerts = [_a(alias=""), _a(alias=""), _a(alias="x")]
        groups = group_by_alias(alerts)
        assert len(groups[""]) == 2


# ---------------------------------------------------------------------------
# compute_noise_rows
# ---------------------------------------------------------------------------


class TestComputeNoiseRows:
    def test_returns_one_row_per_alias(self):
        alerts = [_a(alias="x"), _a(alias="x"), _a(alias="y")]
        rows = compute_noise_rows(alerts)
        assert len(rows) == 2
        aliases = {r.alias for r in rows}
        assert aliases == {"x", "y"}

    def test_sorted_by_fires_desc(self):
        alerts = (
            [_a(alias="quiet")]
            + [_a(alias="medium")] * 3
            + [_a(alias="loud")] * 10
        )
        rows = compute_noise_rows(alerts)
        assert [r.alias for r in rows] == ["loud", "medium", "quiet"]

    def test_tiebreak_by_alias_ascending(self):
        alerts = [_a(alias="b"), _a(alias="a")]
        rows = compute_noise_rows(alerts)
        # Both have 1 fire, so alphabetical wins.
        assert [r.alias for r in rows] == ["a", "b"]

    def test_ack_rate(self):
        alerts = [
            _a(alias="x", ack=True),
            _a(alias="x", ack=True),
            _a(alias="x", ack=False),
            _a(alias="x", ack=False),
        ]
        (row,) = compute_noise_rows(alerts)
        assert row.ack_count == 2
        assert row.ack_rate == 0.5

    def test_auto_close_rate_counts_only_closed_and_not_ackd(self):
        alerts = [
            _a(alias="x", status="closed", ack=False),  # counts
            _a(alias="x", status="closed", ack=False),  # counts
            _a(alias="x", status="closed", ack=True),   # doesn't count (ackd)
            _a(alias="x", status="open", ack=False),    # doesn't count (still open)
        ]
        (row,) = compute_noise_rows(alerts)
        assert row.auto_closed_no_ack == 2
        assert row.auto_close_rate == 0.5

    def test_days_active_uses_utc_date(self):
        alerts = [
            _a(alias="x", when=datetime(2026, 7, 10, 8, tzinfo=timezone.utc)),
            _a(alias="x", when=datetime(2026, 7, 10, 23, tzinfo=timezone.utc)),
            _a(alias="x", when=datetime(2026, 7, 11, 3, tzinfo=timezone.utc)),
        ]
        (row,) = compute_noise_rows(alerts)
        assert row.days_active == 2
        assert row.fires_per_active_day == 1.5

    def test_first_and_last_seen(self):
        early = datetime(2026, 7, 1, tzinfo=timezone.utc)
        late = datetime(2026, 7, 15, tzinfo=timezone.utc)
        alerts = [
            _a(alias="x", when=late),
            _a(alias="x", when=early),
            _a(alias="x", when=datetime(2026, 7, 5, tzinfo=timezone.utc)),
        ]
        (row,) = compute_noise_rows(alerts)
        assert row.first_seen == early
        assert row.last_seen == late

    def test_teams_are_deduped_and_sorted(self):
        alerts = [
            _a(alias="x", team="DevOps"),
            _a(alias="x", team="DIP"),
            _a(alias="x", team="DevOps"),
            _a(alias="x", team=""),  # empty team dropped
        ]
        (row,) = compute_noise_rows(alerts)
        # Python's default string sort is case-sensitive lexicographic,
        # so "DIP" comes before "DevOps" (uppercase < lowercase).
        assert row.teams == "DIP, DevOps"

    def test_sample_message_prefers_first_non_empty(self):
        alerts = [
            _a(alias="x", message=""),
            _a(alias="x", message="real message"),
            _a(alias="x", message="another"),
        ]
        (row,) = compute_noise_rows(alerts)
        assert row.sample_message == "real message"

    def test_sample_message_falls_back_to_first_if_all_empty(self):
        alerts = [_a(alias="x", message=""), _a(alias="x", message="")]
        (row,) = compute_noise_rows(alerts)
        assert row.sample_message == ""

    def test_empty_input_returns_empty_list(self):
        assert compute_noise_rows([]) == []


# ---------------------------------------------------------------------------
# filter_by_min_fires + top_n
# ---------------------------------------------------------------------------


class TestFilters:
    def _rows(self, fire_counts):
        # Build unique aliases so the sort tiebreak is stable.
        return compute_noise_rows(
            [
                _a(alias=f"a{i:02d}")
                for i, n in enumerate(fire_counts)
                for _ in range(n)
            ]
        )

    def test_filter_keeps_only_frequent_enough(self):
        rows = self._rows([10, 5, 3, 1])
        assert [r.fires for r in filter_by_min_fires(rows, 5)] == [10, 5]

    def test_filter_zero_threshold_keeps_all(self):
        rows = self._rows([1, 2, 3])
        assert len(filter_by_min_fires(rows, 0)) == 3

    def test_top_n_returns_first_n(self):
        rows = self._rows([10, 8, 6, 4, 2])
        assert [r.fires for r in top_n(rows, 3)] == [10, 8, 6]

    def test_top_n_more_than_available_returns_all(self):
        rows = self._rows([5, 3])
        assert len(top_n(rows, 10)) == 2

    def test_top_n_zero_returns_empty(self):
        # Guards against the Python slicing trap where rows[:-1] would
        # sneakily return "all but the last" if we did `rows[:n]` with n=0
        # via subtraction. Here we assert the safe path.
        rows = self._rows([5, 3])
        assert top_n(rows, 0) == []

    def test_top_n_negative_returns_empty(self):
        rows = self._rows([5, 3])
        assert top_n(rows, -1) == []


# ---------------------------------------------------------------------------
# summarize
# ---------------------------------------------------------------------------


class TestSummarize:
    def test_empty_input_is_all_zeros(self):
        s = summarize([], [])
        assert s.total_alerts == 0
        assert s.unique_aliases == 0
        assert s.overall_ack_rate == 0.0
        assert s.window_start is None and s.window_end is None

    def test_totals(self):
        alerts = [
            _a(alias="x", ack=True, status="closed"),
            _a(alias="x", ack=False, status="closed"),
            _a(alias="y", ack=False, status="closed"),
            _a(alias="y", ack=False, status="open"),
        ]
        rows = compute_noise_rows(alerts)
        s = summarize(alerts, rows)
        assert s.total_alerts == 4
        assert s.unique_aliases == 2
        assert s.overall_ack_rate == 0.25       # 1 of 4
        assert s.overall_auto_close_rate == 0.5  # 2 of 4 closed+unackd

    def test_top_share_reflects_concentration(self):
        # 100 alerts total; the top alias produces 60 of them.
        alerts = [_a(alias="loud") for _ in range(60)]
        alerts += [_a(alias=f"quiet{i:03d}") for i in range(40)]
        rows = compute_noise_rows(alerts)
        s = summarize(alerts, rows)
        # 60/100 from the single loudest.
        assert s.top_5_share == pytest.approx(0.60 + 4 / 100)  # loud + 4 quiets
        assert s.top_10_share == pytest.approx(0.60 + 9 / 100)

    def test_window_spans_earliest_to_latest(self):
        early = datetime(2026, 7, 1, tzinfo=timezone.utc)
        late = datetime(2026, 7, 15, tzinfo=timezone.utc)
        alerts = [
            _a(alias="x", when=late),
            _a(alias="y", when=early),
        ]
        rows = compute_noise_rows(alerts)
        s = summarize(alerts, rows)
        assert s.window_start == early
        assert s.window_end == late


# ---------------------------------------------------------------------------
# coarse_alias
# ---------------------------------------------------------------------------


class TestCoarseAlias:
    def test_extracts_datadog_monitor_id(self):
        raw = "org_id:305115|metric:jaris.data_platform.pipelines.runs|monitor_id:143823196|#job:paysafe_msot_transformer"
        assert coarse_alias(raw) == "dd:monitor_143823196"

    def test_same_monitor_different_tags_collapse_together(self):
        # This is the exact fragmentation we saw in the real CSV: one
        # monitor_id spread across many #job:X variants.
        a = "org_id:305115|metric:X|monitor_id:143823196|#job:jaris_shared"
        b = "org_id:305115|metric:X|monitor_id:143823196|#job:paysafe"
        c = "org_id:305115|metric:X|monitor_id:143823196|#job:hubspot"
        assert coarse_alias(a) == coarse_alias(b) == coarse_alias(c)

    def test_uuid_alias_passes_through_unchanged(self):
        raw = "9c55c906-5ac6-0bda-9cf7-2558bf8de63e"
        assert coarse_alias(raw) == raw

    def test_uuid_with_trailing_epoch_suffix_passes_through(self):
        # Seen in the wild: some UUIDs carry a trailing '-<ms-epoch>'.
        raw = "5a5f72c3-49a5-40bd-867f-87e2f02e3887-1776631854809"
        assert coarse_alias(raw) == raw

    def test_empty_alias_passes_through(self):
        assert coarse_alias("") == ""

    def test_missing_monitor_id_passes_through(self):
        # No 'monitor_id:' fragment at all \u2014 stays untouched.
        raw = "org_id:305115|metric:foo|#service:ledger"
        assert coarse_alias(raw) == raw

    def test_non_numeric_monitor_id_falls_back_to_raw(self):
        # Defensive: if the id isn't digits, don't invent a synthetic
        # key that might collide with something else.
        raw = "org_id:305115|monitor_id:not_a_number|#env:prod"
        assert coarse_alias(raw) == raw

    def test_metric_none_still_extracts(self):
        # Real CSV had 'metric:None|monitor_id:71761688' rows.
        raw = "org_id:305115|metric:None|monitor_id:71761688"
        assert coarse_alias(raw) == "dd:monitor_71761688"


# ---------------------------------------------------------------------------
# compute_noise_rows with a key_fn
# ---------------------------------------------------------------------------


class TestComputeNoiseRowsWithKeyFn:
    def test_key_fn_defaults_to_alias(self):
        # Regression: passing no key_fn must behave exactly like before.
        alerts = [_a(alias="x"), _a(alias="x"), _a(alias="y")]
        rows = compute_noise_rows(alerts)
        assert {r.alias: r.fires for r in rows} == {"x": 2, "y": 1}

    def test_coarse_grouping_merges_datadog_fragments(self):
        alerts = [
            _a(alias="org_id:1|monitor_id:100|#a:1"),
            _a(alias="org_id:1|monitor_id:100|#a:2"),
            _a(alias="org_id:1|monitor_id:100|#a:3"),
            _a(alias="org_id:1|monitor_id:200|#b:1"),
            _a(alias="uuid-alone"),
        ]
        rows = compute_noise_rows(
            alerts, key_fn=lambda a: coarse_alias(a.alias)
        )
        result = {r.alias: r.fires for r in rows}
        assert result == {
            "dd:monitor_100": 3,
            "dd:monitor_200": 1,
            "uuid-alone": 1,
        }

    def test_coarse_grouping_preserves_ack_and_teams(self):
        # Ensure the aggregation math is per-group, not per-raw-alias:
        # 3 fragments \u2192 1 coarse row, ack_count sums correctly.
        alerts = [
            _a(alias="org_id:1|monitor_id:100|#a:1", ack=True, team="DIP"),
            _a(alias="org_id:1|monitor_id:100|#a:2", ack=False, team="DIP"),
            _a(alias="org_id:1|monitor_id:100|#a:3", ack=True, team="OOT"),
        ]
        rows = compute_noise_rows(
            alerts, key_fn=lambda a: coarse_alias(a.alias)
        )
        assert len(rows) == 1
        r = rows[0]
        assert r.alias == "dd:monitor_100"
        assert r.fires == 3
        assert r.ack_count == 2
        assert r.ack_rate == pytest.approx(2 / 3)
        # Teams are deduped and comma-joined, sorted alphabetically.
        assert r.teams == "DIP, OOT"
