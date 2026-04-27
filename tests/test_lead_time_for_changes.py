"""Unit tests for lucille.lead_time_for_changes (pure functions only)."""
from datetime import date, datetime, timezone
from context import lucille  # noqa: F401
from lucille.lead_time_for_changes import (
    aggregate_weekly_metrics,
    aggregate_weekly_metrics_by_project,
    build_change_records,
    calculate_lead_time_hours,
    compute_percentile,
)


def _dt(year, month, day, hour=12):
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


def _dep(repo, version, deployed_at, commits):
    return {"repo": repo, "version": version, "deployed_at": deployed_at, "commits": commits}


def _commit(sha, ticket_keys):
    return {"sha": sha, "message": "", "ticket_keys": ticket_keys}


class TestCalculateLeadTimeHours:
    def test_two_day_gap(self):
        assert calculate_lead_time_hours(_dt(2025, 6, 10), _dt(2025, 6, 8)) == 48.0

    def test_zero_gap(self):
        now = _dt(2025, 6, 10)
        assert calculate_lead_time_hours(now, now) == 0.0

    def test_fractional_hours(self):
        deployed = datetime(2025, 6, 10, 13, 30, tzinfo=timezone.utc)
        started = datetime(2025, 6, 10, 12, 0, tzinfo=timezone.utc)
        assert calculate_lead_time_hours(deployed, started) == 1.5


class TestComputePercentile:
    def test_p75_above_median(self):
        values = list(range(1, 101))
        assert compute_percentile(values, 75) > compute_percentile(values, 50)

    def test_p90_above_p75(self):
        values = list(range(1, 101))
        assert compute_percentile(values, 90) > compute_percentile(values, 75)


class TestBuildChangeRecords:
    def test_maps_ticket_to_lead_time(self):
        dep = _dep("repo-a", "v1.0", _dt(2025, 6, 10), [_commit("abc", ["OOT-123"])])
        records = build_change_records([dep], {"OOT-123": _dt(2025, 6, 8)})
        assert len(records) == 1
        assert records[0].ticket_key == "OOT-123"
        assert records[0].lead_time_hours == 48.0

    def test_sets_jira_project_from_ticket_key(self):
        dep = _dep("repo-a", "v1.0", _dt(2025, 6, 10), [_commit("abc", ["SSJ-456"])])
        records = build_change_records([dep], {"SSJ-456": _dt(2025, 6, 8)})
        assert records[0].jira_project == "SSJ"

    def test_missing_ticket_gives_none_lead_time(self):
        dep = _dep("repo-a", "v1.0", _dt(2025, 6, 10), [_commit("abc", ["OOT-999"])])
        records = build_change_records([dep], {})
        assert len(records) == 1
        assert records[0].lead_time_hours is None

    def test_commit_with_multiple_tickets_creates_multiple_records(self):
        dep = _dep("repo-a", "v1.0", _dt(2025, 6, 10), [
            _commit("abc", ["OOT-1", "SSJ-2"])
        ])
        records = build_change_records([dep], {
            "OOT-1": _dt(2025, 6, 8),
            "SSJ-2": _dt(2025, 6, 9),
        })
        assert len(records) == 2

    def test_commit_with_no_tickets_creates_no_record(self):
        dep = _dep("repo-a", "v1.0", _dt(2025, 6, 10), [_commit("abc", [])])
        records = build_change_records([dep], {})
        assert records == []

    def test_deployment_id_format(self):
        dep = _dep("my-repo", "v2.3.1", _dt(2025, 6, 10), [_commit("x", ["OOT-1"])])
        records = build_change_records([dep], {"OOT-1": _dt(2025, 6, 9)})
        assert records[0].deployment_id == "my-repo/v2.3.1"


class TestAggregateWeeklyMetrics:
    def _build(self):
        # Week of 2025-06-09 (Mon): two deployments, one unmapped commit
        dep1 = _dep("r", "v1", _dt(2025, 6, 10), [
            _commit("a", ["OOT-1"]),
            _commit("b", []),           # unmapped
        ])
        dep2 = _dep("r", "v2", _dt(2025, 6, 11), [_commit("c", ["OOT-2"])])
        # Week of 2025-06-16 (Mon): one deployment
        dep3 = _dep("r", "v3", _dt(2025, 6, 17), [_commit("d", ["OOT-3"])])
        start_dates = {
            "OOT-1": _dt(2025, 6, 8),   # 48h
            "OOT-2": _dt(2025, 6, 9),   # 48h
            "OOT-3": _dt(2025, 6, 14),  # 72h
        }
        records = build_change_records([dep1, dep2, dep3], start_dates)
        return records, [dep1, dep2, dep3]

    def test_groups_into_two_weeks(self):
        records, deps = self._build()
        weekly = aggregate_weekly_metrics(records, deps)
        assert len(weekly) == 2

    def test_week_start_is_monday(self):
        records, deps = self._build()
        weekly = aggregate_weekly_metrics(records, deps)
        assert weekly[0].week_start == date(2025, 6, 9)   # Monday
        assert weekly[1].week_start == date(2025, 6, 16)  # Monday

    def test_deployment_count_first_week(self):
        records, deps = self._build()
        weekly = aggregate_weekly_metrics(records, deps)
        assert weekly[0].deployment_count == 2

    def test_change_count_first_week(self):
        records, deps = self._build()
        weekly = aggregate_weekly_metrics(records, deps)
        assert weekly[0].change_count == 2

    def test_unmapped_commits_counted(self):
        records, deps = self._build()
        weekly = aggregate_weekly_metrics(records, deps)
        assert weekly[0].unmapped_commits == 1
        assert weekly[1].unmapped_commits == 0

    def test_median_computed(self):
        records, deps = self._build()
        weekly = aggregate_weekly_metrics(records, deps)
        # Both week-1 records are 48h
        assert weekly[0].median_lead_time_hours == 48.0
        assert weekly[1].median_lead_time_hours == 72.0


class TestAggregateWeeklyByProject:
    def test_groups_same_week_different_projects_as_separate_rows(self):
        dep = _dep("r", "v1", _dt(2025, 6, 10), [
            _commit("a", ["OOT-1"]),
            _commit("b", ["SSJ-2"]),
        ])
        records = build_change_records([dep], {
            "OOT-1": _dt(2025, 6, 8),
            "SSJ-2": _dt(2025, 6, 9),
        })
        weekly = aggregate_weekly_metrics_by_project(records, [dep])
        projects = {m.jira_project for m in weekly}
        assert "OOT" in projects
        assert "SSJ" in projects

    def test_per_project_stats_are_independent(self):
        dep = _dep("r", "v1", _dt(2025, 6, 10), [
            _commit("a", ["OOT-1"]),   # 48h
            _commit("b", ["SSJ-2"]),   # 24h
        ])
        records = build_change_records([dep], {
            "OOT-1": _dt(2025, 6, 8),
            "SSJ-2": _dt(2025, 6, 9),
        })
        weekly = aggregate_weekly_metrics_by_project(records, [dep])
        oot = next(m for m in weekly if m.jira_project == "OOT")
        ssj = next(m for m in weekly if m.jira_project == "SSJ")
        assert oot.median_lead_time_hours == 48.0
        assert ssj.median_lead_time_hours == 24.0

    def test_same_project_different_weeks_is_two_rows(self):
        dep1 = _dep("r", "v1", _dt(2025, 6, 10), [_commit("a", ["OOT-1"])])
        dep2 = _dep("r", "v2", _dt(2025, 6, 17), [_commit("b", ["OOT-2"])])
        records = build_change_records([dep1, dep2], {
            "OOT-1": _dt(2025, 6, 8),
            "OOT-2": _dt(2025, 6, 15),
        })
        weekly = aggregate_weekly_metrics_by_project(records, [dep1, dep2])
        oot_rows = [m for m in weekly if m.jira_project == "OOT"]
        assert len(oot_rows) == 2
