"""
Unit tests for SUP Cycle Time Analysis

Tests focus on pure functions with no side effects.
"""

import pytest
from datetime import datetime, timedelta

from lucille.jira.sup_cycle_time import (
    calculate_cycle_time_days,
    get_week_label,
    get_date_range,
    group_issues_by_week,
    calculate_weekly_averages,
    classify_trend,
    build_cycle_time_summary,
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def sample_issues_data():
    """Three processed issues across two resolved weeks."""
    return [
        {
            'key': 'SUP-1', 'summary': 'Issue 1',
            'created': '2026-02-02 09:00', 'resolved': '2026-02-09 10:00',
            'cycle_time_days': 7.04, 'resolved_week': '2026-W06',
            'status': 'Done', 'assignee': 'Alice', 'reporter': 'Bob',
        },
        {
            'key': 'SUP-2', 'summary': 'Issue 2',
            'created': '2026-02-01 08:00', 'resolved': '2026-02-10 08:00',
            'cycle_time_days': 9.0, 'resolved_week': '2026-W06',
            'status': 'Done', 'assignee': 'Alice', 'reporter': 'Carol',
        },
        {
            'key': 'SUP-3', 'summary': 'Issue 3',
            'created': '2026-02-10 11:00', 'resolved': '2026-02-14 11:00',
            'cycle_time_days': 4.0, 'resolved_week': '2026-W07',
            'status': 'Done', 'assignee': 'Dave', 'reporter': 'Bob',
        },
    ]


@pytest.fixture
def growing_weekly_stats():
    """Weekly stats with a clear growing cycle-time trend (CV < 0.4)."""
    return [
        ('2026-W01', 4.0, 5),
        ('2026-W02', 4.2, 4),
        ('2026-W03', 5.8, 4),
        ('2026-W04', 6.0, 5),
    ]


@pytest.fixture
def shrinking_weekly_stats():
    """Weekly stats with a clear shrinking cycle-time trend."""
    return [
        ('2026-W01', 6.0, 5),
        ('2026-W02', 5.8, 4),
        ('2026-W03', 4.2, 4),
        ('2026-W04', 4.0, 5),
    ]


# ============================================================================
# Tests for calculate_cycle_time_days
# ============================================================================

def test_calculate_cycle_time_days_basic():
    created = datetime(2026, 2, 1, 9, 0, 0)
    resolved = datetime(2026, 2, 8, 9, 0, 0)
    assert calculate_cycle_time_days(created, resolved) == 7.0


def test_calculate_cycle_time_days_partial():
    created = datetime(2026, 2, 1, 9, 0, 0)
    resolved = datetime(2026, 2, 1, 21, 0, 0)  # 12 hours later
    assert calculate_cycle_time_days(created, resolved) == 0.5


def test_calculate_cycle_time_days_missing_created():
    resolved = datetime(2026, 2, 8)
    assert calculate_cycle_time_days(None, resolved) == 0.0


def test_calculate_cycle_time_days_missing_resolved():
    created = datetime(2026, 2, 1)
    assert calculate_cycle_time_days(created, None) == 0.0


def test_calculate_cycle_time_days_both_none():
    assert calculate_cycle_time_days(None, None) == 0.0


def test_calculate_cycle_time_days_same_instant():
    dt = datetime(2026, 2, 1, 12, 0, 0)
    assert calculate_cycle_time_days(dt, dt) == 0.0


# ============================================================================
# Tests for get_week_label
# ============================================================================

def test_get_week_label_format():
    import re
    for month in range(1, 13):
        dt = datetime(2026, month, 15)
        assert re.match(r'^\d{4}-W\d{2}$', get_week_label(dt))


def test_get_week_label_same_week():
    monday = datetime(2026, 2, 9)
    friday = datetime(2026, 2, 13)
    assert get_week_label(monday) == get_week_label(friday)


def test_get_week_label_different_weeks():
    assert get_week_label(datetime(2026, 2, 9)) != get_week_label(datetime(2026, 2, 16))


def test_get_week_label_year_in_label():
    assert get_week_label(datetime(2025, 6, 1)).startswith('2025-W')


# ============================================================================
# Tests for get_date_range
# ============================================================================

def test_get_date_range_returns_strings():
    start, end = get_date_range(8)
    assert isinstance(start, str) and isinstance(end, str)


def test_get_date_range_valid_format():
    start, end = get_date_range(4)
    datetime.strptime(start, '%Y-%m-%d')
    datetime.strptime(end, '%Y-%m-%d')


def test_get_date_range_start_before_end():
    start, end = get_date_range(8)
    assert start < end


def test_get_date_range_exact_span():
    for weeks in [1, 4, 8]:
        start, end = get_date_range(weeks)
        start_dt = datetime.strptime(start, '%Y-%m-%d')
        end_dt = datetime.strptime(end, '%Y-%m-%d')
        assert (end_dt - start_dt).days == weeks * 7


# ============================================================================
# Tests for group_issues_by_week
# ============================================================================

def test_group_issues_by_week_basic(sample_issues_data):
    grouped = group_issues_by_week(sample_issues_data)
    assert '2026-W06' in grouped
    assert '2026-W07' in grouped
    assert len(grouped['2026-W06']) == 2
    assert len(grouped['2026-W07']) == 1


def test_group_issues_by_week_empty():
    assert group_issues_by_week([]) == {}


def test_group_issues_by_week_missing_week_skipped():
    issues = [
        {'key': 'SUP-1', 'resolved_week': '2026-W05', 'cycle_time_days': 3.0},
        {'key': 'SUP-2', 'cycle_time_days': 2.0},  # no resolved_week
    ]
    grouped = group_issues_by_week(issues)
    assert list(grouped.keys()) == ['2026-W05']


# ============================================================================
# Tests for calculate_weekly_averages
# ============================================================================

def test_calculate_weekly_averages_basic(sample_issues_data):
    grouped = group_issues_by_week(sample_issues_data)
    stats = calculate_weekly_averages(grouped)
    week_map = {w: (avg, n) for w, avg, n in stats}

    assert '2026-W06' in week_map
    assert '2026-W07' in week_map
    assert week_map['2026-W06'][0] == pytest.approx((7.04 + 9.0) / 2, rel=1e-3)
    assert week_map['2026-W07'][0] == pytest.approx(4.0)


def test_calculate_weekly_averages_sorted():
    grouped = {
        '2026-W08': [{'cycle_time_days': 3.0}],
        '2026-W06': [{'cycle_time_days': 5.0}],
    }
    stats = calculate_weekly_averages(grouped)
    weeks = [w for w, _, _ in stats]
    assert weeks == sorted(weeks)


def test_calculate_weekly_averages_skips_zero_cycle_times():
    grouped = {
        '2026-W05': [
            {'cycle_time_days': 0.0},
            {'cycle_time_days': 4.0},
        ]
    }
    stats = calculate_weekly_averages(grouped)
    _, avg, count = stats[0]
    assert avg == 4.0   # only non-zero value included in average
    assert count == 2   # but count still reflects all issues


def test_calculate_weekly_averages_all_zero():
    grouped = {'2026-W05': [{'cycle_time_days': 0.0}]}
    stats = calculate_weekly_averages(grouped)
    _, avg, _ = stats[0]
    assert avg == 0.0


def test_calculate_weekly_averages_empty():
    assert calculate_weekly_averages({}) == []


# ============================================================================
# Tests for classify_trend
# ============================================================================

def test_classify_trend_growing():
    # CV < 0.4, second half clearly higher than first
    assert classify_trend([4.0, 4.2, 5.8, 6.0]) == 'growing'


def test_classify_trend_shrinking():
    assert classify_trend([6.0, 5.8, 4.2, 4.0]) == 'shrinking'


def test_classify_trend_stable():
    assert classify_trend([4.0, 4.1, 3.9, 4.0]) == 'stable'


def test_classify_trend_highly_variable():
    # Large swings → high CV
    assert classify_trend([1.0, 10.0, 2.0, 9.0]) == 'highly variable'


def test_classify_trend_empty():
    assert classify_trend([]) == 'insufficient data'


def test_classify_trend_single_value():
    assert classify_trend([5.0]) == 'insufficient data'


def test_classify_trend_two_values_growing():
    assert classify_trend([3.0, 6.0]) == 'growing'


def test_classify_trend_two_values_shrinking():
    assert classify_trend([6.0, 3.0]) == 'shrinking'


def test_classify_trend_all_zeros():
    assert classify_trend([0.0, 0.0, 0.0]) == 'stable'


def test_classify_trend_custom_change_threshold():
    # [4.0, 4.5, 5.0, 5.5]: ~24% half-over-half growth → 'growing' by default
    # but 'stable' when threshold raised to 50%
    values = [4.0, 4.5, 5.0, 5.5]
    assert classify_trend(values) == 'growing'
    assert classify_trend(values, change_threshold=0.5) == 'stable'


def test_classify_trend_custom_variability_threshold():
    # Moderately variable data: 'highly variable' at tight threshold, 'growing' at loose one
    values = [4.0, 4.2, 5.8, 6.0]
    assert classify_trend(values, variability_threshold=0.1) == 'highly variable'
    assert classify_trend(values, variability_threshold=0.9) == 'growing'


def test_classify_trend_first_half_zero_second_nonzero():
    # Going from zero to non-zero is high variation — CV >> 0.4
    assert classify_trend([0.0, 0.0, 3.0, 4.0]) == 'highly variable'


# ============================================================================
# Tests for build_cycle_time_summary
# ============================================================================

def test_build_cycle_time_summary_empty():
    result = build_cycle_time_summary([], '2026-01-01', '2026-02-19')
    assert len(result) == 2
    assert 'No resolved tickets' in result[1]


def test_build_cycle_time_summary_date_range_in_first_line():
    stats = [('2026-W06', 4.0, 5), ('2026-W07', 4.5, 3)]
    result = build_cycle_time_summary(stats, '2026-01-01', '2026-02-19')
    assert '2026-01-01' in result[0]
    assert '2026-02-19' in result[0]


def test_build_cycle_time_summary_total_ticket_count():
    stats = [('2026-W06', 4.0, 5), ('2026-W07', 4.5, 3)]
    result = build_cycle_time_summary(stats, '2026-01-01', '2026-02-19')
    assert '8' in result[0]  # 5 + 3


def test_build_cycle_time_summary_returns_three_lines():
    stats = [('2026-W06', 4.0, 5), ('2026-W07', 4.5, 3)]
    result = build_cycle_time_summary(stats, '2026-01-01', '2026-02-19')
    assert len(result) == 3


def test_build_cycle_time_summary_has_trend_line():
    stats = [('2026-W06', 4.0, 5), ('2026-W07', 4.5, 3)]
    result = build_cycle_time_summary(stats, '2026-01-01', '2026-02-19')
    assert result[2].startswith('Trend:')


def test_build_cycle_time_summary_growing(growing_weekly_stats):
    result = build_cycle_time_summary(growing_weekly_stats, '2026-01-01', '2026-01-28')
    assert 'Growing' in result[2]


def test_build_cycle_time_summary_shrinking(shrinking_weekly_stats):
    result = build_cycle_time_summary(shrinking_weekly_stats, '2026-01-01', '2026-01-28')
    assert 'Shrinking' in result[2]


def test_build_cycle_time_summary_highly_variable():
    stats = [
        ('2026-W01', 1.0, 3),
        ('2026-W02', 10.0, 2),
        ('2026-W03', 2.0, 4),
        ('2026-W04', 9.0, 3),
    ]
    result = build_cycle_time_summary(stats, '2026-01-01', '2026-01-28')
    assert 'variable' in result[2].lower()


def test_build_cycle_time_summary_stable():
    stats = [
        ('2026-W01', 4.0, 5),
        ('2026-W02', 4.1, 4),
        ('2026-W03', 3.9, 4),
        ('2026-W04', 4.0, 5),
    ]
    result = build_cycle_time_summary(stats, '2026-01-01', '2026-01-28')
    assert 'Stable' in result[2]


def test_build_cycle_time_summary_average_in_second_line():
    stats = [('2026-W06', 4.0, 5), ('2026-W07', 6.0, 3)]
    result = build_cycle_time_summary(stats, '2026-01-01', '2026-02-19')
    # Overall average of weekly avgs = (4.0 + 6.0) / 2 = 5.0
    assert '5.0' in result[1]


def test_build_cycle_time_summary_growing_mentions_halves(growing_weekly_stats):
    result = build_cycle_time_summary(growing_weekly_stats, '2026-01-01', '2026-01-28')
    # Should mention the two half averages
    assert 'days' in result[2]
    assert 'vs.' in result[2]
