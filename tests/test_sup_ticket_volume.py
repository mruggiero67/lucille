"""
Unit tests for SUP Ticket Volume Analysis

Tests focus on pure functions with no side effects.
External dependencies (Jira API, filesystem) are mocked.
"""

import re
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from lucille.jira.sup_ticket_volume import (
    get_week_label,
    get_date_range,
    extract_issue_fields,
    group_issues_by_created_week,
    calculate_weekly_counts,
    process_issues,
    classify_trend,
    build_volume_summary,
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def sample_processed_issues():
    """Two issues in W05 and one in W06."""
    return [
        {
            'key': 'SUP-1', 'summary': 'Issue 1',
            'created': '2026-02-02 09:00', 'created_week': '2026-W05',
            'status': 'Done', 'assignee': 'Alice', 'reporter': 'Bob',
        },
        {
            'key': 'SUP-2', 'summary': 'Issue 2',
            'created': '2026-02-03 10:00', 'created_week': '2026-W05',
            'status': 'In Progress', 'assignee': 'Alice', 'reporter': 'Carol',
        },
        {
            'key': 'SUP-3', 'summary': 'Issue 3',
            'created': '2026-02-09 11:00', 'created_week': '2026-W06',
            'status': 'Open', 'assignee': 'Dave', 'reporter': 'Bob',
        },
    ]


@pytest.fixture
def sample_raw_issue():
    """A minimal valid raw Jira API issue payload."""
    return {
        'key': 'SUP-42',
        'fields': {
            'summary': 'Something broke',
            'created': '2026-02-10T09:30:00.000+0000',
            'status': {'name': 'In Progress'},
            'assignee': {'displayName': 'Alice Smith'},
            'reporter': {'displayName': 'Bob Jones'},
        },
    }


# ============================================================================
# Tests for get_week_label
# ============================================================================

def test_get_week_label_known_monday():
    """2026-02-09 is a Monday in week 06 (strftime %U counts from Sunday)."""
    dt = datetime(2026, 2, 9)
    label = get_week_label(dt)
    assert label.startswith('2026-W')
    # Week number should be a zero-padded two-digit int
    assert re.match(r'^\d{4}-W\d{2}$', label)


def test_get_week_label_format():
    """Label always matches YYYY-WNN pattern."""
    for month in range(1, 13):
        dt = datetime(2026, month, 15)
        label = get_week_label(dt)
        assert re.match(r'^\d{4}-W\d{2}$', label), f"Bad label for {dt}: {label}"


def test_get_week_label_same_week_consecutive_days():
    """Consecutive days within the same ISO week share the same label."""
    monday = datetime(2026, 2, 9)
    friday = datetime(2026, 2, 13)
    assert get_week_label(monday) == get_week_label(friday)


def test_get_week_label_different_weeks():
    """Days in different weeks produce different labels."""
    week1 = datetime(2026, 2, 9)   # Mon
    week2 = datetime(2026, 2, 16)  # Mon, one week later
    assert get_week_label(week1) != get_week_label(week2)


def test_get_week_label_year_in_label():
    """Year portion of the label matches the datetime's year."""
    dt = datetime(2025, 6, 1)
    label = get_week_label(dt)
    assert label.startswith('2025-W')


# ============================================================================
# Tests for get_date_range
# ============================================================================

def test_get_date_range_returns_two_strings():
    start, end = get_date_range(8)
    assert isinstance(start, str)
    assert isinstance(end, str)


def test_get_date_range_valid_date_format():
    """Both dates must be parseable as YYYY-MM-DD."""
    start, end = get_date_range(4)
    datetime.strptime(start, '%Y-%m-%d')
    datetime.strptime(end, '%Y-%m-%d')


def test_get_date_range_start_before_end():
    start, end = get_date_range(8)
    assert start < end


def test_get_date_range_span_matches_weeks():
    """Formatted date span equals weeks_back * 7 days."""
    for weeks in [1, 4, 8, 12]:
        start, end = get_date_range(weeks)
        start_dt = datetime.strptime(start, '%Y-%m-%d')
        end_dt = datetime.strptime(end, '%Y-%m-%d')
        assert (end_dt - start_dt).days == weeks * 7, (
            f"Expected {weeks * 7} days for {weeks} weeks, "
            f"got {(end_dt - start_dt).days}"
        )


# ============================================================================
# Tests for extract_issue_fields
# ============================================================================

def test_extract_issue_fields_normal(sample_raw_issue):
    result = extract_issue_fields(sample_raw_issue)
    assert result is not None
    assert result['key'] == 'SUP-42'
    assert result['summary'] == 'Something broke'
    assert result['status'] == 'In Progress'
    assert result['assignee'] == 'Alice Smith'
    assert result['reporter'] == 'Bob Jones'


def test_extract_issue_fields_created_date_formatted(sample_raw_issue):
    result = extract_issue_fields(sample_raw_issue)
    assert result is not None
    assert result['created'].startswith('2026-02-10')


def test_extract_issue_fields_created_week_populated(sample_raw_issue):
    result = extract_issue_fields(sample_raw_issue)
    assert result is not None
    assert re.match(r'^\d{4}-W\d{2}$', result['created_week'])


def test_extract_issue_fields_missing_created():
    """Issue with no 'created' field returns None."""
    issue = {'key': 'SUP-1', 'fields': {'summary': 'No date'}}
    assert extract_issue_fields(issue) is None


def test_extract_issue_fields_empty_created():
    """Issue with empty 'created' string returns None."""
    issue = {'key': 'SUP-1', 'fields': {'summary': 'Empty date', 'created': ''}}
    assert extract_issue_fields(issue) is None


def test_extract_issue_fields_invalid_created():
    """Unparseable 'created' value returns None."""
    issue = {
        'key': 'SUP-7',
        'fields': {
            'summary': 'Bad date',
            'created': 'not-a-real-date-$$$$',
            'status': {'name': 'Open'},
            'assignee': None,
            'reporter': None,
        },
    }
    assert extract_issue_fields(issue) is None


def test_extract_issue_fields_null_assignee():
    """Null assignee field defaults to 'Unassigned'."""
    issue = {
        'key': 'SUP-5',
        'fields': {
            'summary': 'Unassigned issue',
            'created': '2026-02-10T09:00:00.000+0000',
            'status': {'name': 'Open'},
            'assignee': None,
            'reporter': {'displayName': 'Carol'},
        },
    }
    result = extract_issue_fields(issue)
    assert result is not None
    assert result['assignee'] == 'Unassigned'


def test_extract_issue_fields_null_reporter():
    """Null reporter field defaults to 'Unknown'."""
    issue = {
        'key': 'SUP-6',
        'fields': {
            'summary': 'No reporter',
            'created': '2026-02-10T09:00:00.000+0000',
            'status': {'name': 'Open'},
            'assignee': {'displayName': 'Dave'},
            'reporter': None,
        },
    }
    result = extract_issue_fields(issue)
    assert result is not None
    assert result['reporter'] == 'Unknown'


def test_extract_issue_fields_null_status():
    """Null status field defaults to empty string."""
    issue = {
        'key': 'SUP-8',
        'fields': {
            'summary': 'No status',
            'created': '2026-02-10T09:00:00.000+0000',
            'status': None,
            'assignee': None,
            'reporter': None,
        },
    }
    result = extract_issue_fields(issue)
    assert result is not None
    assert result['status'] == ''


def test_extract_issue_fields_missing_fields_key():
    """Issue dict with no 'fields' key returns None (no created date)."""
    issue = {'key': 'SUP-9'}
    assert extract_issue_fields(issue) is None


# ============================================================================
# Tests for group_issues_by_created_week
# ============================================================================

def test_group_issues_by_created_week_basic(sample_processed_issues):
    grouped = group_issues_by_created_week(sample_processed_issues)
    assert set(grouped.keys()) == {'2026-W05', '2026-W06'}
    assert len(grouped['2026-W05']) == 2
    assert len(grouped['2026-W06']) == 1


def test_group_issues_by_created_week_empty():
    assert group_issues_by_created_week([]) == {}


def test_group_issues_by_created_week_single_issue():
    issues = [{'key': 'SUP-1', 'created_week': '2026-W10'}]
    grouped = group_issues_by_created_week(issues)
    assert '2026-W10' in grouped
    assert len(grouped['2026-W10']) == 1


def test_group_issues_by_created_week_missing_week_skipped():
    """Issues without a 'created_week' key are silently skipped."""
    issues = [
        {'key': 'SUP-1', 'created_week': '2026-W05'},
        {'key': 'SUP-2'},  # no created_week
    ]
    grouped = group_issues_by_created_week(issues)
    assert list(grouped.keys()) == ['2026-W05']
    assert len(grouped['2026-W05']) == 1


def test_group_issues_by_created_week_all_same_week():
    issues = [
        {'key': f'SUP-{i}', 'created_week': '2026-W05'}
        for i in range(5)
    ]
    grouped = group_issues_by_created_week(issues)
    assert len(grouped) == 1
    assert len(grouped['2026-W05']) == 5


def test_group_issues_by_created_week_preserves_issue_data(sample_processed_issues):
    grouped = group_issues_by_created_week(sample_processed_issues)
    keys_in_w05 = {issue['key'] for issue in grouped['2026-W05']}
    assert keys_in_w05 == {'SUP-1', 'SUP-2'}


# ============================================================================
# Tests for calculate_weekly_counts
# ============================================================================

def test_calculate_weekly_counts_basic(sample_processed_issues):
    grouped = group_issues_by_created_week(sample_processed_issues)
    counts = calculate_weekly_counts(grouped)
    week_map = dict(counts)
    assert week_map['2026-W05'] == 2
    assert week_map['2026-W06'] == 1


def test_calculate_weekly_counts_empty():
    assert calculate_weekly_counts({}) == []


def test_calculate_weekly_counts_sorted_chronologically():
    """Output is sorted by week label (lexicographic == chronological for YYYY-WNN)."""
    grouped = {
        '2026-W08': [{'key': 'X'}],
        '2026-W06': [{'key': 'Y'}, {'key': 'Z'}],
        '2026-W07': [{'key': 'A'}],
    }
    counts = calculate_weekly_counts(grouped)
    weeks = [w for w, _ in counts]
    assert weeks == sorted(weeks)


def test_calculate_weekly_counts_single_week():
    grouped = {'2026-W05': [{'key': 'SUP-1'}, {'key': 'SUP-2'}]}
    counts = calculate_weekly_counts(grouped)
    assert counts == [('2026-W05', 2)]


def test_calculate_weekly_counts_returns_list_of_tuples():
    grouped = {'2026-W01': [{}], '2026-W02': [{}, {}]}
    counts = calculate_weekly_counts(grouped)
    assert all(isinstance(item, tuple) and len(item) == 2 for item in counts)


def test_calculate_weekly_counts_counts_are_ints():
    grouped = {'2026-W01': [{'key': 'A'}, {'key': 'B'}]}
    counts = calculate_weekly_counts(grouped)
    _, count = counts[0]
    assert isinstance(count, int)
    assert count == 2


# ============================================================================
# Tests for process_issues
# ============================================================================

def test_process_issues_normal(sample_raw_issue):
    result = process_issues([sample_raw_issue])
    assert len(result) == 1
    assert result[0]['key'] == 'SUP-42'


def test_process_issues_empty():
    assert process_issues([]) == []


def test_process_issues_skips_issues_with_missing_created():
    issues = [
        {
            'key': 'SUP-1',
            'fields': {
                'summary': 'Has date',
                'created': '2026-02-10T09:00:00.000+0000',
                'status': {'name': 'Open'},
                'assignee': None,
                'reporter': None,
            },
        },
        {
            'key': 'SUP-2',
            'fields': {
                'summary': 'No date',
                # 'created' deliberately absent
                'status': {'name': 'Open'},
                'assignee': None,
                'reporter': None,
            },
        },
    ]
    result = process_issues(issues)
    assert len(result) == 1
    assert result[0]['key'] == 'SUP-1'


def test_process_issues_multiple_valid():
    issues = [
        {
            'key': f'SUP-{i}',
            'fields': {
                'summary': f'Issue {i}',
                'created': f'2026-02-{i:02d}T09:00:00.000+0000',
                'status': {'name': 'Open'},
                'assignee': {'displayName': 'Alice'},
                'reporter': {'displayName': 'Bob'},
            },
        }
        for i in range(1, 6)
    ]
    result = process_issues(issues)
    assert len(result) == 5


def test_process_issues_all_required_keys_present(sample_raw_issue):
    result = process_issues([sample_raw_issue])
    record = result[0]
    expected_keys = {'key', 'summary', 'created', 'created_week', 'status', 'assignee', 'reporter'}
    assert expected_keys == set(record.keys())


def test_process_issues_created_week_is_valid_format(sample_raw_issue):
    result = process_issues([sample_raw_issue])
    assert re.match(r'^\d{4}-W\d{2}$', result[0]['created_week'])


# ============================================================================
# Tests for classify_trend
# ============================================================================

def test_classify_trend_growing():
    assert classify_trend([4.0, 4.2, 5.8, 6.0]) == 'growing'


def test_classify_trend_shrinking():
    assert classify_trend([6.0, 5.8, 4.2, 4.0]) == 'shrinking'


def test_classify_trend_stable():
    assert classify_trend([4.0, 4.1, 3.9, 4.0]) == 'stable'


def test_classify_trend_highly_variable():
    assert classify_trend([1.0, 10.0, 2.0, 9.0]) == 'highly variable'


def test_classify_trend_insufficient_data_empty():
    assert classify_trend([]) == 'insufficient data'


def test_classify_trend_insufficient_data_single():
    assert classify_trend([5.0]) == 'insufficient data'


def test_classify_trend_all_zeros():
    assert classify_trend([0.0, 0.0, 0.0]) == 'stable'


# ============================================================================
# Tests for build_volume_summary
# ============================================================================

def test_build_volume_summary_empty():
    result = build_volume_summary([], '2026-01-01', '2026-02-19')
    assert len(result) == 2
    assert 'No tickets' in result[1]


def test_build_volume_summary_date_range_in_first_line():
    counts = [('2026-W01', 10), ('2026-W02', 12)]
    result = build_volume_summary(counts, '2026-01-01', '2026-02-19')
    assert '2026-01-01' in result[0]
    assert '2026-02-19' in result[0]


def test_build_volume_summary_total_correct():
    counts = [('2026-W01', 10), ('2026-W02', 12)]
    result = build_volume_summary(counts, '2026-01-01', '2026-02-19')
    assert '22' in result[0]  # 10 + 12


def test_build_volume_summary_average_correct():
    counts = [('2026-W01', 10), ('2026-W02', 10), ('2026-W03', 10)]
    result = build_volume_summary(counts, '2026-01-01', '2026-02-19')
    assert '10.0' in result[1]


def test_build_volume_summary_returns_three_lines():
    counts = [('2026-W01', 10), ('2026-W02', 12)]
    result = build_volume_summary(counts, '2026-01-01', '2026-02-19')
    assert len(result) == 3


def test_build_volume_summary_has_trend_line():
    counts = [('2026-W01', 10), ('2026-W02', 12)]
    result = build_volume_summary(counts, '2026-01-01', '2026-02-19')
    assert result[2].startswith('Trend:')


def test_build_volume_summary_growing():
    # CV < 0.4, second half clearly higher
    counts = [('2026-W01', 8), ('2026-W02', 9), ('2026-W03', 12), ('2026-W04', 13)]
    result = build_volume_summary(counts, '2026-01-01', '2026-01-28')
    assert 'Growing' in result[2]


def test_build_volume_summary_shrinking():
    counts = [('2026-W01', 13), ('2026-W02', 12), ('2026-W03', 9), ('2026-W04', 8)]
    result = build_volume_summary(counts, '2026-01-01', '2026-01-28')
    assert 'Shrinking' in result[2]


def test_build_volume_summary_highly_variable():
    counts = [('2026-W01', 2), ('2026-W02', 20), ('2026-W03', 3), ('2026-W04', 18)]
    result = build_volume_summary(counts, '2026-01-01', '2026-01-28')
    assert 'variable' in result[2].lower()


def test_build_volume_summary_stable():
    counts = [('2026-W01', 10), ('2026-W02', 10), ('2026-W03', 10), ('2026-W04', 10)]
    result = build_volume_summary(counts, '2026-01-01', '2026-01-28')
    assert 'Stable' in result[2]


def test_build_volume_summary_growing_mentions_halves():
    counts = [('2026-W01', 8), ('2026-W02', 9), ('2026-W03', 12), ('2026-W04', 13)]
    result = build_volume_summary(counts, '2026-01-01', '2026-01-28')
    assert 'tickets' in result[2]
    assert 'vs.' in result[2]
