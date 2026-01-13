"""
Unit tests for Jira Cycle Time Analysis

Tests focus on pure functions with no side effects.
"""

import pytest
from datetime import datetime, timedelta
from lucille.jira.jira_cycle_time_analysis import (
    calculate_time_in_state,
    calculate_cycle_time,
    calculate_total_cycle_time,
    calculate_deployment_wait_time,
    calculate_summary_statistics,
    identify_bottlenecks,
    categorize_cycle_time,
    calculate_distribution,
    STATES
)


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def sample_transitions():
    """Sample transitions for a typical issue lifecycle."""
    base_time = datetime(2025, 1, 1, 9, 0, 0)
    return [
        {'to_state': 'Ready for Development', 'from_state': 'Backlog', 'timestamp': base_time},
        {'to_state': 'In Progress', 'from_state': 'Ready for Development', 'timestamp': base_time + timedelta(days=1)},
        {'to_state': 'Review', 'from_state': 'In Progress', 'timestamp': base_time + timedelta(days=3)},
        {'to_state': 'Ready for Testing', 'from_state': 'Review', 'timestamp': base_time + timedelta(days=4)},
        {'to_state': 'In Testing', 'from_state': 'Ready for Testing', 'timestamp': base_time + timedelta(days=5)},
        {'to_state': 'To Deploy', 'from_state': 'In Testing', 'timestamp': base_time + timedelta(days=6)},
        {'to_state': 'Done', 'from_state': 'To Deploy', 'timestamp': base_time + timedelta(days=10)},
    ]


@pytest.fixture
def sample_cycle_time():
    """Sample cycle time dictionary."""
    return {
        'Ready for Development': 1.0,
        'In Progress': 2.0,
        'Review': 1.0,
        'Ready for Testing': 1.0,
        'In Testing': 1.0,
        'To Deploy': 4.0,
        'Done': 0.0,
    }


@pytest.fixture
def multiple_cycle_times():
    """Multiple cycle time dictionaries for statistics testing."""
    return [
        {
            'Ready for Development': 1.0,
            'In Progress': 2.0,
            'Review': 1.0,
            'Ready for Testing': 1.0,
            'In Testing': 1.0,
            'To Deploy': 4.0,
            'Done': 0.0,
        },
        {
            'Ready for Development': 2.0,
            'In Progress': 3.0,
            'Review': 2.0,
            'Ready for Testing': 1.0,
            'In Testing': 2.0,
            'To Deploy': 2.0,
            'Done': 0.0,
        },
        {
            'Ready for Development': 0.5,
            'In Progress': 1.0,
            'Review': 0.5,
            'Ready for Testing': 0.5,
            'In Testing': 0.5,
            'To Deploy': 1.0,
            'Done': 0.0,
        },
    ]


# ============================================================================
# Tests for calculate_time_in_state
# ============================================================================

def test_calculate_time_in_state_normal_flow():
    """Test time calculation for a state with single entry/exit."""
    base_time = datetime(2025, 1, 1, 9, 0, 0)
    transitions = [
        {'to_state': 'In Progress', 'timestamp': base_time},
        {'to_state': 'Review', 'timestamp': base_time + timedelta(days=2)},
    ]

    result = calculate_time_in_state(
        transitions,
        'In Progress',
        ['Review', 'Done']
    )

    assert result == 2.0


def test_calculate_time_in_state_multiple_entries():
    """Test time calculation for a state with multiple entry/exit cycles."""
    base_time = datetime(2025, 1, 1, 9, 0, 0)
    transitions = [
        {'to_state': 'In Progress', 'timestamp': base_time},
        {'to_state': 'Review', 'timestamp': base_time + timedelta(days=1)},
        {'to_state': 'In Progress', 'timestamp': base_time + timedelta(days=2)},
        {'to_state': 'Done', 'timestamp': base_time + timedelta(days=4)},
    ]

    result = calculate_time_in_state(
        transitions,
        'In Progress',
        ['Review', 'Done']
    )

    assert result == 3.0  # 1 day + 2 days


def test_calculate_time_in_state_no_entry():
    """Test time calculation when state is never entered."""
    base_time = datetime(2025, 1, 1, 9, 0, 0)
    transitions = [
        {'to_state': 'In Progress', 'timestamp': base_time},
        {'to_state': 'Done', 'timestamp': base_time + timedelta(days=2)},
    ]

    result = calculate_time_in_state(
        transitions,
        'Review',
        ['Done']
    )

    assert result == 0.0


def test_calculate_time_in_state_partial_hours():
    """Test time calculation with partial days (hours)."""
    base_time = datetime(2025, 1, 1, 9, 0, 0)
    transitions = [
        {'to_state': 'Review', 'timestamp': base_time},
        {'to_state': 'Done', 'timestamp': base_time + timedelta(hours=12)},
    ]

    result = calculate_time_in_state(
        transitions,
        'Review',
        ['Done']
    )

    assert result == 0.5


# ============================================================================
# Tests for calculate_cycle_time
# ============================================================================

def test_calculate_cycle_time_complete_flow(sample_transitions):
    """Test cycle time calculation for complete workflow."""
    result = calculate_cycle_time(sample_transitions, STATES)

    assert result['Ready for Development'] == 1.0
    assert result['In Progress'] == 2.0
    assert result['Review'] == 1.0
    assert result['Ready for Testing'] == 1.0
    assert result['In Testing'] == 1.0
    assert result['To Deploy'] == 4.0
    assert result['Done'] == 0.0


def test_calculate_cycle_time_skipped_states():
    """Test cycle time when some states are skipped."""
    base_time = datetime(2025, 1, 1, 9, 0, 0)
    transitions = [
        {'to_state': 'In Progress', 'timestamp': base_time},
        {'to_state': 'Done', 'timestamp': base_time + timedelta(days=5)},
    ]

    result = calculate_cycle_time(transitions, STATES)

    assert result['In Progress'] == 5.0
    assert result['Review'] == 0.0
    assert result['Ready for Testing'] == 0.0


def test_calculate_cycle_time_empty_transitions():
    """Test cycle time with no transitions."""
    result = calculate_cycle_time([], STATES)

    for state in STATES:
        assert result[state] == 0.0


# ============================================================================
# Tests for calculate_total_cycle_time
# ============================================================================

def test_calculate_total_cycle_time(sample_cycle_time):
    """Test total cycle time calculation."""
    result = calculate_total_cycle_time(sample_cycle_time)
    assert result == 10.0


def test_calculate_total_cycle_time_zero():
    """Test total cycle time with all zeros."""
    cycle_time = {state: 0.0 for state in STATES}
    result = calculate_total_cycle_time(cycle_time)
    assert result == 0.0


def test_calculate_total_cycle_time_partial():
    """Test total cycle time with some states having time."""
    cycle_time = {
        'Ready for Development': 0.0,
        'In Progress': 3.5,
        'Review': 1.5,
        'Ready for Testing': 0.0,
        'In Testing': 0.0,
        'To Deploy': 0.0,
        'Done': 0.0,
    }
    result = calculate_total_cycle_time(cycle_time)
    assert result == 5.0


# ============================================================================
# Tests for calculate_deployment_wait_time
# ============================================================================

def test_calculate_deployment_wait_time(sample_cycle_time):
    """Test deployment wait time extraction."""
    result = calculate_deployment_wait_time(sample_cycle_time)
    assert result == 4.0


def test_calculate_deployment_wait_time_zero():
    """Test deployment wait time when To Deploy was not used."""
    cycle_time = {state: 0.0 for state in STATES}
    result = calculate_deployment_wait_time(cycle_time)
    assert result == 0.0


def test_calculate_deployment_wait_time_missing_key():
    """Test deployment wait time when To Deploy key is missing."""
    cycle_time = {'In Progress': 2.0}
    result = calculate_deployment_wait_time(cycle_time)
    assert result == 0.0


# ============================================================================
# Tests for calculate_summary_statistics
# ============================================================================

def test_calculate_summary_statistics(multiple_cycle_times):
    """Test summary statistics calculation."""
    result = calculate_summary_statistics(multiple_cycle_times)

    assert 'average_cycle_time' in result
    assert 'std_dev' in result
    assert 'median_cycle_time' in result
    assert 'min_cycle_time' in result
    assert 'max_cycle_time' in result
    assert 'average_deployment_wait' in result

    # Check reasonable values
    assert result['average_cycle_time'] > 0
    assert result['median_cycle_time'] == 10.0  # Middle value of 4, 10, 12
    assert result['min_cycle_time'] == 4.0
    assert result['max_cycle_time'] == 12.0


def test_calculate_summary_statistics_empty():
    """Test summary statistics with empty list."""
    result = calculate_summary_statistics([])

    assert result['average_cycle_time'] == 0.0
    assert result['std_dev'] == 0.0
    assert result['median_cycle_time'] == 0.0
    assert result['average_deployment_wait'] == 0.0


def test_calculate_summary_statistics_single_issue():
    """Test summary statistics with single issue."""
    cycle_times = [{
        'Ready for Development': 1.0,
        'In Progress': 2.0,
        'Review': 1.0,
        'Ready for Testing': 1.0,
        'In Testing': 1.0,
        'To Deploy': 4.0,
        'Done': 0.0,
    }]

    result = calculate_summary_statistics(cycle_times)

    assert result['average_cycle_time'] == 10.0
    assert result['min_cycle_time'] == 10.0
    assert result['max_cycle_time'] == 10.0
    assert result['average_deployment_wait'] == 4.0


# ============================================================================
# Tests for identify_bottlenecks
# ============================================================================

def test_identify_bottlenecks(multiple_cycle_times):
    """Test bottleneck identification."""
    result = identify_bottlenecks(multiple_cycle_times, STATES)

    # Should return dictionary with all states
    assert len(result) == len(STATES)

    # Should be sorted by time descending
    values = list(result.values())
    assert values == sorted(values, reverse=True)

    # Check specific bottleneck (To Deploy should be high)
    assert result['To Deploy'] > 0


def test_identify_bottlenecks_single_issue(sample_cycle_time):
    """Test bottleneck identification with single issue."""
    result = identify_bottlenecks([sample_cycle_time], STATES)

    # To Deploy should be the bottleneck
    bottleneck_state = list(result.keys())[0]
    assert bottleneck_state == 'To Deploy'
    assert result['To Deploy'] == 4.0


def test_identify_bottlenecks_empty():
    """Test bottleneck identification with empty list."""
    result = identify_bottlenecks([], STATES)

    # Should return all states with 0.0
    for state in STATES:
        assert result[state] == 0.0


# ============================================================================
# Tests for categorize_cycle_time
# ============================================================================

def test_categorize_cycle_time_ranges():
    """Test cycle time categorization across all ranges."""
    assert categorize_cycle_time(0.5) == '0-2 days'
    assert categorize_cycle_time(2.0) == '0-2 days'
    assert categorize_cycle_time(3.0) == '3-5 days'
    assert categorize_cycle_time(5.0) == '3-5 days'
    assert categorize_cycle_time(6.0) == '6-10 days'
    assert categorize_cycle_time(10.0) == '6-10 days'
    assert categorize_cycle_time(11.0) == '11-20 days'
    assert categorize_cycle_time(20.0) == '11-20 days'
    assert categorize_cycle_time(21.0) == '20+ days'
    assert categorize_cycle_time(100.0) == '20+ days'


def test_categorize_cycle_time_boundaries():
    """Test cycle time categorization at boundary conditions."""
    assert categorize_cycle_time(2.0) == '0-2 days'
    assert categorize_cycle_time(2.1) == '3-5 days'
    assert categorize_cycle_time(5.0) == '3-5 days'
    assert categorize_cycle_time(5.1) == '6-10 days'


def test_categorize_cycle_time_zero():
    """Test cycle time categorization with zero."""
    assert categorize_cycle_time(0.0) == '0-2 days'


# ============================================================================
# Tests for calculate_distribution
# ============================================================================

def test_calculate_distribution(multiple_cycle_times):
    """Test cycle time distribution calculation."""
    result = calculate_distribution(multiple_cycle_times)

    # Should have all categories
    expected_categories = ['0-2 days', '3-5 days', '6-10 days', '11-20 days', '20+ days']
    assert list(result.keys()) == expected_categories

    # Total count should match input
    assert sum(result.values()) == len(multiple_cycle_times)


def test_calculate_distribution_single_category():
    """Test distribution when all issues fall in one category."""
    cycle_times = [
        {state: 0.2 for state in STATES},
        {state: 0.3 for state in STATES},
        {state: 0.1 for state in STATES},
    ]

    result = calculate_distribution(cycle_times)

    # All should be in 0-2 days
    assert result['0-2 days'] == 3
    assert result['3-5 days'] == 0
    assert result['6-10 days'] == 0


def test_calculate_distribution_empty():
    """Test distribution with empty list."""
    result = calculate_distribution([])

    # Should have all categories with zero counts
    for count in result.values():
        assert count == 0


def test_calculate_distribution_varied():
    """Test distribution with issues across multiple categories."""
    cycle_times = [
        {'Ready for Development': 1.0, 'In Progress': 0.5, 'Review': 0.0, 'Ready for Testing': 0.0,
         'In Testing': 0.0, 'To Deploy': 0.0, 'Done': 0.0},  # 1.5 days -> 0-2 days
        {'Ready for Development': 2.0, 'In Progress': 2.0, 'Review': 0.5, 'Ready for Testing': 0.0,
         'In Testing': 0.0, 'To Deploy': 0.0, 'Done': 0.0},  # 4.5 days -> 3-5 days
        {'Ready for Development': 3.0, 'In Progress': 5.0, 'Review': 1.0, 'Ready for Testing': 0.0,
         'In Testing': 0.0, 'To Deploy': 0.0, 'Done': 0.0},  # 9 days -> 6-10 days
        {'Ready for Development': 5.0, 'In Progress': 10.0, 'Review': 2.0, 'Ready for Testing': 0.0,
         'In Testing': 0.0, 'To Deploy': 0.0, 'Done': 0.0},  # 17 days -> 11-20 days
        {'Ready for Development': 10.0, 'In Progress': 15.0, 'Review': 3.0, 'Ready for Testing': 0.0,
         'In Testing': 0.0, 'To Deploy': 0.0, 'Done': 0.0},  # 28 days -> 20+ days
    ]

    result = calculate_distribution(cycle_times)

    assert result['0-2 days'] == 1
    assert result['3-5 days'] == 1
    assert result['6-10 days'] == 1
    assert result['11-20 days'] == 1
    assert result['20+ days'] == 1


# ============================================================================
# Edge Cases and Error Handling
# ============================================================================

def test_calculate_cycle_time_out_of_order_transitions():
    """Test cycle time calculation handles out-of-order timestamps gracefully."""
    # This shouldn't happen in practice but test robustness
    base_time = datetime(2025, 1, 1, 9, 0, 0)
    transitions = [
        {'to_state': 'Done', 'timestamp': base_time + timedelta(days=5)},
        {'to_state': 'In Progress', 'timestamp': base_time},
    ]

    # Should not raise exception
    result = calculate_cycle_time(transitions, STATES)
    assert isinstance(result, dict)


def test_large_cycle_time_values():
    """Test handling of very large cycle time values."""
    cycle_time = {state: 1000.0 for state in STATES}

    total = calculate_total_cycle_time(cycle_time)
    assert total == 7000.0

    category = categorize_cycle_time(total)
    assert category == '20+ days'
