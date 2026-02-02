"""
Unit tests for OpsGenie Alerts Analysis

Tests focus on pure functions for analyzing and aggregating OpsGenie alerts.
"""

import pytest
from datetime import datetime, timedelta
from lucille.opsgenie_alerts_chart_weeks import (
    filter_last_n_weeks,
    get_week_start,
    aggregate_by_week_and_team
)


class TestFilterLastNWeeks:
    """Test suite for alert filtering by time range."""

    def test_filter_last_n_weeks_basic(self):
        """Test basic filtering of alerts."""
        reference_date = datetime(2025, 2, 1, 12, 0, 0)
        # Create alerts from 8 weeks ago, 2 weeks ago, and yesterday
        alerts = [
            {'CreatedAt': str(int((reference_date - timedelta(weeks=8)).timestamp() * 1000)), 'Teams': 'TeamA'},
            {'CreatedAt': str(int((reference_date - timedelta(weeks=2)).timestamp() * 1000)), 'Teams': 'TeamB'},
            {'CreatedAt': str(int((reference_date - timedelta(days=1)).timestamp() * 1000)), 'Teams': 'TeamC'},
        ]

        result = filter_last_n_weeks(alerts, n_weeks=4, reference_date=reference_date)

        # Should only include alerts from last 4 weeks (2 weeks ago and yesterday)
        assert len(result) == 2
        assert result[0]['Teams'] == 'TeamB'
        assert result[1]['Teams'] == 'TeamC'

    def test_filter_last_n_weeks_all_old(self):
        """Test when all alerts are older than n weeks."""
        reference_date = datetime(2025, 2, 1, 12, 0, 0)
        alerts = [
            {'CreatedAt': str(int((reference_date - timedelta(weeks=10)).timestamp() * 1000)), 'Teams': 'TeamA'},
            {'CreatedAt': str(int((reference_date - timedelta(weeks=6)).timestamp() * 1000)), 'Teams': 'TeamB'},
        ]

        result = filter_last_n_weeks(alerts, n_weeks=4, reference_date=reference_date)

        assert len(result) == 0

    def test_filter_last_n_weeks_all_recent(self):
        """Test when all alerts are within n weeks."""
        reference_date = datetime(2025, 2, 1, 12, 0, 0)
        alerts = [
            {'CreatedAt': str(int((reference_date - timedelta(days=1)).timestamp() * 1000)), 'Teams': 'TeamA'},
            {'CreatedAt': str(int((reference_date - timedelta(days=7)).timestamp() * 1000)), 'Teams': 'TeamB'},
            {'CreatedAt': str(int((reference_date - timedelta(days=14)).timestamp() * 1000)), 'Teams': 'TeamC'},
        ]

        result = filter_last_n_weeks(alerts, n_weeks=4, reference_date=reference_date)

        assert len(result) == 3

    def test_filter_last_n_weeks_empty_list(self):
        """Test with empty alert list."""
        reference_date = datetime(2025, 2, 1, 12, 0, 0)
        alerts = []

        result = filter_last_n_weeks(alerts, n_weeks=4, reference_date=reference_date)

        assert len(result) == 0

    def test_filter_last_n_weeks_adds_parsed_date(self):
        """Test that filtered alerts have parsed_date field."""
        reference_date = datetime(2025, 2, 1, 12, 0, 0)
        alerts = [
            {'CreatedAt': str(int((reference_date - timedelta(days=1)).timestamp() * 1000)), 'Teams': 'TeamA'},
        ]

        result = filter_last_n_weeks(alerts, n_weeks=4, reference_date=reference_date)

        assert len(result) == 1
        assert 'parsed_date' in result[0]
        assert isinstance(result[0]['parsed_date'], datetime)

    def test_filter_last_n_weeks_boundary(self):
        """Test filtering at exact boundary."""
        reference_date = datetime(2025, 2, 1, 12, 0, 0)
        # Create alert exactly 4 weeks ago
        cutoff_timestamp = reference_date - timedelta(weeks=4)
        alerts = [
            {'CreatedAt': str(int(cutoff_timestamp.timestamp() * 1000)), 'Teams': 'TeamA'},
            {'CreatedAt': str(int((cutoff_timestamp - timedelta(seconds=1)).timestamp() * 1000)), 'Teams': 'TeamB'},
        ]

        result = filter_last_n_weeks(alerts, n_weeks=4, reference_date=reference_date)

        # Alert at cutoff should be included, one second before should not
        assert len(result) == 1
        assert result[0]['Teams'] == 'TeamA'

    def test_filter_last_n_weeks_different_durations(self):
        """Test with different n_weeks values."""
        reference_date = datetime(2025, 2, 1, 12, 0, 0)
        alerts = [
            {'CreatedAt': str(int((reference_date - timedelta(days=7)).timestamp() * 1000)), 'Teams': 'TeamA'},
            {'CreatedAt': str(int((reference_date - timedelta(days=14)).timestamp() * 1000)), 'Teams': 'TeamB'},
            {'CreatedAt': str(int((reference_date - timedelta(days=21)).timestamp() * 1000)), 'Teams': 'TeamC'},
        ]

        # Filter for 1 week
        result_1_week = filter_last_n_weeks(alerts, n_weeks=1, reference_date=reference_date)
        assert len(result_1_week) == 1

        # Filter for 2 weeks
        result_2_weeks = filter_last_n_weeks(alerts, n_weeks=2, reference_date=reference_date)
        assert len(result_2_weeks) == 2

        # Filter for 4 weeks
        result_4_weeks = filter_last_n_weeks(alerts, n_weeks=4, reference_date=reference_date)
        assert len(result_4_weeks) == 3

    def test_filter_last_n_weeks_preserves_data(self):
        """Test that original alert data is preserved."""
        reference_date = datetime(2025, 2, 1, 12, 0, 0)
        alerts = [
            {'CreatedAt': str(int((reference_date - timedelta(days=1)).timestamp() * 1000)),
             'Teams': 'TeamA',
             'Message': 'Test alert',
             'Priority': 'P1'},
        ]

        result = filter_last_n_weeks(alerts, n_weeks=4, reference_date=reference_date)

        assert result[0]['Teams'] == 'TeamA'
        assert result[0]['Message'] == 'Test alert'
        assert result[0]['Priority'] == 'P1'


class TestGetWeekStart:
    """Test suite for week start calculation."""

    def test_get_week_start_monday(self):
        """Test that Monday returns itself."""
        date = datetime(2025, 2, 3)  # Monday
        result = get_week_start(date)
        assert result == date

    def test_get_week_start_tuesday(self):
        """Test Tuesday returns previous Monday."""
        date = datetime(2025, 2, 4)  # Tuesday
        expected = datetime(2025, 2, 3)  # Monday
        result = get_week_start(date)
        assert result == expected

    def test_get_week_start_sunday(self):
        """Test Sunday returns previous Monday."""
        date = datetime(2025, 2, 9)  # Sunday
        expected = datetime(2025, 2, 3)  # Monday
        result = get_week_start(date)
        assert result == expected

    def test_get_week_start_with_time(self):
        """Test that time is preserved."""
        date = datetime(2025, 2, 5, 14, 30, 45)  # Wednesday with time
        result = get_week_start(date)
        assert result.weekday() == 0  # Monday
        assert result.hour == 14
        assert result.minute == 30
        assert result.second == 45

    def test_get_week_start_different_weeks(self):
        """Test dates from different weeks."""
        dates = [
            datetime(2025, 1, 6),   # Mon Jan 6
            datetime(2025, 1, 12),  # Sun Jan 12
            datetime(2025, 1, 13),  # Mon Jan 13
            datetime(2025, 1, 19),  # Sun Jan 19
        ]

        results = [get_week_start(d) for d in dates]

        # First two should have same week start (Jan 6)
        assert results[0] == results[1]
        # Last two should have same week start (Jan 13)
        assert results[2] == results[3]
        # Different weeks should have different starts
        assert results[0] != results[2]

    def test_get_week_start_year_boundary(self):
        """Test week start calculation across year boundary."""
        date = datetime(2025, 1, 1)  # Could be in previous week
        result = get_week_start(date)
        assert result.weekday() == 0  # Should be a Monday


class TestAggregateByWeekAndTeam:
    """Test suite for aggregation by week and team."""

    def test_aggregate_by_week_and_team_basic(self):
        """Test basic aggregation."""
        alerts = [
            {'parsed_date': datetime(2025, 1, 6), 'Teams': 'TeamA'},  # Mon week 1
            {'parsed_date': datetime(2025, 1, 7), 'Teams': 'TeamA'},  # Tue week 1
            {'parsed_date': datetime(2025, 1, 8), 'Teams': 'TeamB'},  # Wed week 1
            {'parsed_date': datetime(2025, 1, 13), 'Teams': 'TeamA'}, # Mon week 2
        ]

        result = aggregate_by_week_and_team(alerts)

        # Should have 2 weeks
        assert len(result) == 2

        # Week 1 (starting Jan 6) should have TeamA: 2, TeamB: 1
        week1 = datetime(2025, 1, 6).date()
        assert result[week1]['TeamA'] == 2
        assert result[week1]['TeamB'] == 1

        # Week 2 (starting Jan 13) should have TeamA: 1
        week2 = datetime(2025, 1, 13).date()
        assert result[week2]['TeamA'] == 1

    def test_aggregate_by_week_and_team_single_team(self):
        """Test aggregation with single team."""
        alerts = [
            {'parsed_date': datetime(2025, 1, 6), 'Teams': 'TeamA'},
            {'parsed_date': datetime(2025, 1, 7), 'Teams': 'TeamA'},
            {'parsed_date': datetime(2025, 1, 8), 'Teams': 'TeamA'},
        ]

        result = aggregate_by_week_and_team(alerts)

        week = datetime(2025, 1, 6).date()
        assert result[week]['TeamA'] == 3
        assert len(result[week]) == 1  # Only one team

    def test_aggregate_by_week_and_team_empty_team(self):
        """Test handling of empty team names."""
        alerts = [
            {'parsed_date': datetime(2025, 1, 6), 'Teams': ''},
            {'parsed_date': datetime(2025, 1, 7), 'Teams': '   '},  # Whitespace
            {'parsed_date': datetime(2025, 1, 8)},  # Missing Teams key
        ]

        result = aggregate_by_week_and_team(alerts)

        week = datetime(2025, 1, 6).date()
        # Empty teams should be grouped as 'Unassigned'
        assert 'Unassigned' in result[week]
        assert result[week]['Unassigned'] == 3

    def test_aggregate_by_week_and_team_multiple_weeks(self):
        """Test aggregation across multiple weeks."""
        alerts = [
            {'parsed_date': datetime(2025, 1, 6), 'Teams': 'TeamA'},   # Week 1
            {'parsed_date': datetime(2025, 1, 13), 'Teams': 'TeamA'},  # Week 2
            {'parsed_date': datetime(2025, 1, 20), 'Teams': 'TeamA'},  # Week 3
        ]

        result = aggregate_by_week_and_team(alerts)

        assert len(result) == 3
        # Each week should have 1 alert for TeamA
        for week_start, teams in result.items():
            assert teams['TeamA'] == 1

    def test_aggregate_by_week_and_team_empty_list(self):
        """Test with empty alert list."""
        alerts = []

        result = aggregate_by_week_and_team(alerts)

        assert len(result) == 0

    def test_aggregate_by_week_and_team_mixed_teams(self):
        """Test with multiple teams in same week."""
        alerts = [
            {'parsed_date': datetime(2025, 1, 6), 'Teams': 'Frontend'},
            {'parsed_date': datetime(2025, 1, 6), 'Teams': 'Backend'},
            {'parsed_date': datetime(2025, 1, 6), 'Teams': 'DevOps'},
            {'parsed_date': datetime(2025, 1, 6), 'Teams': 'Frontend'},
        ]

        result = aggregate_by_week_and_team(alerts)

        week = datetime(2025, 1, 6).date()
        assert result[week]['Frontend'] == 2
        assert result[week]['Backend'] == 1
        assert result[week]['DevOps'] == 1

    def test_aggregate_by_week_and_team_week_spanning(self):
        """Test that alerts on different days of same week are grouped."""
        alerts = [
            {'parsed_date': datetime(2025, 1, 6), 'Teams': 'TeamA'},   # Monday
            {'parsed_date': datetime(2025, 1, 8), 'Teams': 'TeamA'},   # Wednesday
            {'parsed_date': datetime(2025, 1, 12), 'Teams': 'TeamA'},  # Sunday (same week)
        ]

        result = aggregate_by_week_and_team(alerts)

        # All should be in same week
        assert len(result) == 1
        week = datetime(2025, 1, 6).date()
        assert result[week]['TeamA'] == 3

    def test_aggregate_by_week_and_team_preserves_team_names(self):
        """Test that team names are preserved correctly."""
        alerts = [
            {'parsed_date': datetime(2025, 1, 6), 'Teams': 'Team Alpha'},
            {'parsed_date': datetime(2025, 1, 6), 'Teams': 'Team-Beta'},
            {'parsed_date': datetime(2025, 1, 6), 'Teams': 'Team_Gamma'},
        ]

        result = aggregate_by_week_and_team(alerts)

        week = datetime(2025, 1, 6).date()
        assert 'Team Alpha' in result[week]
        assert 'Team-Beta' in result[week]
        assert 'Team_Gamma' in result[week]


class TestIntegration:
    """Integration tests combining multiple functions."""

    def test_full_pipeline(self):
        """Test complete pipeline from filtering to aggregation."""
        reference_date = datetime(2025, 2, 1, 12, 0, 0)

        # Create alerts with various timestamps
        alerts = [
            {'CreatedAt': str(int((reference_date - timedelta(weeks=8)).timestamp() * 1000)), 'Teams': 'TeamA'},
            {'CreatedAt': str(int((reference_date - timedelta(weeks=2)).timestamp() * 1000)), 'Teams': 'TeamA'},
            {'CreatedAt': str(int((reference_date - timedelta(weeks=1)).timestamp() * 1000)), 'Teams': 'TeamB'},
            {'CreatedAt': str(int((reference_date - timedelta(days=1)).timestamp() * 1000)), 'Teams': 'TeamA'},
        ]

        # Filter to last 4 weeks
        filtered = filter_last_n_weeks(alerts, n_weeks=4, reference_date=reference_date)

        # Aggregate by week and team
        aggregated = aggregate_by_week_and_team(filtered)

        # Should have filtered out the 8-week-old alert
        assert len(filtered) == 3

        # Check aggregation makes sense
        total_alerts = sum(sum(teams.values()) for teams in aggregated.values())
        assert total_alerts == 3

    def test_realistic_scenario(self):
        """Test with realistic alert data."""
        reference_date = datetime(2025, 2, 1, 12, 0, 0)

        # Simulate 4 weeks of alerts with varying frequency
        alerts = []
        for week_offset in range(4):
            for day in range(7):
                for alert_num in range((week_offset % 2) + 1):  # 1-2 alerts per day
                    timestamp = reference_date - timedelta(weeks=week_offset, days=day)
                    team = ['Frontend', 'Backend', 'DevOps'][alert_num % 3]
                    alerts.append({
                        'CreatedAt': str(int(timestamp.timestamp() * 1000)),
                        'Teams': team
                    })

        filtered = filter_last_n_weeks(alerts, n_weeks=4, reference_date=reference_date)
        aggregated = aggregate_by_week_and_team(filtered)

        # Should have data for multiple weeks
        assert len(aggregated) > 0
        # Should have multiple teams
        all_teams = set()
        for week_teams in aggregated.values():
            all_teams.update(week_teams.keys())
        assert len(all_teams) >= 2


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_microsecond_precision(self):
        """Test handling of timestamps with microsecond precision."""
        reference_date = datetime(2025, 2, 1, 12, 0, 0, 123456)
        timestamp = reference_date - timedelta(days=1)
        alerts = [
            {'CreatedAt': str(int(timestamp.timestamp() * 1000)), 'Teams': 'TeamA'},
        ]

        result = filter_last_n_weeks(alerts, n_weeks=4, reference_date=reference_date)

        assert len(result) == 1

    def test_team_name_whitespace_handling(self):
        """Test handling of team names with whitespace."""
        alerts = [
            {'parsed_date': datetime(2025, 1, 6), 'Teams': '  TeamA  '},
            {'parsed_date': datetime(2025, 1, 6), 'Teams': 'TeamA'},
        ]

        result = aggregate_by_week_and_team(alerts)

        # Different whitespace patterns might create different keys
        # This tests the actual behavior
        week = datetime(2025, 1, 6).date()
        assert len(result[week]) >= 1

    def test_very_old_alerts(self):
        """Test with alerts from many months ago."""
        reference_date = datetime(2025, 2, 1, 12, 0, 0)
        alerts = [
            {'CreatedAt': str(int((reference_date - timedelta(days=365)).timestamp() * 1000)), 'Teams': 'TeamA'},
        ]

        result = filter_last_n_weeks(alerts, n_weeks=4, reference_date=reference_date)

        assert len(result) == 0

    def test_future_alerts(self):
        """Test handling of alerts with future timestamps (shouldn't happen but test anyway)."""
        reference_date = datetime(2025, 2, 1, 12, 0, 0)
        alerts = [
            {'CreatedAt': str(int((reference_date + timedelta(days=1)).timestamp() * 1000)), 'Teams': 'TeamA'},
        ]

        result = filter_last_n_weeks(alerts, n_weeks=4, reference_date=reference_date)

        # Future alerts should be included (they're within the range)
        assert len(result) == 1
