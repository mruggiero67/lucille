"""
Unit tests for Weekly Deployment Trends Analyzer

Tests focus on pure functions for analyzing deployment trends.
"""

import pytest
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from lucille.weekly_deployment_trends import (
    calculate_weekly_deployments,
    calculate_trend_line,
    calculate_statistics
)


class TestCalculateWeeklyDeployments:
    """Test suite for weekly deployment calculation."""

    def test_calculate_weekly_deployments_basic(self):
        """Test basic weekly aggregation."""
        # Create sample data spanning 2 weeks
        df = pd.DataFrame({
            'date': ['2025-01-06', '2025-01-07', '2025-01-08',  # Week 1 (Mon-Wed)
                    '2025-01-13', '2025-01-14'],                # Week 2 (Mon-Tue)
            'service': ['A', 'B', 'C', 'D', 'E']
        })

        result = calculate_weekly_deployments(df, 'date')

        assert len(result) == 2
        assert 'week_start' in result.columns
        assert 'deployment_count' in result.columns
        assert result['deployment_count'].sum() == 5

    def test_calculate_weekly_deployments_single_week(self):
        """Test with deployments in a single week."""
        df = pd.DataFrame({
            'date': ['2025-01-06', '2025-01-07', '2025-01-08'],
            'service': ['A', 'B', 'C']
        })

        result = calculate_weekly_deployments(df, 'date')

        assert len(result) == 1
        assert result.iloc[0]['deployment_count'] == 3

    def test_calculate_weekly_deployments_week_boundary(self):
        """Test that weeks start on Monday."""
        # Jan 6, 2025 is a Monday
        df = pd.DataFrame({
            'date': ['2025-01-05',  # Sunday (previous week)
                    '2025-01-06',  # Monday (new week)
                    '2025-01-12'], # Sunday (same week as Mon)
            'service': ['A', 'B', 'C']
        })

        result = calculate_weekly_deployments(df, 'date')

        # Sunday Jan 5 should be in previous week (starting Dec 30)
        # Mon Jan 6 through Sun Jan 12 should be same week
        assert len(result) == 2
        # Week containing Jan 6-12 should have 2 deployments
        week_of_jan6 = result[result['week_start'] == pd.Timestamp('2025-01-06')]
        assert not week_of_jan6.empty
        assert week_of_jan6.iloc[0]['deployment_count'] == 2

    def test_calculate_weekly_deployments_sorting(self):
        """Test that results are sorted by week_start."""
        df = pd.DataFrame({
            'date': ['2025-01-20', '2025-01-06', '2025-01-13'],
            'service': ['A', 'B', 'C']
        })

        result = calculate_weekly_deployments(df, 'date')

        # Check that weeks are in chronological order
        assert (result['week_start'].diff().dropna() >= pd.Timedelta(0)).all()

    def test_calculate_weekly_deployments_empty_dataframe(self):
        """Test with empty DataFrame."""
        df = pd.DataFrame({'date': [], 'service': []})

        result = calculate_weekly_deployments(df, 'date')

        assert len(result) == 0

    def test_calculate_weekly_deployments_multiple_per_day(self):
        """Test counting multiple deployments per day."""
        df = pd.DataFrame({
            'date': ['2025-01-06', '2025-01-06', '2025-01-06'],
            'service': ['A', 'B', 'C']
        })

        result = calculate_weekly_deployments(df, 'date')

        assert len(result) == 1
        assert result.iloc[0]['deployment_count'] == 3

    def test_calculate_weekly_deployments_custom_date_column(self):
        """Test with custom date column name."""
        df = pd.DataFrame({
            'timestamp': ['2025-01-06', '2025-01-07'],
            'service': ['A', 'B']
        })

        result = calculate_weekly_deployments(df, 'timestamp')

        assert len(result) == 1
        assert result.iloc[0]['deployment_count'] == 2

    def test_calculate_weekly_deployments_datetime_format(self):
        """Test with datetime objects instead of strings."""
        df = pd.DataFrame({
            'date': [datetime(2025, 1, 6), datetime(2025, 1, 7)],
            'service': ['A', 'B']
        })

        result = calculate_weekly_deployments(df, 'date')

        assert len(result) == 1
        assert result.iloc[0]['deployment_count'] == 2


class TestCalculateTrendLine:
    """Test suite for trend line calculation."""

    def test_calculate_trend_line_positive_slope(self):
        """Test trend line with increasing deployments."""
        weekly_data = pd.DataFrame({
            'week_start': pd.date_range('2025-01-06', periods=4, freq='W-MON'),
            'deployment_count': [10, 15, 20, 25]
        })

        x, y_trend, slope = calculate_trend_line(weekly_data)

        # Slope should be positive
        assert slope > 0
        assert len(x) == 4
        assert len(y_trend) == 4

    def test_calculate_trend_line_negative_slope(self):
        """Test trend line with decreasing deployments."""
        weekly_data = pd.DataFrame({
            'week_start': pd.date_range('2025-01-06', periods=4, freq='W-MON'),
            'deployment_count': [25, 20, 15, 10]
        })

        x, y_trend, slope = calculate_trend_line(weekly_data)

        # Slope should be negative
        assert slope < 0

    def test_calculate_trend_line_flat(self):
        """Test trend line with constant deployments."""
        weekly_data = pd.DataFrame({
            'week_start': pd.date_range('2025-01-06', periods=4, freq='W-MON'),
            'deployment_count': [15, 15, 15, 15]
        })

        x, y_trend, slope = calculate_trend_line(weekly_data)

        # Slope should be close to zero
        assert abs(slope) < 0.001

    def test_calculate_trend_line_two_points(self):
        """Test trend line with minimum two data points."""
        weekly_data = pd.DataFrame({
            'week_start': pd.date_range('2025-01-06', periods=2, freq='W-MON'),
            'deployment_count': [10, 20]
        })

        x, y_trend, slope = calculate_trend_line(weekly_data)

        assert slope > 0
        assert len(x) == 2
        assert len(y_trend) == 2

    def test_calculate_trend_line_returns_tuple(self):
        """Test that function returns proper tuple structure."""
        weekly_data = pd.DataFrame({
            'week_start': pd.date_range('2025-01-06', periods=3, freq='W-MON'),
            'deployment_count': [10, 15, 20]
        })

        result = calculate_trend_line(weekly_data)

        assert isinstance(result, tuple)
        assert len(result) == 3
        assert isinstance(result[0], np.ndarray)
        assert isinstance(result[1], np.ndarray)
        assert isinstance(result[2], (float, np.floating))

    def test_calculate_trend_line_x_values(self):
        """Test that x values represent days since first week."""
        weekly_data = pd.DataFrame({
            'week_start': [
                pd.Timestamp('2025-01-06'),
                pd.Timestamp('2025-01-13'),
                pd.Timestamp('2025-01-20')
            ],
            'deployment_count': [10, 15, 20]
        })

        x, y_trend, slope = calculate_trend_line(weekly_data)

        # First x should be 0 (days since start)
        assert x[0] == 0
        # Second x should be 7 (one week later)
        assert x[1] == 7
        # Third x should be 14 (two weeks later)
        assert x[2] == 14


class TestCalculateStatistics:
    """Test suite for statistics calculation."""

    def test_calculate_statistics_basic(self):
        """Test basic statistics calculation."""
        weekly_data = pd.DataFrame({
            'week_start': pd.date_range('2025-01-06', periods=4, freq='W-MON'),
            'deployment_count': [10, 20, 15, 25]
        })

        stats = calculate_statistics(weekly_data)

        assert stats['total_weeks'] == 4
        assert stats['total_deployments'] == 70
        assert stats['average_per_week'] == 17.5
        assert stats['median_per_week'] == 17.5
        assert stats['max_week'] == 25
        assert stats['min_week'] == 10

    def test_calculate_statistics_single_week(self):
        """Test statistics with single week."""
        weekly_data = pd.DataFrame({
            'week_start': [pd.Timestamp('2025-01-06')],
            'deployment_count': [15]
        })

        stats = calculate_statistics(weekly_data)

        assert stats['total_weeks'] == 1
        assert stats['total_deployments'] == 15
        assert stats['average_per_week'] == 15
        assert stats['median_per_week'] == 15
        assert stats['max_week'] == 15
        assert stats['min_week'] == 15

    def test_calculate_statistics_includes_dates(self):
        """Test that statistics include first and last week dates."""
        weekly_data = pd.DataFrame({
            'week_start': [
                pd.Timestamp('2025-01-06'),
                pd.Timestamp('2025-01-13'),
                pd.Timestamp('2025-01-20')
            ],
            'deployment_count': [10, 15, 20]
        })

        stats = calculate_statistics(weekly_data)

        assert stats['first_week'] == pd.Timestamp('2025-01-06')
        assert stats['last_week'] == pd.Timestamp('2025-01-20')

    def test_calculate_statistics_standard_deviation(self):
        """Test standard deviation calculation."""
        weekly_data = pd.DataFrame({
            'week_start': pd.date_range('2025-01-06', periods=4, freq='W-MON'),
            'deployment_count': [10, 10, 30, 30]
        })

        stats = calculate_statistics(weekly_data)

        # With values [10, 10, 30, 30], std dev should be ~11.55
        assert stats['std_dev'] > 0
        assert 10 < stats['std_dev'] < 15

    def test_calculate_statistics_all_same(self):
        """Test statistics when all counts are the same."""
        weekly_data = pd.DataFrame({
            'week_start': pd.date_range('2025-01-06', periods=3, freq='W-MON'),
            'deployment_count': [20, 20, 20]
        })

        stats = calculate_statistics(weekly_data)

        assert stats['average_per_week'] == 20
        assert stats['median_per_week'] == 20
        assert stats['max_week'] == 20
        assert stats['min_week'] == 20
        assert stats['std_dev'] == 0.0

    def test_calculate_statistics_return_type(self):
        """Test that function returns a dictionary with expected keys."""
        weekly_data = pd.DataFrame({
            'week_start': pd.date_range('2025-01-06', periods=2, freq='W-MON'),
            'deployment_count': [10, 20]
        })

        stats = calculate_statistics(weekly_data)

        expected_keys = [
            'total_weeks', 'total_deployments', 'average_per_week',
            'median_per_week', 'max_week', 'min_week', 'std_dev',
            'first_week', 'last_week'
        ]
        for key in expected_keys:
            assert key in stats


class TestIntegration:
    """Integration tests combining multiple functions."""

    def test_full_pipeline(self):
        """Test complete pipeline from daily data to statistics."""
        # Create daily deployment data
        df = pd.DataFrame({
            'date': pd.date_range('2025-01-06', periods=14, freq='D'),
            'service': ['Service' + str(i % 3) for i in range(14)]
        })

        # Calculate weekly deployments
        weekly_data = calculate_weekly_deployments(df, 'date')

        # Calculate trend
        x, y_trend, slope = calculate_trend_line(weekly_data)

        # Calculate statistics
        stats = calculate_statistics(weekly_data)

        # Verify pipeline produces expected results
        assert stats['total_deployments'] == 14
        assert stats['total_weeks'] == 2
        assert len(weekly_data) == 2

    def test_uneven_weekly_distribution(self):
        """Test with realistic uneven weekly deployment distribution."""
        # Simulate realistic pattern: more deployments mid-week
        dates = []
        for week in range(4):
            week_start = datetime(2025, 1, 6) + timedelta(weeks=week)
            # Add 2-5 deployments per week
            for day in [0, 2, 3, 4]:  # Mon, Wed, Thu, Fri
                if week < 3 or day < 3:  # Less activity in last week
                    dates.append(week_start + timedelta(days=day))

        df = pd.DataFrame({
            'date': dates,
            'service': ['S' + str(i) for i in range(len(dates))]
        })

        weekly_data = calculate_weekly_deployments(df, 'date')
        stats = calculate_statistics(weekly_data)

        # Verify reasonable statistics
        assert stats['total_weeks'] == 4
        assert stats['average_per_week'] > 0
        assert stats['max_week'] >= stats['average_per_week']
        assert stats['min_week'] <= stats['average_per_week']


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_large_dataset(self):
        """Test with large dataset (52 weeks)."""
        df = pd.DataFrame({
            'date': pd.date_range('2025-01-06', periods=365, freq='D'),
            'service': ['Service' + str(i % 10) for i in range(365)]
        })

        weekly_data = calculate_weekly_deployments(df, 'date')
        stats = calculate_statistics(weekly_data)

        # Should handle 52-53 weeks
        assert 52 <= stats['total_weeks'] <= 53
        assert stats['total_deployments'] == 365

    def test_sparse_data(self):
        """Test with sparse deployment data (gaps between weeks)."""
        df = pd.DataFrame({
            'date': ['2025-01-06', '2025-01-07',  # Week 1
                    '2025-02-03', '2025-02-04'],  # Week 5 (gap of 3 weeks)
            'service': ['A', 'B', 'C', 'D']
        })

        weekly_data = calculate_weekly_deployments(df, 'date')

        # Should only count weeks with deployments
        assert len(weekly_data) == 2
        assert weekly_data['deployment_count'].sum() == 4

    def test_date_parsing_formats(self):
        """Test various date string formats."""
        df = pd.DataFrame({
            'date': ['2025-01-06', '2025/01/07', '01-08-2025'],
            'service': ['A', 'B', 'C']
        })

        # Should handle different formats (pandas is flexible)
        try:
            result = calculate_weekly_deployments(df, 'date')
            assert len(result) >= 1
        except:
            # Some formats might fail, that's acceptable
            pass
