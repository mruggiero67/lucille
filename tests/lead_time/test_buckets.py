"""Unit tests for lucille.lead_time.buckets (pure functions only)."""
from context import lucille  # noqa: F401
from lucille.lead_time.buckets import assign_bucket, bucket_colors, bucket_labels


class TestAssignBucket:
    def test_zero_hours(self):
        assert assign_bucket(0.0) == "<4 hours"

    def test_within_4_hours(self):
        assert assign_bucket(3.9) == "<4 hours"

    def test_boundary_4_hours(self):
        assert assign_bucket(4.0) == "4-24 hours"

    def test_midpoint_4_to_24(self):
        assert assign_bucket(12.0) == "4-24 hours"

    def test_just_before_24(self):
        assert assign_bucket(23.9) == "4-24 hours"

    def test_boundary_24_hours(self):
        assert assign_bucket(24.0) == "1-3 days"

    def test_midpoint_1_to_3_days(self):
        assert assign_bucket(48.0) == "1-3 days"

    def test_just_before_3_days(self):
        assert assign_bucket(71.9) == "1-3 days"

    def test_boundary_3_days(self):
        assert assign_bucket(72.0) == "3-7 days"

    def test_midpoint_3_to_7_days(self):
        assert assign_bucket(120.0) == "3-7 days"

    def test_just_before_7_days(self):
        assert assign_bucket(167.9) == "3-7 days"

    def test_boundary_7_days(self):
        assert assign_bucket(168.0) == "7-14 days"

    def test_midpoint_7_to_14_days(self):
        assert assign_bucket(240.0) == "7-14 days"

    def test_just_before_14_days(self):
        assert assign_bucket(335.9) == "7-14 days"

    def test_boundary_14_days(self):
        assert assign_bucket(336.0) == "14-30 days"

    def test_midpoint_14_to_30_days(self):
        assert assign_bucket(500.0) == "14-30 days"

    def test_just_before_30_days(self):
        assert assign_bucket(719.9) == "14-30 days"

    def test_boundary_30_days(self):
        assert assign_bucket(720.0) == "30+ days"

    def test_extreme_outlier(self):
        assert assign_bucket(8760.0) == "30+ days"


class TestBucketLabels:
    def test_returns_list(self):
        assert isinstance(bucket_labels(), list)

    def test_seven_buckets(self):
        assert len(bucket_labels()) == 7

    def test_first_label(self):
        assert bucket_labels()[0] == "<4 hours"

    def test_last_label(self):
        assert bucket_labels()[-1] == "30+ days"

    def test_returns_independent_copy(self):
        labels1 = bucket_labels()
        labels2 = bucket_labels()
        labels1[0] = "mutated"
        assert labels2[0] != "mutated"


class TestBucketColors:
    def test_same_length_as_labels(self):
        assert len(bucket_colors()) == len(bucket_labels())

    def test_all_hex_strings(self):
        for color in bucket_colors():
            assert isinstance(color, str)
            assert color.startswith("#")

    def test_returns_independent_copy(self):
        colors1 = bucket_colors()
        colors2 = bucket_colors()
        colors1[0] = "mutated"
        assert colors2[0] != "mutated"
