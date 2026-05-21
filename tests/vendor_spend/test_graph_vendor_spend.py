"""Unit tests for vendor_spend.graph_vendor_spend."""

from pathlib import Path

import pandas as pd
import pytest

from lucille.vendor_spend.graph_vendor_spend import (
    DEFAULT_VENDOR_COLORS,
    FALLBACK_VENDOR_COLOR,
    _png_path_for_csv,
    build_dataframe,
    color_for_vendor,
    pivot_for_plot,
    render_chart,
    render_per_vendor_charts,
    summarise,
    vendor_filename_slug,
)


@pytest.fixture
def sample_csv(tmp_path) -> Path:
    p = tmp_path / "2026_05_01_vendor_spend.csv"
    p.write_text(
        "week_start,vendor,amount_usd,source,fetched_at\n"
        "2026-04-13,AWS,100.00,src,STAMP\n"
        "2026-04-13,Datadog,50.00,src,STAMP\n"
        "2026-04-13,Databricks,25.00,src,STAMP\n"
        "2026-04-20,AWS,110.00,src,STAMP\n"
        "2026-04-20,Datadog,55.00,src,STAMP\n"
        "2026-04-20,Databricks,30.00,src,STAMP\n"
    )
    return p


class TestBuildDataframe:
    def test_columns_and_types(self, sample_csv):
        df = build_dataframe(sample_csv)
        assert set(df.columns) >= {"week_start", "vendor", "amount_usd"}
        assert df["amount_usd"].dtype == float
        assert len(df) == 6

    def test_sorted(self, sample_csv):
        df = build_dataframe(sample_csv)
        assert list(df["week_start"]) == sorted(df["week_start"])


class TestPivotForPlot:
    def test_pivot_shape(self, sample_csv):
        wide = pivot_for_plot(build_dataframe(sample_csv))
        assert wide.shape == (2, 3)  # 2 weeks x 3 vendors
        assert set(wide.columns) == {"AWS", "Databricks", "Datadog"}

    def test_pivot_values(self, sample_csv):
        wide = pivot_for_plot(build_dataframe(sample_csv))
        assert wide.loc[wide.index[0], "AWS"] == 100.0
        assert wide.loc[wide.index[1], "Datadog"] == 55.0


class TestSummarise:
    def test_summary_shape(self, sample_csv):
        s = summarise(build_dataframe(sample_csv))
        assert s["weeks"] == ["2026-04-13", "2026-04-20"]
        assert set(s["vendors"]) == {"AWS", "Databricks", "Datadog"}
        assert s["totals_by_vendor"]["AWS"] == 210.0
        assert s["by_vendor"]["Datadog"] == [50.0, 55.0]


class TestRenderChart:
    def test_writes_png_and_returns_summary(self, sample_csv, tmp_path):
        out = tmp_path / "out.png"
        summary = render_chart(build_dataframe(sample_csv), out)
        assert out.exists()
        assert out.stat().st_size > 0
        assert summary["totals_by_vendor"]["AWS"] == 210.0

    def test_empty_dataframe_raises(self, tmp_path):
        empty = pd.DataFrame(columns=["week_start", "vendor", "amount_usd"])
        with pytest.raises(ValueError):
            render_chart(empty, tmp_path / "x.png")


class TestColorForVendor:
    def test_known_vendors_return_default_colors(self):
        assert color_for_vendor("AWS")        == DEFAULT_VENDOR_COLORS["aws"]
        assert color_for_vendor("Datadog")    == DEFAULT_VENDOR_COLORS["datadog"]
        assert color_for_vendor("Databricks") == DEFAULT_VENDOR_COLORS["databricks"]

    def test_case_insensitive(self):
        assert color_for_vendor("aws") == DEFAULT_VENDOR_COLORS["aws"]
        assert color_for_vendor("AWS") == DEFAULT_VENDOR_COLORS["aws"]
        assert color_for_vendor("AwS") == DEFAULT_VENDOR_COLORS["aws"]

    def test_strips_parenthetical_suffix(self):
        # Comparison-CSV scripts label vendors with "(Console export)";
        # those should share a color with the live-API variant.
        assert color_for_vendor("AWS (Console export)") == DEFAULT_VENDOR_COLORS["aws"]
        assert color_for_vendor("Databricks (Console export)") == DEFAULT_VENDOR_COLORS["databricks"]
        assert color_for_vendor("Datadog (Console export)") == DEFAULT_VENDOR_COLORS["datadog"]

    def test_strips_arbitrary_parenthetical(self):
        assert color_for_vendor("AWS (Whatever)") == DEFAULT_VENDOR_COLORS["aws"]
        assert color_for_vendor("AWS  (with extra space)  ") == DEFAULT_VENDOR_COLORS["aws"]

    def test_unknown_vendor_returns_fallback(self):
        assert color_for_vendor("GCP") == FALLBACK_VENDOR_COLOR
        assert color_for_vendor("Snowflake") == FALLBACK_VENDOR_COLOR

    def test_empty_or_none_returns_fallback(self):
        assert color_for_vendor("") == FALLBACK_VENDOR_COLOR
        assert color_for_vendor(None) == FALLBACK_VENDOR_COLOR  # type: ignore[arg-type]

    def test_custom_fallback(self):
        assert color_for_vendor("GCP", fallback="#000000") == "#000000"

    def test_overrides_win_over_defaults(self):
        out = color_for_vendor("AWS", overrides={"AWS": "#123456"})
        assert out == "#123456"

    def test_overrides_normalize_keys(self):
        # Override keys should be matched after the same normalization
        # (case-insensitive, parenthetical stripped) as the lookup.
        out = color_for_vendor(
            "AWS (Console export)", overrides={"aws": "#abcdef"}
        )
        assert out == "#abcdef"

    def test_overrides_dont_affect_other_vendors(self):
        overrides = {"AWS": "#000000"}
        assert color_for_vendor("Datadog", overrides=overrides) == DEFAULT_VENDOR_COLORS["datadog"]

    def test_default_palette_distinct(self):
        # Sanity: the three named vendors are all different colors.
        colors = {DEFAULT_VENDOR_COLORS[k] for k in ("aws", "datadog", "databricks")}
        assert len(colors) == 3


class TestVendorFilenameSlug:
    @pytest.mark.parametrize(
        "vendor,expected",
        [
            ("AWS",                              "aws"),
            ("Databricks",                       "databricks"),
            ("Datadog",                          "datadog"),
            ("AWS (Console export)",             "aws_console_export"),
            ("Databricks (Console export)",      "databricks_console_export"),
            ("  weird  / chars!  ",              "weird_chars"),
            ("Multiple   spaces",                "multiple_spaces"),
            ("",                                 "vendor"),  # never empty
            ("___",                              "vendor"),  # all-junk
        ],
    )
    def test_slugifies(self, vendor, expected):
        assert vendor_filename_slug(vendor) == expected

    def test_no_path_separators(self):
        assert "/" not in vendor_filename_slug("AWS/Cost")
        assert "\\" not in vendor_filename_slug("AWS\\Cost")


class TestRenderPerVendorCharts:
    def test_writes_one_png_per_vendor(self, sample_csv, tmp_path):
        df = build_dataframe(sample_csv)
        paths = render_per_vendor_charts(df, tmp_path, base_stem="weekly")
        assert set(paths.keys()) == {"AWS", "Databricks", "Datadog"}
        for vendor, png in paths.items():
            assert png.exists(), f"PNG missing for {vendor}"
            assert png.stat().st_size > 0
            assert png.name.startswith("weekly_")
            assert png.suffix == ".png"

    def test_filenames_use_vendor_slug(self, sample_csv, tmp_path):
        df = build_dataframe(sample_csv)
        paths = render_per_vendor_charts(df, tmp_path, base_stem="weekly")
        assert paths["AWS"].name == "weekly_aws.png"
        assert paths["Databricks"].name == "weekly_databricks.png"
        assert paths["Datadog"].name == "weekly_datadog.png"

    def test_creates_output_dir(self, sample_csv, tmp_path):
        target = tmp_path / "a" / "b" / "c"
        df = build_dataframe(sample_csv)
        render_per_vendor_charts(df, target, base_stem="x")
        assert target.is_dir()

    def test_empty_dataframe_raises(self, tmp_path):
        empty = pd.DataFrame(columns=["week_start", "vendor", "amount_usd"])
        with pytest.raises(ValueError):
            render_per_vendor_charts(empty, tmp_path, base_stem="x")


class TestPngPathForCsv:
    def test_default_dir_is_csv_parent(self, tmp_path):
        csv = tmp_path / "2026_05_01_vendor_spend.csv"
        assert _png_path_for_csv(csv, None) == tmp_path / "2026_05_01_vendor_spend.png"

    def test_explicit_dir(self, tmp_path):
        csv = tmp_path / "2026_05_01_vendor_spend.csv"
        out = tmp_path / "elsewhere"
        assert _png_path_for_csv(csv, out) == out / "2026_05_01_vendor_spend.png"
