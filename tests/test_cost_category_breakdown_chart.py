"""Unit tests for lucille.jira.cost_category_breakdown_chart (pure helpers + render smoke test)."""

import pandas as pd
import pytest

from lucille.jira.cost_category_breakdown_chart import (
    all_categories,
    category_color_map,
    compute_percent_breakdown,
    load_epics,
    parse_pie_projects,
    project_totals,
    render_chart,
    render_pie,
)


def _df(rows):
    """Build a tiny labeled-epics DataFrame from (project, category, sp, tc) tuples."""
    return pd.DataFrame(
        rows,
        columns=["project", "cost_category", "story_points_sum", "ticket_count"],
    )


class TestComputePercentBreakdown:
    def test_basic_mix(self):
        df = _df([
            ("DIP", "platform",    10, 2),
            ("DIP", "revenue",     30, 6),
            ("OOT", "reliability",  5, 1),
            ("OOT", "revenue",     15, 3),
        ])
        pct = compute_percent_breakdown(df, "story_points_sum")
        # DIP: 10 platform / 40 total = 25%, 30 revenue / 40 = 75%
        assert pct.loc["DIP", "platform"] == pytest.approx(25.0)
        assert pct.loc["DIP", "revenue"] == pytest.approx(75.0)
        # OOT: 5/20 reliability = 25%, 15/20 revenue = 75%
        assert pct.loc["OOT", "reliability"] == pytest.approx(25.0)
        assert pct.loc["OOT", "revenue"] == pytest.approx(75.0)

    def test_rows_sum_to_100(self):
        df = _df([
            ("DIP", "platform", 10, 2),
            ("DIP", "revenue",  30, 6),
            ("OOT", "platform",  7, 1),
        ])
        pct = compute_percent_breakdown(df, "story_points_sum")
        assert pct.sum(axis=1).round(6).eq(100.0).all()

    def test_zero_total_project_renders_as_zero_row(self):
        # Project with all zero story points should not become NaN.
        df = _df([
            ("DIP", "platform", 0, 1),
            ("DIP", "revenue",  0, 2),
            ("OOT", "platform", 5, 1),
        ])
        pct = compute_percent_breakdown(df, "story_points_sum")
        assert pct.loc["DIP"].sum() == 0.0
        assert pct.loc["DIP", "platform"] == 0.0
        assert pct.loc["DIP", "revenue"] == 0.0
        assert pct.loc["OOT", "platform"] == pytest.approx(100.0)

    def test_missing_category_in_project_is_zero(self):
        df = _df([
            ("DIP", "platform", 10, 1),
            ("OOT", "revenue",  20, 2),
        ])
        pct = compute_percent_breakdown(df, "story_points_sum")
        assert pct.loc["DIP", "platform"] == pytest.approx(100.0)
        assert pct.loc["DIP", "revenue"] == pytest.approx(0.0)
        assert pct.loc["OOT", "platform"] == pytest.approx(0.0)
        assert pct.loc["OOT", "revenue"] == pytest.approx(100.0)

    def test_empty_dataframe_returns_empty(self):
        empty = _df([])
        assert compute_percent_breakdown(empty, "story_points_sum").empty

    def test_works_for_ticket_count_metric(self):
        df = _df([
            ("DIP", "platform", 0, 3),
            ("DIP", "revenue",  0, 1),
        ])
        pct = compute_percent_breakdown(df, "ticket_count")
        assert pct.loc["DIP", "platform"] == pytest.approx(75.0)
        assert pct.loc["DIP", "revenue"] == pytest.approx(25.0)

    def test_projects_sorted_alphabetically(self):
        df = _df([
            ("OOT", "platform", 1, 1),
            ("DIP", "platform", 1, 1),
            ("SSJ", "platform", 1, 1),
        ])
        pct = compute_percent_breakdown(df, "story_points_sum")
        assert list(pct.index) == ["DIP", "OOT", "SSJ"]


class TestProjectTotals:
    def test_basic_sum_per_project(self):
        df = _df([
            ("DIP", "platform", 10, 2),
            ("DIP", "revenue",  30, 6),
            ("OOT", "platform",  5, 1),
        ])
        totals = project_totals(df, "story_points_sum")
        assert totals["DIP"] == 40
        assert totals["OOT"] == 5

    def test_ticket_count(self):
        df = _df([
            ("DIP", "platform", 10, 2),
            ("DIP", "revenue",  30, 6),
            ("OOT", "platform",  5, 1),
        ])
        totals = project_totals(df, "ticket_count")
        assert totals["DIP"] == 8
        assert totals["OOT"] == 1

    def test_empty(self):
        assert project_totals(_df([]), "story_points_sum").empty


class TestAllCategories:
    def test_union_across_panels(self):
        a = pd.DataFrame(columns=["platform", "revenue"])
        b = pd.DataFrame(columns=["revenue", "reliability"])
        assert all_categories(a, b) == ["platform", "reliability", "revenue"]

    def test_single_panel(self):
        a = pd.DataFrame(columns=["platform"])
        assert all_categories(a) == ["platform"]

    def test_no_panels(self):
        assert all_categories() == []


class TestLoadEpicsTrimsWhitespace:
    def test_strips_crlf_artefacts(self, tmp_path):
        # Mimic CRLF + trailing whitespace in the cost_category column.
        body = (
            "project,cost_category,story_points_sum,ticket_count\r\n"
            "DIP,platform,10,2\r\n"
            "OOT,revenue ,5,1\r\n"
        )
        path = tmp_path / "in.csv"
        path.write_bytes(body.encode("utf-8"))
        df = load_epics(path)
        assert sorted(df["cost_category"].unique()) == ["platform", "revenue"]


class TestCategoryColorMap:
    def test_returns_one_color_per_category(self):
        cm = category_color_map(["platform", "revenue", "reliability"])
        assert set(cm.keys()) == {"platform", "revenue", "reliability"}
        assert len({tuple(v) for v in cm.values()}) == 3  # all distinct

    def test_empty_input_returns_empty_map(self):
        assert category_color_map([]) == {}

    def test_stable_for_same_input(self):
        a = category_color_map(["platform", "revenue"])
        b = category_color_map(["platform", "revenue"])
        assert a == b


class TestParsePieProjects:
    def test_basic(self):
        assert parse_pie_projects("OOT,SSJ") == ["OOT", "SSJ"]

    def test_strips_whitespace(self):
        assert parse_pie_projects(" OOT , SSJ ") == ["OOT", "SSJ"]

    def test_drops_empty_entries(self):
        assert parse_pie_projects("OOT,,SSJ,") == ["OOT", "SSJ"]

    def test_empty_string_returns_empty_list(self):
        assert parse_pie_projects("") == []

    def test_none_returns_empty_list(self):
        assert parse_pie_projects(None) == []


class TestRenderChartSmoke:
    def test_writes_a_png(self, tmp_path):
        df = _df([
            ("DIP", "platform",    10, 2),
            ("DIP", "revenue",     30, 6),
            ("OOT", "reliability",  5, 1),
            ("OOT", "revenue",     15, 3),
        ])
        out = tmp_path / "chart.png"
        result = render_chart(df, out, title="test")
        assert result == out
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_df_raises(self, tmp_path):
        with pytest.raises(ValueError):
            render_chart(_df([]), tmp_path / "x.png")

    def test_no_categories_raises(self, tmp_path):
        df_no_cat = pd.DataFrame({
            "project": [],
            "cost_category": [],
            "story_points_sum": [],
            "ticket_count": [],
        })
        with pytest.raises(ValueError):
            render_chart(df_no_cat, tmp_path / "x.png")


class TestRenderPie:
    def test_writes_a_png_for_a_project(self, tmp_path):
        df = _df([
            ("OOT", "platform",    0, 5),
            ("OOT", "revenue",     0, 3),
            ("OOT", "reliability", 0, 2),
            ("DIP", "platform",    0, 100),  # noise; should be ignored
        ])
        out = tmp_path / "oot.png"
        result = render_pie(df, "OOT", out)
        assert result == out
        assert out.exists()
        assert out.stat().st_size > 0

    def test_unknown_project_raises(self, tmp_path):
        df = _df([("OOT", "platform", 0, 1)])
        with pytest.raises(ValueError, match="GHOST"):
            render_pie(df, "GHOST", tmp_path / "x.png")

    def test_project_with_zero_total_raises(self, tmp_path):
        df = _df([
            ("OOT", "platform", 0, 0),
            ("OOT", "revenue",  0, 0),
        ])
        with pytest.raises(ValueError, match="zero tickets"):
            render_pie(df, "OOT", tmp_path / "x.png")

    def test_empty_df_raises(self, tmp_path):
        with pytest.raises(ValueError):
            render_pie(_df([]), "OOT", tmp_path / "x.png")

    def test_zero_value_categories_dropped_silently(self, tmp_path):
        # platform has 0 tickets; should not break or produce a 0% slice.
        df = _df([
            ("OOT", "platform", 0, 0),
            ("OOT", "revenue",  0, 5),
        ])
        out = tmp_path / "oot.png"
        # Just needs to render without raising.
        assert render_pie(df, "OOT", out).exists()

    def test_accepts_shared_color_map(self, tmp_path):
        df = _df([
            ("OOT", "platform", 0, 5),
            ("OOT", "revenue",  0, 3),
        ])
        cmap = category_color_map(["platform", "reliability", "revenue"])
        out = tmp_path / "oot.png"
        render_pie(df, "OOT", out, color_map=cmap)
        assert out.exists()
