"""Tests for lucille.opsgenie.main.

Covers CSV/PNG/TXT output shapes and the end-to-end CLI wiring, plus the
label-shortening helper (which has real logic around long Datadog-style
aliases).
"""

import csv
from datetime import datetime, timezone
from pathlib import Path

import pytest

from context import lucille  # noqa: F401
from lucille.opsgenie.io import Alert
from lucille.opsgenie.main import (
    _short_label,
    main,
    render_summary,
    render_top_n_chart,
    write_ranked_csv,
)
from lucille.opsgenie.noise import compute_noise_rows, summarize


def _alert(alias, message="msg", ack=False, status="closed",
           when=None, team="DIP"):
    return Alert(
        alert_id="id",
        alias=alias,
        message=message,
        status=status,
        acknowledged=ack,
        created_at=when or datetime(2026, 7, 10, 12, tzinfo=timezone.utc),
        owner="",
        team=team,
        count=1,
    )


# ---------------------------------------------------------------------------
# _short_label
# ---------------------------------------------------------------------------


class TestShortLabel:
    def test_prefers_short_message(self):
        assert _short_label("some-alias", "Short message") == "Short message"

    def test_uses_only_first_line_of_multiline_message(self):
        assert _short_label("a", "first line\nsecond line") == "first line"

    def test_truncates_long_message_with_ellipsis(self):
        long_msg = "x" * 200
        out = _short_label("a", long_msg)
        assert out.endswith("\u2026")
        assert len(out) == 60

    def test_falls_back_to_alias_when_message_empty(self):
        assert _short_label("my-alias", "") == "my-alias"

    def test_falls_back_to_alias_when_message_is_none(self):
        # Defensive: caller might hand us None instead of "".
        assert _short_label("my-alias", None) == "my-alias"

    def test_truncates_long_alias_when_no_message(self):
        long_alias = "org_id:305115|" + "x" * 200
        out = _short_label(long_alias, "")
        assert out.endswith("\u2026")
        assert len(out) == 60


# ---------------------------------------------------------------------------
# write_ranked_csv
# ---------------------------------------------------------------------------


class TestWriteRankedCsv:
    def test_writes_expected_columns(self, tmp_path):
        rows = compute_noise_rows([_alert("x"), _alert("x", ack=True)])
        out = tmp_path / "out.csv"
        write_ranked_csv(rows, out)
        with open(out) as f:
            reader = csv.DictReader(f)
            fields = reader.fieldnames
            data = list(reader)
        assert "alias" in fields
        assert "fires" in fields
        assert "ack_rate_pct" in fields
        assert data[0]["alias"] == "x"
        assert data[0]["fires"] == "2"
        assert data[0]["ack_rate_pct"] == "50.0"

    def test_creates_parent_dir(self, tmp_path):
        # Nested path that doesn't exist yet.
        out = tmp_path / "nested" / "deeper" / "out.csv"
        write_ranked_csv(compute_noise_rows([_alert("x")]), out)
        assert out.exists()

    def test_empty_rows_writes_header_only(self, tmp_path):
        out = tmp_path / "out.csv"
        write_ranked_csv([], out)
        with open(out) as f:
            lines = f.readlines()
        assert len(lines) == 1  # header only


# ---------------------------------------------------------------------------
# render_top_n_chart
# ---------------------------------------------------------------------------


class TestRenderChart:
    def test_writes_a_png(self, tmp_path):
        alerts = [_alert("x") for _ in range(5)] + [_alert("y") for _ in range(3)]
        rows = compute_noise_rows(alerts)
        out = tmp_path / "chart.png"
        render_top_n_chart(rows, out, total_alerts=8, window_days=7)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_rows_skips_output(self, tmp_path):
        out = tmp_path / "chart.png"
        render_top_n_chart([], out, total_alerts=0, window_days=0)
        assert not out.exists()


# ---------------------------------------------------------------------------
# render_summary
# ---------------------------------------------------------------------------


class TestRenderSummary:
    def test_summary_contains_expected_lines(self, tmp_path):
        alerts = [_alert("x", ack=True), _alert("x"), _alert("y")]
        rows = compute_noise_rows(alerts)
        s = summarize(alerts, rows)
        out = tmp_path / "summary.txt"
        render_summary(s, rows, out)
        text = out.read_text()
        assert "Total alerts:" in text
        assert "Ack rate" in text.lower() or "ack rate" in text.lower()
        assert "Top 5 aliases" in text
        # Top 10 monitor list should appear when there are rows.
        assert "noisiest" in text.lower()

    def test_summary_handles_empty(self, tmp_path):
        s = summarize([], [])
        out = tmp_path / "summary.txt"
        render_summary(s, [], out)
        text = out.read_text()
        assert "Total alerts:              0" in text
        # No "noisiest" section when there are no rows to list.
        assert "noisiest" not in text.lower()


# ---------------------------------------------------------------------------
# main (end-to-end)
# ---------------------------------------------------------------------------


class TestMainCli:
    def _write_input_csv(self, tmp_path: Path) -> Path:
        header = (
            "Alert ID,Alias,TinyID,Message,Status,IsSeen,Acknowledged,"
            "Snoozed,CreatedAt,CreatedAtDate,UpdatedAt,UpdatedAtDate,"
            "Count,Owner,Teams"
        )
        rows = []
        # 5 fires of a noisy monitor, all unackd.
        for _ in range(5):
            rows.append(
                f"id,noisy-alias,tid,Disk full,closed,true,false,false,"
                f"1783684800000,,,,,1,,DevOps"
            )
        # 1 fire of a quiet monitor, ackd.
        rows.append(
            "id,quiet-alias,tid,Blip,closed,true,true,false,"
            "1783684800000,,,,,1,bo,DIP"
        )
        p = tmp_path / "in.csv"
        p.write_text(header + "\n" + "\n".join(rows) + "\n")
        return p

    def _write_config(self, tmp_path: Path) -> Path:
        cfg = tmp_path / "graphs.yaml"
        cfg.write_text(f"opsgenie_output_directory: {tmp_path / 'out'}\n")
        return cfg

    def test_end_to_end_produces_all_three_outputs(self, tmp_path, capsys):
        csv_path = self._write_input_csv(tmp_path)
        cfg_path = self._write_config(tmp_path)
        rc = main([
            "--csv", str(csv_path),
            "--config", str(cfg_path),
            "--top-n", "5",
            "--min-fires", "1",
        ])
        assert rc == 0
        out_dir = tmp_path / "out"
        outputs = list(out_dir.iterdir())
        names = sorted(p.name for p in outputs)
        assert any(n.endswith("_opsgenie_noise_ranked.csv") for n in names)
        assert any(n.endswith("_opsgenie_noise_top_5.png") for n in names)
        assert any(n.endswith("_opsgenie_noise_summary.txt") for n in names)

        # Punchline goes to stdout.
        captured = capsys.readouterr()
        assert "6 alerts" in captured.out  # 5 noisy + 1 quiet
        assert "2 monitors" in captured.out

    def test_returns_2_when_csv_missing(self, tmp_path):
        cfg_path = self._write_config(tmp_path)
        rc = main([
            "--csv", str(tmp_path / "does-not-exist.csv"),
            "--config", str(cfg_path),
        ])
        assert rc == 2

    def test_min_fires_filters_ranked_csv(self, tmp_path):
        csv_path = self._write_input_csv(tmp_path)
        cfg_path = self._write_config(tmp_path)
        # min-fires=5 excludes the quiet-alias row.
        main([
            "--csv", str(csv_path),
            "--config", str(cfg_path),
            "--min-fires", "5",
        ])
        ranked = next(
            (tmp_path / "out").glob("*_opsgenie_noise_ranked.csv")
        )
        with open(ranked) as f:
            data = list(csv.DictReader(f))
        assert len(data) == 1
        assert data[0]["alias"] == "noisy-alias"
