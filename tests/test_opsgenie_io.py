"""Tests for lucille.opsgenie.io."""

from datetime import timezone
from pathlib import Path

from context import lucille  # noqa: F401
from lucille.opsgenie.io import (
    Alert,
    _parse_bool,
    _parse_count,
    _parse_created_at,
    load_alerts,
)


class TestParseHelpers:
    def test_parse_bool_true(self):
        assert _parse_bool("true") is True
        assert _parse_bool("TRUE") is True
        assert _parse_bool(" true ") is True

    def test_parse_bool_false(self):
        assert _parse_bool("false") is False
        assert _parse_bool("") is False
        assert _parse_bool("nonsense") is False

    def test_parse_created_at_is_utc(self):
        # 2026-07-10 12:00:00 UTC in milliseconds.
        ms = "1783684800000"
        dt = _parse_created_at(ms)
        assert dt.tzinfo == timezone.utc
        assert dt.year == 2026 and dt.month == 7 and dt.day == 10
        assert dt.hour == 12

    def test_parse_count_defaults_to_1_when_blank(self):
        assert _parse_count("") == 1
        assert _parse_count("   ") == 1

    def test_parse_count_defaults_to_1_when_malformed(self):
        # Better to under-count than to crash on a malformed row.
        assert _parse_count("N/A") == 1

    def test_parse_count_parses_valid_int(self):
        assert _parse_count("5") == 5


class TestLoadAlerts:
    def _write_csv(self, tmp_path: Path, rows: list) -> Path:
        header = (
            "Alert ID,Alias,TinyID,Message,Status,IsSeen,Acknowledged,"
            "Snoozed,CreatedAt,CreatedAtDate,UpdatedAt,UpdatedAtDate,"
            "Count,Owner,Teams"
        )
        p = tmp_path / "opsgenie.csv"
        with open(p, "w", encoding="utf-8") as f:
            f.write(header + "\n")
            for row in rows:
                f.write(row + "\n")
        return p

    def test_loads_basic_row(self, tmp_path):
        p = self._write_csv(tmp_path, [
            "aid1,my-alias,tid1,Something broke,closed,true,true,false,"
            "1783080000000,2026-07-10T12:00:00Z,1783080600000,"
            "2026-07-10T12:10:00Z,1,bo@jaris.io,DIP"
        ])
        alerts = load_alerts(p)
        assert len(alerts) == 1
        a = alerts[0]
        assert a.alert_id == "aid1"
        assert a.alias == "my-alias"
        assert a.message == "Something broke"
        assert a.status == "closed"
        assert a.acknowledged is True
        assert a.owner == "bo@jaris.io"
        assert a.team == "DIP"
        assert a.count == 1

    def test_skips_row_with_unparseable_created_at(self, tmp_path):
        p = self._write_csv(tmp_path, [
            "aid1,a1,tid1,ok,closed,true,true,false,not-a-number,,,,,,",
            "aid2,a2,tid2,also ok,closed,true,false,false,"
            "1783080000000,,,,,,",
        ])
        alerts = load_alerts(p)
        assert len(alerts) == 1
        assert alerts[0].alert_id == "aid2"

    def test_normalizes_status_to_lowercase(self, tmp_path):
        p = self._write_csv(tmp_path, [
            "aid1,a1,tid1,ok,CLOSED,true,true,false,1783080000000,,,,,,",
        ])
        assert load_alerts(p)[0].status == "closed"

    def test_strips_whitespace_from_fields(self, tmp_path):
        p = self._write_csv(tmp_path, [
            "  aid1  ,  a1  ,tid1,  msg  ,closed,true,true,false,"
            "1783080000000,,,,,  bo  ,  DIP  ",
        ])
        a = load_alerts(p)[0]
        assert a.alert_id == "aid1"
        assert a.alias == "a1"
        assert a.message == "msg"
        assert a.owner == "bo"
        assert a.team == "DIP"

    def test_handles_missing_optional_columns_gracefully(self, tmp_path):
        # Owner and Teams can legitimately be empty.
        p = self._write_csv(tmp_path, [
            "aid1,a1,tid1,ok,closed,true,false,false,1783080000000,,,,,,",
        ])
        a = load_alerts(p)[0]
        assert a.owner == ""
        assert a.team == ""

    def test_ack_true_and_false_both_parsed(self, tmp_path):
        p = self._write_csv(tmp_path, [
            "aid1,a1,t,msg,closed,true,true,false,1783080000000,,,,,,",
            "aid2,a2,t,msg,closed,true,false,false,1783080000000,,,,,,",
        ])
        alerts = load_alerts(p)
        assert alerts[0].acknowledged is True
        assert alerts[1].acknowledged is False

    def test_empty_csv_returns_empty_list(self, tmp_path):
        p = self._write_csv(tmp_path, [])
        assert load_alerts(p) == []
