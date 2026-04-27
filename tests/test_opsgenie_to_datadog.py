import textwrap
from pathlib import Path

import pytest
import yaml

import context  # noqa: F401 — adds project root to sys.path

from lucille.datadog.opsgenie_to_datadog import (
    Config,
    ROTATION_SECONDS,
    build_escalation_step,
    build_schedule_layer,
    build_team_handle,
    build_team_payload,
    load_config,
    split_cell,
)


# ── split_cell ────────────────────────────────────────────────────────────────


def test_split_cell_empty_string():
    assert split_cell("") == []


def test_split_cell_none_like():
    assert split_cell(None) == []  # type: ignore[arg-type]


def test_split_cell_single_value():
    assert split_cell("alice@example.com") == ["alice@example.com"]


def test_split_cell_multiple_values():
    assert split_cell("alice@example.com; bob@example.com") == [
        "alice@example.com",
        "bob@example.com",
    ]


def test_split_cell_trims_whitespace():
    assert split_cell("  alice@example.com ;  bob@example.com  ") == [
        "alice@example.com",
        "bob@example.com",
    ]


def test_split_cell_custom_sep():
    assert split_cell("a,b,c", sep=",") == ["a", "b", "c"]


def test_split_cell_skips_blank_segments():
    assert split_cell("; alice@example.com; ") == ["alice@example.com"]


# ── build_team_handle ─────────────────────────────────────────────────────────


def test_build_team_handle_spaces():
    assert build_team_handle("Platform Eng") == "platform-eng"


def test_build_team_handle_underscores():
    assert build_team_handle("platform_eng") == "platform-eng"


def test_build_team_handle_mixed():
    assert build_team_handle("Platform_Eng Team") == "platform-eng-team"


def test_build_team_handle_already_lowercase():
    assert build_team_handle("oncall") == "oncall"


# ── build_team_payload ────────────────────────────────────────────────────────


def test_build_team_payload_structure():
    payload = build_team_payload("SRE Team", "Site reliability")
    assert payload["data"]["type"] == "teams"
    assert payload["data"]["attributes"]["name"] == "SRE Team"
    assert payload["data"]["attributes"]["handle"] == "sre-team"
    assert payload["data"]["attributes"]["description"] == "Site reliability"


def test_build_team_payload_empty_description():
    payload = build_team_payload("Infra", "")
    assert payload["data"]["attributes"]["description"] == ""


# ── build_schedule_layer ──────────────────────────────────────────────────────


USER_MAP = {"alice@example.com": "uid-1", "bob@example.com": "uid-2"}


def _rot(**kwargs) -> dict:
    defaults = {
        "rotation_name": "Primary",
        "participants": "alice@example.com; bob@example.com",
        "type": "weekly",
        "length": "1",
        "start_date": "2025-01-01T00:00:00Z",
        "end_date": "",
    }
    return {**defaults, **kwargs}


def test_build_schedule_layer_basic():
    layer = build_schedule_layer(_rot(), USER_MAP)
    assert layer is not None
    assert layer["name"] == "Primary"
    assert layer["interval"] == {"seconds": ROTATION_SECONDS["weekly"]}
    assert "effective_date" in layer
    assert {"type": "users", "id": "uid-1"} in layer["members"]
    assert {"type": "users", "id": "uid-2"} in layer["members"]


def test_build_schedule_layer_daily():
    layer = build_schedule_layer(_rot(type="daily", length="2"), USER_MAP)
    assert layer is not None
    assert layer["interval"] == {"seconds": ROTATION_SECONDS["daily"] * 2}


def test_build_schedule_layer_unknown_type_falls_back_to_weeks():
    layer = build_schedule_layer(_rot(type="fortnightly"), USER_MAP)
    assert layer is not None
    assert layer["interval"] == {"seconds": ROTATION_SECONDS["weekly"]}


def test_build_schedule_layer_includes_end_date():
    layer = build_schedule_layer(_rot(end_date="2025-12-31T00:00:00Z"), USER_MAP)
    assert layer is not None
    assert layer["end"] == "2025-12-31T00:00:00Z"


def test_build_schedule_layer_omits_end_when_empty():
    layer = build_schedule_layer(_rot(end_date=""), USER_MAP)
    assert layer is not None
    assert "end" not in layer


def test_build_schedule_layer_no_known_users_returns_none():
    layer = build_schedule_layer(_rot(participants="unknown@example.com"), USER_MAP)
    assert layer is None


def test_build_schedule_layer_partial_users():
    layer = build_schedule_layer(
        _rot(participants="alice@example.com; unknown@example.com"), USER_MAP
    )
    assert layer is not None
    assert len(layer["members"]) == 1
    assert layer["members"][0]["id"] == "uid-1"


def test_build_schedule_layer_zero_length_defaults_to_one():
    layer = build_schedule_layer(_rot(length="0"), USER_MAP)
    assert layer is not None
    assert layer["interval"] == {"seconds": ROTATION_SECONDS["weekly"]}


def test_build_schedule_layer_empty_participants_returns_none():
    layer = build_schedule_layer(_rot(participants=""), USER_MAP)
    assert layer is None


def test_build_schedule_layer_default_name_when_missing():
    layer = build_schedule_layer(_rot(rotation_name=""), USER_MAP)
    assert layer is not None
    assert layer["name"] == "Primary"


# ── build_escalation_step ─────────────────────────────────────────────────────


USERS = {"alice@example.com": "uid-1"}
TEAMS = {"SRE": "team-1"}
SCHEDS = {"Primary Schedule": "sched-1"}


def _rule(**kwargs) -> dict:
    defaults = {
        "recipient_type": "user",
        "recipient_name": "alice@example.com",
        "delay_mins": "5",
    }
    return {**defaults, **kwargs}


def test_build_escalation_step_user():
    step = build_escalation_step(_rule(), USERS, SCHEDS)
    assert step is not None
    assert step["escalate_after_seconds"] == 300  # 5 mins * 60
    assert step["targets"] == [{"type": "users", "id": "uid-1"}]


def test_build_escalation_step_team_skipped():
    step = build_escalation_step(
        _rule(recipient_type="team", recipient_name="SRE"), USERS, SCHEDS
    )
    assert step is None


def test_build_escalation_step_schedule():
    step = build_escalation_step(
        _rule(recipient_type="schedule", recipient_name="Primary Schedule"),
        USERS,
        SCHEDS,
    )
    assert step is not None
    assert step["targets"] == [{"type": "schedules", "id": "sched-1"}]


def test_build_escalation_step_unknown_type_resolves_user():
    step = build_escalation_step(
        _rule(recipient_type="", recipient_name="alice@example.com"), USERS, SCHEDS
    )
    assert step is not None
    assert step["targets"][0]["type"] == "users"


def test_build_escalation_step_unknown_type_resolves_schedule():
    step = build_escalation_step(
        _rule(recipient_type="", recipient_name="Primary Schedule"), USERS, SCHEDS
    )
    assert step is not None
    assert step["targets"][0]["type"] == "schedules"


def test_build_escalation_step_unresolvable_returns_none():
    step = build_escalation_step(
        _rule(recipient_name="nobody@example.com"), USERS, SCHEDS
    )
    assert step is None


def test_build_escalation_step_zero_delay_floors_to_60s():
    step = build_escalation_step(_rule(delay_mins="0"), USERS, SCHEDS)
    assert step is not None
    assert step["escalate_after_seconds"] == 60


def test_build_escalation_step_missing_delay_floors_to_60s():
    step = build_escalation_step(_rule(delay_mins=""), USERS, SCHEDS)
    assert step is not None
    assert step["escalate_after_seconds"] == 60


def test_build_escalation_step_large_delay_capped_at_36000s():
    step = build_escalation_step(_rule(delay_mins="720"), USERS, SCHEDS)
    assert step is not None
    assert step["escalate_after_seconds"] == 36000


# ── load_config ───────────────────────────────────────────────────────────────


def _write_config(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(textwrap.dedent(content))
    return p


def test_load_config_reads_yaml(tmp_path):
    cfg_path = _write_config(
        tmp_path,
        """
        datadog:
          api_key: test-api-key
          app_key: test-app-key
          site: datadoghq.eu
        migration:
          input_dir: /tmp/export
          dry_run: true
          request_delay: 0.5
        """,
    )
    cfg = load_config(cfg_path)
    assert cfg.api_key == "test-api-key"
    assert cfg.app_key == "test-app-key"
    assert cfg.site == "datadoghq.eu"
    assert cfg.input_dir == Path("/tmp/export")
    assert cfg.dry_run is True
    assert cfg.request_delay == 0.5


def test_load_config_overrides_site(tmp_path):
    cfg_path = _write_config(
        tmp_path,
        """
        datadog:
          api_key: k
          app_key: a
          site: datadoghq.com
        migration: {}
        """,
    )
    cfg = load_config(cfg_path, overrides={"site": "datadoghq.eu"})
    assert cfg.site == "datadoghq.eu"


def test_load_config_overrides_dry_run(tmp_path):
    cfg_path = _write_config(
        tmp_path,
        """
        datadog:
          api_key: k
          app_key: a
        migration:
          dry_run: false
        """,
    )
    cfg = load_config(cfg_path, overrides={"dry_run": True})
    assert cfg.dry_run is True


def test_load_config_overrides_input_dir(tmp_path):
    cfg_path = _write_config(
        tmp_path,
        """
        datadog:
          api_key: k
          app_key: a
        migration:
          input_dir: ./default
        """,
    )
    cfg = load_config(cfg_path, overrides={"input_dir": "/custom/path"})
    assert cfg.input_dir == Path("/custom/path")


def test_load_config_base_url(tmp_path):
    cfg_path = _write_config(
        tmp_path,
        """
        datadog:
          api_key: k
          app_key: a
          site: datadoghq.eu
        migration: {}
        """,
    )
    cfg = load_config(cfg_path)
    assert cfg.base_url == "https://api.datadoghq.eu"


def test_load_config_missing_api_key_exits(tmp_path):
    cfg_path = _write_config(
        tmp_path,
        """
        datadog:
          api_key: ""
          app_key: valid-app-key
        migration: {}
        """,
    )
    with pytest.raises(SystemExit):
        load_config(cfg_path)


def test_load_config_missing_app_key_exits(tmp_path):
    cfg_path = _write_config(
        tmp_path,
        """
        datadog:
          api_key: valid-api-key
          app_key: ""
        migration: {}
        """,
    )
    with pytest.raises(SystemExit):
        load_config(cfg_path)
