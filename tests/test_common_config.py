"""Tests for lucille.common.config.load_yaml_config."""

import os
import tempfile
import unittest
from pathlib import Path

import pytest

from context import lucille  # noqa: F401
from lucille.common.config import load_yaml_config


def _write_tmp_yaml(content: str) -> str:
    fh = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    fh.write(content)
    fh.close()
    return fh.name


class TestLoadYamlConfig(unittest.TestCase):

    def test_loads_valid_yaml(self):
        path = _write_tmp_yaml("foo: 1\nbar: two\n")
        try:
            cfg = load_yaml_config(path)
            self.assertEqual(cfg, {"foo": 1, "bar": "two"})
        finally:
            os.unlink(path)

    def test_empty_file_returns_empty_dict(self):
        path = _write_tmp_yaml("")
        try:
            self.assertEqual(load_yaml_config(path), {})
        finally:
            os.unlink(path)

    def test_missing_file_exit_by_default(self):
        with pytest.raises(SystemExit):
            load_yaml_config("/nonexistent/path.yaml")

    def test_missing_file_raise_mode(self):
        with pytest.raises(FileNotFoundError):
            load_yaml_config("/nonexistent/path.yaml", on_missing="raise")

    def test_missing_file_empty_mode(self):
        self.assertEqual(load_yaml_config("/nonexistent/path.yaml", on_missing="empty"), {})

    def test_none_path_empty_mode(self):
        self.assertEqual(load_yaml_config(None, on_missing="empty"), {})

    def test_subsection(self):
        path = _write_tmp_yaml("jira:\n  base_url: https://x\n  username: u\n")
        try:
            cfg = load_yaml_config(path, subsection="jira")
            self.assertEqual(cfg, {"base_url": "https://x", "username": "u"})
        finally:
            os.unlink(path)

    def test_missing_subsection_exits(self):
        path = _write_tmp_yaml("other: {}\n")
        try:
            with pytest.raises(SystemExit):
                load_yaml_config(path, subsection="jira")
        finally:
            os.unlink(path)

    def test_required_keys_present(self):
        path = _write_tmp_yaml("a: 1\nb: 2\n")
        try:
            cfg = load_yaml_config(path, required_keys=("a", "b"))
            self.assertEqual(cfg, {"a": 1, "b": 2})
        finally:
            os.unlink(path)

    def test_required_keys_missing_exits(self):
        path = _write_tmp_yaml("a: 1\n")
        try:
            with pytest.raises(SystemExit):
                load_yaml_config(path, required_keys=("a", "b"))
        finally:
            os.unlink(path)

    def test_subsection_and_required_keys_combined(self):
        path = _write_tmp_yaml("jira:\n  base_url: x\n  username: u\n  api_token: t\n")
        try:
            cfg = load_yaml_config(
                path,
                subsection="jira",
                required_keys=("base_url", "username", "api_token"),
            )
            self.assertEqual(cfg["base_url"], "x")
        finally:
            os.unlink(path)

    def test_malformed_yaml_exits(self):
        path = _write_tmp_yaml("foo: [unterminated\n")
        try:
            with pytest.raises(SystemExit):
                load_yaml_config(path)
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
