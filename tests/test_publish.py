"""Tests for lucille.publish.

The bug being fixed: previously, ``main()`` called ``update_page`` before
``upload_attachment``. Confluence would re-render the page with the new
body \u2014 which references attachments by filename \u2014 find those attachments
missing, cache the render with broken image icons, and continue serving
the broken cache even after the attachments finished uploading.

The tests below assert on the *order* of client calls, since that's the
whole point of the fix.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from context import lucille  # noqa: F401
from lucille.publish import publish_page


@pytest.fixture
def fake_client():
    """A duck-typed stand-in for ConfluenceClient.

    All methods record their invocations on the shared MagicMock, so
    tests can inspect call order via ``.mock_calls``.
    """
    c = MagicMock()
    c.create_page.return_value = {"id": "999", "version": {"number": 1}}
    c.upload_attachment.side_effect = lambda pid, path: Path(path).name
    return c


# ---------------------------------------------------------------------------
# Existing-page path
# ---------------------------------------------------------------------------


class TestExistingPage:
    def test_uploads_all_attachments_before_updating_body(self, fake_client, tmp_path):
        fake_client.find_page.return_value = {
            "id": "42", "version": {"number": 7},
        }
        images = [tmp_path / "a.png", tmp_path / "b.png"]

        publish_page(fake_client, "My Page", "<p>body</p>", images)

        # Extract the ordered list of (method, args) pairs.
        method_order = [c[0] for c in fake_client.mock_calls]
        upload_indices = [i for i, m in enumerate(method_order) if m == "upload_attachment"]
        update_index = method_order.index("update_page")

        assert upload_indices, "expected at least one upload_attachment call"
        # Every upload must happen before the body is set.
        assert max(upload_indices) < update_index, (
            f"upload happened after update_page: {method_order}"
        )

    def test_update_page_uses_current_version(self, fake_client, tmp_path):
        fake_client.find_page.return_value = {
            "id": "42", "version": {"number": 7},
        }
        publish_page(fake_client, "My Page", "<p>body</p>", [tmp_path / "a.png"])

        # ConfluenceClient.update_page expects the *current* version;
        # it bumps internally. We pass 7, not 8.
        fake_client.update_page.assert_called_once()
        _, args, _ = fake_client.update_page.mock_calls[0]
        assert args == ("42", "My Page", "<p>body</p>", 7)

    def test_does_not_create_page_when_it_exists(self, fake_client, tmp_path):
        fake_client.find_page.return_value = {
            "id": "42", "version": {"number": 3},
        }
        publish_page(fake_client, "My Page", "<p>body</p>", [tmp_path / "a.png"])
        fake_client.create_page.assert_not_called()


# ---------------------------------------------------------------------------
# New-page path
# ---------------------------------------------------------------------------


class TestNewPage:
    def test_creates_page_with_placeholder_first(self, fake_client, tmp_path):
        fake_client.find_page.return_value = None
        publish_page(
            fake_client, "My Page", "<p>real body</p>",
            [tmp_path / "a.png"], parent_id="root",
        )

        # First body-touching call must be create_page.
        first_body_call = next(
            c for c in fake_client.mock_calls
            if c[0] in ("create_page", "update_page")
        )
        assert first_body_call[0] == "create_page"
        # The initial body must NOT be the real body \u2014 attachments aren't
        # up yet. It should be a placeholder.
        args = first_body_call[1]
        # create_page(title, body, parent_id=...)
        title_arg, body_arg = args[0], args[1]
        assert title_arg == "My Page"
        assert "real body" not in body_arg

    def test_uploads_attachments_before_setting_real_body(self, fake_client, tmp_path):
        fake_client.find_page.return_value = None
        images = [tmp_path / "a.png", tmp_path / "b.png"]

        publish_page(fake_client, "My Page", "<p>real body</p>", images,
                     parent_id="root")

        method_order = [c[0] for c in fake_client.mock_calls]
        create_index = method_order.index("create_page")
        upload_indices = [
            i for i, m in enumerate(method_order) if m == "upload_attachment"
        ]
        update_index = method_order.index("update_page")

        # create -> uploads -> update
        assert all(create_index < u for u in upload_indices)
        assert max(upload_indices) < update_index

    def test_real_body_applied_via_update_page(self, fake_client, tmp_path):
        fake_client.find_page.return_value = None
        publish_page(fake_client, "My Page", "<p>real body</p>",
                     [tmp_path / "a.png"], parent_id="root")

        fake_client.update_page.assert_called_once()
        _, args, _ = fake_client.update_page.mock_calls[0]
        # update_page(page_id, title, body, current_version)
        page_id, title, body, version = args
        assert page_id == "999"                       # from create_page mock
        assert title == "My Page"
        assert body == "<p>real body</p>"             # real body now
        assert version == 1                           # new pages start at 1

    def test_returns_page_id(self, fake_client, tmp_path):
        fake_client.find_page.return_value = None
        pid = publish_page(
            fake_client, "My Page", "<p>body</p>",
            [tmp_path / "a.png"], parent_id="root",
        )
        assert pid == "999"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_no_images_still_sets_body(self, fake_client):
        fake_client.find_page.return_value = {
            "id": "42", "version": {"number": 1},
        }
        publish_page(fake_client, "My Page", "<p>body</p>", [])
        fake_client.upload_attachment.assert_not_called()
        fake_client.update_page.assert_called_once()

    def test_accepts_generator_of_images(self, fake_client, tmp_path):
        # Regression: an early draft consumed the images iterable in a
        # ``len()`` call and then tried to iterate it a second time in
        # the upload loop \u2014 which is silently empty for generators.
        fake_client.find_page.return_value = {
            "id": "42", "version": {"number": 1},
        }
        gen = (p for p in [tmp_path / "a.png", tmp_path / "b.png"])
        publish_page(fake_client, "My Page", "<p>body</p>", gen)
        assert fake_client.upload_attachment.call_count == 2
