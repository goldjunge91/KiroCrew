"""``_deny_cross_app_slot_access`` refuses channel-backed slots for app callers.

The chokepoint guards every route that resolves a slot's transcript through
``slot_history_key`` (detail's transcript read first among them), so an app
owning a channel-backed slot must get the same anti-enumeration 404 there as a
slot it does not own -- otherwise the read route hands over the same foreign
conversation the send/export/fork/rewind boundaries refuse.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from kiro_crew.dashboard.chat_handlers import _deny_cross_app_slot_access


@pytest.fixture(autouse=True)
def _quiet_sel(monkeypatch):
    """The gate audits every refusal; keep the SEL singleton out of these unit tests."""
    from unittest.mock import MagicMock

    from kiro_crew.dashboard import chat_handlers

    monkeypatch.setattr(chat_handlers, "sel", lambda: MagicMock())


def _slot(app="", linked="", channel_origin=False):
    # An unbound channel-born slot keeps the channel transcript's own stem as
    # its name -- that is how ``slot_transcript_key`` finds the transcript.
    key = "slack_1700000000.000100" if channel_origin else "s"
    return SimpleNamespace(
        _app=app, linked_session_key=linked, channel_origin=channel_origin, key=key
    )


def _request(app=""):
    return SimpleNamespace(get=lambda k, default="": app if k == "app" else default)


def _body(resp):
    return json.loads(resp.body)


def test_an_app_is_refused_its_own_channel_linked_slot() -> None:
    resp = _deny_cross_app_slot_access(
        _request("my-app"), _slot(app="my-app", linked="slack:1700000000.000100"), "s", "op"
    )
    assert resp is not None and resp.status == 404
    assert _body(resp)["code"] == "slot_not_found"


def test_an_app_is_refused_its_own_channel_origin_slot() -> None:
    slot = _slot(app="my-app", channel_origin=True)
    resp = _deny_cross_app_slot_access(_request("my-app"), slot, slot.key, "op")
    assert resp is not None and resp.status == 404
    assert _body(resp)["code"] == "slot_not_found"


def test_the_refusal_is_indistinguishable_from_the_not_owned_answer() -> None:
    slot = _slot(app="my-app", channel_origin=True)
    backed = _deny_cross_app_slot_access(_request("my-app"), slot, slot.key, "op")
    foreign = _deny_cross_app_slot_access(_request("my-app"), _slot(app="other-app"), "s", "op")
    assert backed is not None and foreign is not None
    assert backed.status == foreign.status
    assert backed.body == foreign.body


def test_an_app_still_passes_on_its_own_plain_slot() -> None:
    assert _deny_cross_app_slot_access(_request("my-app"), _slot(app="my-app"), "s", "op") is None


def test_the_dashboard_owner_passes_on_a_channel_linked_slot() -> None:
    slot = _slot(app="my-app", linked="slack:1700000000.000100")
    assert _deny_cross_app_slot_access(_request(""), slot, "s", "op") is None


def test_the_cancel_routes_opt_out_of_the_channel_backed_refusal() -> None:
    slot = _slot(app="my-app", linked="slack:1700000000.000100")
    assert (
        _deny_cross_app_slot_access(_request("my-app"), slot, "s", "op", allow_channel_backed=True)
        is None
    )


class TestChokepointMatchesTheOwnershipGate:
    """The chokepoint and ``_check_slot_app_ownership`` (the gate the send,
    continue, summary, resume and source-links routes go through) must refuse
    the same set of slots with a byte-identical 404, or a route's answer would
    tell an app which of its slots carry a channel link."""

    @pytest.mark.parametrize(
        ("slot", "key"),
        [
            (_slot(app="my-app", linked="cron:job-1"), "s"),
            # An unbound channel-born slot keeps the channel transcript's own
            # stem as its name -- that is how ``slot_transcript_key`` finds it.
            (_slot(app="my-app", channel_origin=True), "slack_1700000000.000100"),
            (_slot(app="other-app"), "s"),
            (_slot(app=""), "s"),
        ],
    )
    def test_both_refuse_with_the_same_body(self, slot, key, monkeypatch) -> None:
        from unittest.mock import MagicMock

        from kiro_crew.dashboard import chat_handlers

        monkeypatch.setattr(chat_handlers, "sel", lambda: MagicMock())
        slot.key = key
        choke = _deny_cross_app_slot_access(_request("my-app"), slot, key, "op")
        gate = chat_handlers._check_slot_app_ownership(slot, key, "my-app", "op")
        assert choke is not None and gate is not None
        assert choke.status == gate.status == 404
        assert choke.body == gate.body

    def test_both_pass_a_plain_owned_slot_and_the_dashboard(self) -> None:
        from kiro_crew.dashboard import chat_handlers

        plain = _slot(app="my-app")
        plain.key = "s"
        assert _deny_cross_app_slot_access(_request("my-app"), plain, "s", "op") is None
        assert chat_handlers._check_slot_app_ownership(plain, "s", "my-app", "op") is None
        linked = _slot(app="my-app", linked="cron:job-1")
        linked.key = "s"
        assert _deny_cross_app_slot_access(_request(""), linked, "s", "op") is None
        assert chat_handlers._check_slot_app_ownership(linked, "s", "", "op") is None


@pytest.mark.asyncio
async def test_detail_refuses_when_the_bind_lands_during_its_reads(tmp_path, monkeypatch):
    """The chokepoint's check is synchronous and the detail route awaits many
    times after it (disk reads, the render). A channel/cron injection binding
    the slot during one of them hydrates the in-memory window from the
    channel; the response must not carry it."""
    from unittest.mock import MagicMock

    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    from chat_test_helpers import _make_app, _make_state

    from kiro_crew.dashboard import chat_handlers
    from kiro_crew.dashboard.state import _ChatSlot

    mock_sel = MagicMock()
    monkeypatch.setattr(chat_handlers, "sel", lambda: mock_sel)
    state = _make_state(tmp_path)
    slot = _ChatSlot("s1")
    slot._app = "my-app"
    slot.append("user", "own message", "msg msg-u")
    state._slots[slot.key] = slot

    class _BindingRenderLock:
        # The render lock is the LAST await before the response leaves, so a
        # bind landing here has slipped past every earlier read.
        async def __aenter__(self):
            slot.linked_session_key = "cron:job-1"

        async def __aexit__(self, *exc):
            return False

    slot._detail_render_lock = _BindingRenderLock()

    app = _make_app(state)

    @web.middleware
    async def _as_app(request, handler):
        request["app"] = "my-app"
        return await handler(request)

    # Outermost, so the helper's owner-defaulting middleware sees the app set.
    app.middlewares.insert(0, _as_app)
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/chat/slots/s1")
        assert resp.status == 404
        assert (await resp.json())["code"] == "slot_not_found"
    denied = [c for c in mock_sel.log_api_access.call_args_list if c[1].get("outcome") == "denied"]
    assert len(denied) == 1
    assert denied[0][1]["operation"] == "slot_detail"
    assert denied[0][1]["source"] == "app_isolation"
