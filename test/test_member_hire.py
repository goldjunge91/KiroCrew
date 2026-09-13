"""Hire a crew member from a local Custom Agent file — POST /api/members/hire.

Rollout step 2 of *Crew Member = Custom Agent + Wrapper Layer*: the "adopt"
path. A hire (1) creates the wrapper row through the same validated create
path ``POST /api/agents`` uses, minting the id from the display name, then
(2) copies the source definition into a member-owned agent file whose declared
``name`` is the copy's stem (derived from the member id) and rebinds the row to
it — the private-copy fork the crew editor already uses on first edit.

The step-2 GATE: two members hired from ONE file coexist, each with its own
row and its own copy.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from member_memory_helpers import patch_private_memory_supported

from kiro_crew import agent_state
from kiro_crew.config.loader import KiroCrewAgentConfig, KiroCrewConfig
from kiro_crew.dashboard.handlers import agents as _agents

SOURCE = "reviewer"


@pytest.fixture(autouse=True)
def _owner_caller(monkeypatch):
    monkeypatch.setattr(
        "kiro_crew.dashboard.handlers.source_providers.is_owner_dashboard_request",
        lambda request: True,
    )
    patch_private_memory_supported(monkeypatch)


@pytest.fixture
def agents_dir(tmp_path: Path):
    d = tmp_path / "agents"
    d.mkdir()
    spec = {
        "name": SOURCE,
        "description": "Reviews pull requests.",
        "prompt": "You review code.",
        "model": "claude-x",
        "tools": ["ReadFile"],
    }
    (d / f"{SOURCE}.json").write_text(json.dumps(spec), encoding="utf-8")
    cfg = KiroCrewConfig()
    cfg.agents = {"default": KiroCrewAgentConfig(kiro_agent="kirocrew")}
    cfg.default_agent = "default"
    cfg.save()
    # `list_agents()` (the create path's existence probe) and the fork's spec
    # reads both resolve the agents directory through kiro_crew.agent.
    with (
        patch("kiro_crew.agent.KIRO_AGENTS_DIR", d),
        patch(
            "kiro_crew.dashboard.handlers.agents.list_agents",
            lambda *a, **k: [SimpleNamespace(name=SOURCE), SimpleNamespace(name="kirocrew")],
        ),
    ):
        yield d


def _app() -> web.Application:
    from kiro_crew.dashboard.handlers import (
        api_kirocrew_agents,
        api_member_hire,
        api_members,
    )

    @web.middleware
    async def _auth(request: web.Request, handler):
        request.setdefault("app", "")
        request.setdefault("user", "local-app")
        return await handler(request)

    app = web.Application(middlewares=[_auth])
    app["state"] = MagicMock(sessions=None)
    app.router.add_post("/api/members/hire", api_member_hire)
    app.router.add_get("/api/members", api_members)
    app.router.add_get("/api/agents", api_kirocrew_agents)
    return app


def _hire(display_name: str, **extra) -> dict:
    body = {"display_name": display_name, "source": {"kind": "local", "agent": SOURCE}}
    body.update(extra)
    return body


class TestHire:
    @pytest.mark.asyncio
    async def test_hire_creates_row_and_member_owned_copy(self, agents_dir: Path):
        async with TestClient(TestServer(_app())) as client:
            resp = await client.post(
                "/api/members/hire", json=_hire("Checkout triage", role="Oncall")
            )
            assert resp.status == 200, await resp.text()
            data = await resp.json()
        # The answer is what a caller reads: the minted id and the copy. The
        # label, store and source are read back from the roster row.
        assert data == {"ok": True, "id": "Checkout-triage"}
        # The copy: a real file whose declared name equals its stem, carrying
        # the source definition; the source itself is untouched.
        copy = json.loads((agents_dir / "Checkout-triage.json").read_text(encoding="utf-8"))
        assert copy["name"] == "Checkout-triage"
        assert copy["prompt"] == "You review code."
        assert json.loads((agents_dir / f"{SOURCE}.json").read_text())["name"] == SOURCE
        # Lineage for the fork refresh, and the row rebound to the copy.
        assert agent_state.get_fork_info("Checkout-triage") == {
            "forked_from": SOURCE,
            "private_to": "Checkout-triage",
        }
        row = KiroCrewConfig.load().agents["Checkout-triage"]
        assert row.kiro_agent == "Checkout-triage"
        assert row.display_name == "Checkout triage"
        assert row.role == "Oncall"

    @pytest.mark.asyncio
    async def test_gate_two_members_from_one_file_coexist(self, agents_dir: Path):
        async with TestClient(TestServer(_app())) as client:
            a = await (await client.post("/api/members/hire", json=_hire("Checkout triage"))).json()
            b = await (await client.post("/api/members/hire", json=_hire("Payments triage"))).json()
            roster = {
                r["name"]: r for r in (await (await client.get("/api/members")).json())["members"]
            }
        assert a["id"] == "Checkout-triage" and b["id"] == "Payments-triage"
        assert a["ok"] and b["ok"]
        # Two rows, two copies, one untouched source.
        cfg = KiroCrewConfig.load()
        assert cfg.agents["Checkout-triage"].kiro_agent == "Checkout-triage"
        assert cfg.agents["Payments-triage"].kiro_agent == "Payments-triage"
        assert (agents_dir / "Checkout-triage.json").exists()
        assert (agents_dir / "Payments-triage.json").exists()
        assert json.loads((agents_dir / f"{SOURCE}.json").read_text())["name"] == SOURCE
        # Both listed, each under its own label; both created here.
        assert roster["Checkout-triage"]["display_name"] == "Checkout triage"
        assert roster["Payments-triage"]["display_name"] == "Payments triage"
        assert roster["Checkout-triage"]["source"] == "kirocrew"
        # Lineage on the roster: each is bound to its OWN copy of the source,
        # and the row says which template that copy came from. The built-in
        # default, bound to a shared template directly, reports none.
        assert roster["Checkout-triage"]["kiro_agent"] == "Checkout-triage"
        assert roster["Checkout-triage"]["template_origin"] == SOURCE
        assert roster["Payments-triage"]["template_origin"] == SOURCE
        assert roster["default"]["template_origin"] == ""

    @pytest.mark.asyncio
    async def test_same_display_name_twice_is_409_not_a_second_copy(self, agents_dir: Path):
        async with TestClient(TestServer(_app())) as client:
            assert (await client.post("/api/members/hire", json=_hire("Triage"))).status == 200
            resp = await client.post("/api/members/hire", json=_hire("triage"))
            # "triage" and "Triage" are distinct ids (the grammar is case-sensitive)
            # but the copy's FILENAME is case-folded like every spec filename
            # (macOS/Windows), so the second copy takes the next free stem.
            assert resp.status == 200
            assert KiroCrewConfig.load().agents["triage"].kiro_agent == "triage-2"
            resp = await client.post("/api/members/hire", json=_hire("Triage"))
            assert resp.status == 409
            assert (await resp.json())["code"] == "agent_exists"
        assert sorted(p.stem for p in agents_dir.glob("*.json")) == ["Triage", SOURCE, "triage-2"]

    @pytest.mark.asyncio
    async def test_copy_stem_is_suffixed_when_a_file_already_owns_the_id(self, agents_dir: Path):
        """An unrelated template already named like the member: the copy takes
        the next free stem and the row binds to THAT, never to the stranger."""
        (agents_dir / "Triage.json").write_text(json.dumps({"name": "Triage", "prompt": "other"}))
        with patch(
            "kiro_crew.dashboard.handlers.agents.list_agents",
            lambda *a, **k: [SimpleNamespace(name=SOURCE), SimpleNamespace(name="Triage")],
        ):
            async with TestClient(TestServer(_app())) as client:
                data = await (await client.post("/api/members/hire", json=_hire("Triage"))).json()
        assert data["id"] == "Triage"
        assert KiroCrewConfig.load().agents["Triage"].kiro_agent == "Triage-2"
        assert (
            json.loads((agents_dir / "Triage-2.json").read_text())["prompt"] == "You review code."
        )
        assert json.loads((agents_dir / "Triage.json").read_text())["prompt"] == "other"
        assert KiroCrewConfig.load().agents["Triage"].kiro_agent == "Triage-2"

    @pytest.mark.asyncio
    async def test_a_failed_copy_rolls_the_member_back(self, agents_dir: Path):
        """Atomic: no member is left bound to the SHARED source it was told it
        owns. The copy's error is the answer; the row and its private memory are
        gone, so a retry does not 409 on a half-made member."""
        with patch(
            "kiro_crew.dashboard.handlers.members._agents_handlers._fork_template_for_crew",
            return_value=web.json_response({"error": "x", "code": "fork_failed"}, status=500),
        ):
            async with TestClient(TestServer(_app())) as client:
                resp = await client.post("/api/members/hire", json=_hire("Triage"))
                assert resp.status == 500
                data = await resp.json()
                assert data["code"] == "fork_failed"
                assert data["rolled_back"] is True
                assert "copied" not in data
            assert set(KiroCrewConfig.load().agents) == {"default"}
            assert not (agents_dir / "Triage.json").exists()
        # A retry (the copy works again) is a clean create, not a 409 on a phantom.
        async with TestClient(TestServer(_app())) as client:
            resp = await client.post("/api/members/hire", json=_hire("Triage"))
            assert resp.status == 200, await resp.text()
        cfg = KiroCrewConfig.load()
        assert set(cfg.agents) == {"default", "Triage"}
        assert cfg.agents["Triage"].kiro_agent == "Triage"
        assert (agents_dir / "Triage.json").exists()

    @pytest.mark.asyncio
    async def test_a_source_that_vanishes_mid_hire_rolls_back_with_its_error(
        self, agents_dir: Path
    ):
        """Step 1 saw the file; it is gone by step 3 (a concurrent uninstall).
        The fork's own 404 is the answer and nothing is left behind."""
        real_fork = _agents.__dict__["_fork_template_for_crew"]

        async def vanish_then_fork(request, name, crew, **kwargs):
            (agents_dir / f"{SOURCE}.json").unlink()
            return await real_fork(request, name, crew, **kwargs)

        with patch(
            "kiro_crew.dashboard.handlers.members._agents_handlers._fork_template_for_crew",
            vanish_then_fork,
        ):
            async with TestClient(TestServer(_app())) as client:
                resp = await client.post("/api/members/hire", json=_hire("Triage"))
                assert resp.status == 404, await resp.text()
                data = await resp.json()
        assert data["code"] == "template_not_found"
        assert data["rolled_back"] is True
        assert set(KiroCrewConfig.load().agents) == {"default"}
        assert not (agents_dir / "Triage.json").exists()

    @pytest.mark.asyncio
    async def test_a_member_rebound_mid_hire_is_not_rolled_back(self, agents_dir: Path):
        """The lock is released between create and copy. A writer that rebinds
        the new member in that gap (the fork then answers stale_binding) has
        started shaping it; the roll-back must not delete their row and archive
        its memory. The hire reports the member instead."""

        async def rebind_then_fork(request, name, crew, **kwargs):
            from kiro_crew.config.loader import update_config_locked

            def mutate(doc):
                doc["agents"][crew]["kiro_agent"] = "kirocrew"
                return doc

            update_config_locked(mutate=mutate)
            return web.json_response({"error": "moved", "code": "stale_binding"}, status=409)

        with patch(
            "kiro_crew.dashboard.handlers.members._agents_handlers._fork_template_for_crew",
            rebind_then_fork,
        ):
            async with TestClient(TestServer(_app())) as client:
                resp = await client.post("/api/members/hire", json=_hire("Triage"))
                assert resp.status == 500
                data = await resp.json()
        assert data["code"] == "hire_incomplete"
        assert data["id"] == "Triage"
        assert "rolled_back" not in data
        cfg = KiroCrewConfig.load()
        assert cfg.agents["Triage"].kiro_agent == "kirocrew"

    @pytest.mark.asyncio
    async def test_a_member_recreated_mid_hire_is_neither_forked_nor_rolled_back(
        self, agents_dir: Path
    ):
        """Delete + recreate of the same display name in the create→copy gap
        re-mints the SAME id bound to the SAME source. The only tell is the
        private store name the create minted; the fork must refuse the
        replacement (stale_binding) and the roll-back must leave it."""
        from kiro_crew.config.loader import update_config_locked

        real_fork = _agents.__dict__["_fork_template_for_crew"]
        replaced: dict[str, str] = {}

        async def recreate_then_fork(request, name, crew, **kwargs):
            def mutate(doc):
                # The replacement: same id, same source, a fresh private store.
                doc["agents"][crew]["memory_store"] = "member-Triage-replacement"
                doc.setdefault("memory_stores", {})["member-Triage-replacement"] = {
                    "owner_member": crew,
                    "memory_version": 2,
                }
                replaced["store"] = "member-Triage-replacement"
                return doc

            update_config_locked(mutate=mutate)
            return await real_fork(request, name, crew, **kwargs)

        with patch(
            "kiro_crew.dashboard.handlers.members._agents_handlers._fork_template_for_crew",
            recreate_then_fork,
        ):
            async with TestClient(TestServer(_app())) as client:
                resp = await client.post("/api/members/hire", json=_hire("Triage"))
                assert resp.status == 500, await resp.text()
                data = await resp.json()
        assert data["code"] == "hire_incomplete"
        assert data["id"] == "Triage"
        assert "rolled_back" not in data
        cfg = KiroCrewConfig.load()
        # The replacement is untouched: still bound to the shared source, its
        # own store, no copy made for it.
        assert cfg.agents["Triage"].kiro_agent == SOURCE
        assert cfg.agents["Triage"].memory_store == replaced["store"]
        assert not (agents_dir / "Triage.json").exists()

    @pytest.mark.asyncio
    async def test_a_cancelled_hire_finishes_its_transaction(self, agents_dir: Path):
        """The handler is cancelled (gateway shutdown, client gone) while the
        copy step is in flight. The transaction still reaches its own end: the
        copy lands and the member is whole — never a row committed with the
        copy and the roll-back skipped."""
        import asyncio

        from aiohttp.test_utils import make_mocked_request

        from kiro_crew.dashboard.handlers import api_member_hire

        real_fork = _agents.__dict__["_fork_template_for_crew"]
        entered = asyncio.Event()
        release = asyncio.Event()

        async def slow_fork(request, name, crew, **kwargs):
            entered.set()
            await release.wait()
            return await real_fork(request, name, crew, **kwargs)

        app = _app()
        request = make_mocked_request("POST", "/api/members/hire", app=app, payload=None)
        request["user"] = "local-app"
        body = _hire("Triage")

        async def _json():
            return body

        request.json = _json  # type: ignore[method-assign]
        with patch(
            "kiro_crew.dashboard.handlers.members._agents_handlers._fork_template_for_crew",
            slow_fork,
        ):
            handler = asyncio.ensure_future(api_member_hire(request))
            await entered.wait()
            handler.cancel()  # the outer request is gone mid-copy
            await asyncio.sleep(0)
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await handler
        cfg = KiroCrewConfig.load()
        assert cfg.agents["Triage"].kiro_agent == "Triage"
        assert (agents_dir / "Triage.json").exists()

    @pytest.mark.asyncio
    async def test_a_failed_roll_back_names_the_member_to_delete(self, agents_dir: Path):
        with (
            patch(
                "kiro_crew.dashboard.handlers.members._agents_handlers._fork_template_for_crew",
                return_value=web.json_response({"error": "x", "code": "fork_failed"}, status=500),
            ),
            patch(
                "kiro_crew.dashboard.handlers.members._agents_handlers._delete_crew_record",
                side_effect=OSError("disk"),
            ),
        ):
            async with TestClient(TestServer(_app())) as client:
                resp = await client.post("/api/members/hire", json=_hire("Triage"))
                assert resp.status == 500
                data = await resp.json()
        assert data["code"] == "hire_incomplete"
        assert data["id"] == "Triage"
        assert "Triage" in data["error"]
        # The member IS still there -- the message is honest about it.
        assert "Triage" in KiroCrewConfig.load().agents


class TestHireValidation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "body, code",
        [
            ({"display_name": "x"}, "invalid_source"),
            ({"display_name": "x", "source": "reviewer"}, "invalid_source"),
            (
                {"display_name": "x", "source": {"kind": "store", "agent": SOURCE}},
                "unsupported_source_kind",
            ),
            # Unhashable kinds: a 400, not a TypeError out of the frozenset test.
            (
                {"display_name": "x", "source": {"kind": ["local"], "agent": SOURCE}},
                "unsupported_source_kind",
            ),
            (
                {"display_name": "x", "source": {"kind": {"k": 1}, "agent": SOURCE}},
                "unsupported_source_kind",
            ),
            (
                {"display_name": "x", "source": {"kind": "local", "agent": "../etc"}},
                "invalid_source_agent",
            ),
            ({"display_name": "x", "source": {"kind": "local"}}, "invalid_source_agent"),
            (
                {"display_name": 3, "source": {"kind": "local", "agent": SOURCE}},
                "invalid_display_name",
            ),
        ],
    )
    async def test_refused_bodies_write_nothing(self, agents_dir: Path, body, code):
        before = sorted(p.name for p in agents_dir.iterdir())
        async with TestClient(TestServer(_app())) as client:
            resp = await client.post("/api/members/hire", json=body)
            assert resp.status == 400, await resp.text()
            assert (await resp.json())["code"] == code
        assert sorted(p.name for p in agents_dir.iterdir()) == before
        assert set(KiroCrewConfig.load().agents) == {"default"}

    @pytest.mark.asyncio
    async def test_missing_display_name_is_400(self, agents_dir: Path):
        async with TestClient(TestServer(_app())) as client:
            resp = await client.post(
                "/api/members/hire", json={"source": {"kind": "local", "agent": SOURCE}}
            )
            assert resp.status == 400
        assert set(KiroCrewConfig.load().agents) == {"default"}

    @pytest.mark.asyncio
    async def test_unknown_source_file_is_404_before_anything_is_written(self, agents_dir: Path):
        """The create step accepts an unlisted template with a warning (an
        edition may resolve it); a hire promises a COPY of the file, so a name
        with no file behind it is refused up front and no row is created."""
        before = sorted(p.name for p in agents_dir.iterdir())
        async with TestClient(TestServer(_app())) as client:
            resp = await client.post(
                "/api/members/hire",
                json={"display_name": "Ghost", "source": {"kind": "local", "agent": "no-such"}},
            )
            assert resp.status == 404
            data = await resp.json()
        assert data["code"] == "template_not_found"
        assert "rolled_back" not in data
        assert set(KiroCrewConfig.load().agents) == {"default"}
        assert sorted(p.name for p in agents_dir.iterdir()) == before

    @pytest.mark.asyncio
    async def test_non_owner_is_refused(self, agents_dir: Path, monkeypatch):
        monkeypatch.setattr(
            "kiro_crew.dashboard.handlers.source_providers.is_owner_dashboard_request",
            lambda request: False,
        )
        async with TestClient(TestServer(_app())) as client:
            resp = await client.post("/api/members/hire", json=_hire("Triage"))
            assert resp.status in (401, 403)
        assert set(KiroCrewConfig.load().agents) == {"default"}
