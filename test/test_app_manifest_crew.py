"""The manifest's ``crew`` section: the templates an app offers for hire.

A template is a Custom Agent the app already ships under ``agents`` plus a job
card (role, triggers, initial briefing). Typed like ``contributes`` so it is
checked on every parse, round-trips, and is signed.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from kiro_crew.apps.manifest import (
    _MAX_CREW_ROLE,
    _MAX_CREW_TEMPLATES_PER_APP,
    AppManifest,
    CrewConfig,
    CrewTemplate,
)


def _manifest(**overrides) -> dict:
    base = {
        "name": "oncall-pack",
        "version": "1.2.0",
        "displayName": "Oncall pack",
        "description": "Oncall roles",
        "author": "tester",
        "agents": ["agents/triage.json", "agents/scribe.json"],
    }
    base.update(overrides)
    return base


def _template(**overrides) -> dict:
    base = {
        "agent": "agents/triage.json",
        "role": "Oncall Triage Engineer",
        "description": "Triages pages.",
        "triggers": "incident, prod outage",
        "initial_briefing": "briefings/triage.md",
    }
    base.update(overrides)
    return base


class TestParseAndRoundTrip:
    def test_parses_into_typed_templates(self):
        m = AppManifest.from_dict(_manifest(crew={"templates": [_template()]}))
        assert isinstance(m.crew, CrewConfig)
        assert len(m.crew.templates) == 1
        t = m.crew.templates[0]
        assert isinstance(t, CrewTemplate)
        assert (t.agent, t.role, t.triggers) == (
            "agents/triage.json",
            "Oncall Triage Engineer",
            "incident, prod outage",
        )
        assert t.initial_briefing == "briefings/triage.md"
        # A known field, not `extra`.
        assert "crew" not in m.extra

    def test_round_trips_and_omits_an_empty_section(self):
        d = _manifest(crew={"templates": [_template()]})
        assert AppManifest.from_dict(d).to_dict()["crew"] == d["crew"]
        assert "crew" not in AppManifest.from_dict(_manifest()).to_dict()

    def test_role_whitespace_is_collapsed_and_non_strings_read_as_empty(self):
        m = AppManifest.from_dict(
            _manifest(crew={"templates": [_template(role="  Oncall   Triage ", triggers=7)]})
        )
        t = m.crew.templates[0]
        assert t.role == "Oncall Triage"
        assert t.triggers == ""


class TestValidation:
    def test_a_well_formed_section_validates(self, tmp_path: Path):
        m = AppManifest.from_dict(_manifest(crew={"templates": [_template()]}))
        assert m.validate(app_root=tmp_path) == []

    def test_agent_must_be_one_of_the_shipped_agents(self):
        m = AppManifest.from_dict(
            _manifest(crew={"templates": [_template(agent="agents/ghost.json")]})
        )
        errors = m.validate()
        assert any("not one of the manifest's agents paths" in e for e in errors)
        m = AppManifest.from_dict(_manifest(crew={"templates": [_template(agent="")]}))
        assert any("agent is required" in e for e in m.validate())

    def test_role_is_required_and_bounded(self):
        m = AppManifest.from_dict(_manifest(crew={"templates": [_template(role="")]}))
        assert any("role is required" in e for e in m.validate())
        m = AppManifest.from_dict(
            _manifest(crew={"templates": [_template(role="x" * (_MAX_CREW_ROLE + 1))]})
        )
        assert any(f"role exceeds {_MAX_CREW_ROLE}" in e for e in m.validate())

    @pytest.mark.parametrize("briefing", ["../secrets.md", "/etc/passwd.md", "notes.txt"])
    def test_initial_briefing_is_a_markdown_file_inside_the_app(self, briefing, tmp_path):
        m = AppManifest.from_dict(
            _manifest(crew={"templates": [_template(initial_briefing=briefing)]})
        )
        errors = m.validate(app_root=tmp_path)
        assert errors, briefing
        assert all("initial_briefing" in e for e in errors)

    def test_duplicate_agents_and_the_cap_are_refused(self):
        m = AppManifest.from_dict(_manifest(crew={"templates": [_template(), _template()]}))
        assert any("duplicate agent" in e for e in m.validate())
        many = [_template() for _ in range(_MAX_CREW_TEMPLATES_PER_APP + 1)]
        m = AppManifest.from_dict(_manifest(crew={"templates": many}))
        assert any(f"at most {_MAX_CREW_TEMPLATES_PER_APP}" in e for e in m.validate())

    @pytest.mark.parametrize(
        "crew, needle",
        [
            ("nope", "crew must be an object"),
            ({"templates": "nope"}, "crew.templates must be an array"),
            ({"templates": [_template(), "nope", 3]}, "2 entries are not an object"),
        ],
    )
    def test_malformed_shapes_fail_validation_instead_of_vanishing(self, crew, needle):
        """The fail-open shape `contributes` closes: a value coerced to "nothing"
        would install clean and never list a template."""
        m = AppManifest.from_dict(_manifest(crew=crew))
        errors = m.validate()
        assert any(needle in e for e in errors), errors


class TestSigning:
    def test_the_job_card_is_part_of_the_signed_payload(self):
        plain = AppManifest.from_dict(_manifest(signer="k1"))
        with_crew = AppManifest.from_dict(_manifest(signer="k1", crew={"templates": [_template()]}))
        assert plain.signing_payload() != with_crew.signing_payload()
        body = json.loads(with_crew.signing_payload())
        assert body["crew"]["templates"][0]["role"] == "Oncall Triage Engineer"
        # Changing the role alone changes the bytes.
        tampered = AppManifest.from_dict(
            _manifest(signer="k1", crew={"templates": [_template(role="Boss")]})
        )
        assert tampered.signing_payload() != with_crew.signing_payload()

    def test_a_manifest_without_templates_keeps_its_pre_crew_payload(self):
        """Manifests signed before templates existed must verify unchanged."""
        m = AppManifest.from_dict(_manifest(signer="k1"))
        assert "crew" not in json.loads(m.signing_payload())
