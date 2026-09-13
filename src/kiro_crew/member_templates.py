"""Crew templates: the store listing a crew member is hired from.

A template is an installed app's Custom Agent plus the job card its manifest's
``crew`` section carries (:class:`kiro_crew.apps.manifest.CrewTemplate`). This
module is the read side the hire route composes:

* :func:`resolve_store_template` -- from ``{app, agent}`` to the materialized
  agent file (``<app>--<agent>``, the copy ``apps.bridges`` writes into the
  agents directory when the app is enabled), the card, the version, the agent
  definition and the initial briefing text.
* :func:`template_ref` -- the ``template`` the wrapper row records
  (``<app>/<agent>``), the same namespacing the materialized file uses.
* :func:`write_pristine_copy` -- the unmodified template payload at the
  installed version, kept at ``members/<slug>/template.json`` so a later role
  update can three-way merge (BASE = this file, MINE = the member's agent file,
  THEIRS = the new version). The reader arrives with that update.
* :func:`seed_briefing` -- copy ``initial_briefing`` ONCE into the member's own
  ``briefing.md``; from then on the file is the member's lived state and no
  template operation touches it.

Leaf-ish on purpose: imports the manifest, the members module and the agents
directory resolver, never the dashboard handlers.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from kiro_crew import members
from kiro_crew.agent import kiro_agents_dir_path
from kiro_crew.apps.bridges import _namespace, _safe_link_name
from kiro_crew.apps.manager import app_dir, get_app_manifest, is_app_enabled
from kiro_crew.apps.manifest import CrewTemplate
from kiro_crew.atomic_write import atomic_write
from kiro_crew.pinned_fs import create_and_open_dir_pinned, read_file_pinned

logger = logging.getLogger(__name__)

#: The pristine copy's file name inside ``members/<slug>/``.
PRISTINE_COPY_FILE = "template.json"

#: Largest initial briefing a template may seed (bytes). The member's own
#: briefing is injection-capped downstream; this bounds what a store listing can
#: put on disk in one hire.
INITIAL_BRIEFING_MAX_BYTES = 64 * 1024

#: Largest agent definition a template may ship (bytes) -- the same order as the
#: spec reader caps elsewhere; a store listing is not a place for a novel.
TEMPLATE_SPEC_MAX_BYTES = 512 * 1024

_SAFE_APP_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")


class TemplateUnavailable(Exception):
    """A store template cannot be hired from right now; ``code`` says why.

    ``code`` is a stable machine-readable word the hire route answers with:
    ``app_not_installed``, ``app_disabled``, ``template_not_offered``,
    ``template_spec_unreadable``, ``template_not_materialized``.
    """

    def __init__(self, code: str, message: str, *, status: int = 404):
        super().__init__(message)
        self.code = code
        self.status = status


@dataclass
class StoreTemplate:
    """Everything a hire needs from one store listing, resolved once."""

    app: str
    version: str
    card: CrewTemplate
    #: The declared agent name (spec ``name`` else the file stem).
    agent_name: str
    #: The materialized agent file's stem: ``<app>--<agent_name>``. This is the
    #: ``kiro_agent`` the member is created against and then forked from.
    materialized: str
    #: The template's agent definition as shipped (the pristine BASE).
    spec: dict[str, Any] = field(default_factory=dict)
    initial_briefing: str = ""

    @property
    def ref(self) -> str:
        return template_ref(self.app, self.agent_name)


def template_ref(app: str, agent_name: str) -> str:
    """The ``template`` a wrapper row records: ``<app>/<agent>``."""
    return _namespace(app, agent_name)


def _read_text_pinned(path: Path, cap: int, *, what: str) -> str | None:
    """Read an app- or member-controlled file without following a planted link.

    An installed app's tree is the app's to change between install and hire, and
    a member's directory is agent-writable: a by-name ``read_text`` on either is
    a disclosure primitive pointed at whatever the name resolves to by then --
    replace the briefing with a symlink to a credential file and the next hire
    copies that credential into a prompt-visible briefing. ``read_file_pinned``
    pins the ancestors and refuses a non-regular final component. ``None`` for a
    missing, refused, oversized or undecodable file.
    """
    try:
        text = read_file_pinned(path, what=what, max_bytes=cap + 1)
    except (OSError, ValueError):
        return None
    if len(text.encode("utf-8", "surrogatepass")) > cap:
        return None
    return text


def _read_json_capped(path: Path, cap: int, *, what: str) -> dict[str, Any] | None:
    text = _read_text_pinned(path, cap, what=what)
    if text is None:
        return None
    try:
        data = json.loads(text)
    except ValueError:
        return None
    return data if isinstance(data, dict) else None


def resolve_store_template(app: str, agent_path: str) -> StoreTemplate:
    """Resolve ``{app, agent}`` to a hireable :class:`StoreTemplate`.

    ``agent_path`` is the manifest ``agents`` entry the card names. Raises
    :class:`TemplateUnavailable` for every way the listing cannot be hired
    from: the app is not installed or not enabled (only an enabled app has its
    agents materialized), the manifest offers no card for that agent, the
    shipped spec is unreadable, or the materialized file is missing.
    """
    if not isinstance(app, str) or not _SAFE_APP_NAME_RE.match(app):
        raise TemplateUnavailable("app_not_installed", "source.app must name an installed app")
    manifest = get_app_manifest(app)
    if manifest is None:
        raise TemplateUnavailable("app_not_installed", f"App '{app}' is not installed")
    if not is_app_enabled(app):
        raise TemplateUnavailable(
            "app_disabled", f"App '{app}' is disabled; enable it to hire from it", status=409
        )
    card = next((t for t in manifest.crew.templates if t.agent == agent_path), None)
    if card is None:
        raise TemplateUnavailable(
            "template_not_offered", f"App '{app}' offers no template for {agent_path!r}"
        )
    spec_path = app_dir(app) / agent_path
    spec = _read_json_capped(spec_path, TEMPLATE_SPEC_MAX_BYTES, what="template agent file")
    if spec is None:
        raise TemplateUnavailable(
            "template_spec_unreadable",
            f"The template's agent file {agent_path!r} could not be read",
            status=409,
        )
    declared = spec.get("name")
    agent_name = declared if isinstance(declared, str) and declared else Path(agent_path).stem
    materialized = _safe_link_name(_namespace(app, agent_name))
    if not (kiro_agents_dir_path() / f"{materialized}.json").is_file():
        raise TemplateUnavailable(
            "template_not_materialized",
            f"App '{app}' has not installed its agent {agent_name!r} yet; re-enable the app",
            status=409,
        )
    briefing = ""
    if card.initial_briefing:
        text = _read_text_pinned(
            app_dir(app) / card.initial_briefing,
            INITIAL_BRIEFING_MAX_BYTES,
            what="template initial briefing",
        )
        if text is None:
            logger.warning(
                "template %s/%s: initial_briefing missing, oversized or not a regular file; "
                "not seeded",
                app,
                agent_name,
            )
        else:
            briefing = text
    return StoreTemplate(
        app=app,
        version=manifest.version,
        card=card,
        agent_name=agent_name,
        materialized=materialized,
        spec=spec,
        initial_briefing=briefing,
    )


def pristine_copy_path(slug: str) -> Path:
    return members.member_dir(slug) / PRISTINE_COPY_FILE


def write_pristine_copy(slug: str, template: StoreTemplate) -> Path:
    """Record the unmodified template payload at the installed version.

    The BASE of a later three-way merge: the agent definition as shipped plus
    the card's mergeable fields (role, triggers). Written atomically; the
    member directory is created if the member has not written anything yet.
    """
    path = pristine_copy_path(slug)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "template": template.ref,
        "version": template.version,
        "agent": template.spec,
        "card": {"role": template.card.role, "triggers": template.card.triggers},
    }
    atomic_write(path, json.dumps(payload, indent=2, ensure_ascii=False), fsync=True)
    return path


def seed_briefing(slug: str, text: str) -> bool:
    """Copy a template's initial briefing into the member's own ``briefing.md`` ONCE.

    True when the file was written. A briefing that already exists is the
    member's lived state and is left alone -- decided by the CREATE itself
    (``O_CREAT | O_EXCL`` through the pinned member directory), not by a check
    before it: a member that starts writing its notes between an existence check
    and an atomic replace would have them overwritten by template text. A
    platform where briefings are not read (no ``O_NOFOLLOW``) gets none, so the
    member is not handed a file the runtime will never inject.
    """
    if not text or not members.member_briefing_supported():
        return False
    path = members.member_briefing_path(slug)
    dir_fd = create_and_open_dir_pinned(path.parent, what="member directory")
    try:
        try:
            fd = os.open(
                path.name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=dir_fd,
            )
        except FileExistsError:
            return False
        try:
            os.write(fd, text.encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        os.close(dir_fd)
    return True
