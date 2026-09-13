"""Trusted workflow execution identity, separate from scripts and run snapshots.

The binding is published under the existing read-only member binding root.
Private payloads live under the existing hidden memory root. Neither a script's
session label nor editable run metadata can mint or replace that authority.
"""

from __future__ import annotations

import asyncio
import contextvars
import hashlib
import json
import logging
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kiro_crew import platform_compat
from kiro_crew.atomic_write import atomic_write, fsync_dir
from kiro_crew.config.paths import config_dir
from kiro_crew.member_memory_auth import (
    _publish_private_binding_dir,
    bind_private_session_store,
    private_memory_store_for_session,
    require_private_memory_execution,
)
from kiro_crew.memory_stores import require_memory_store
from kiro_crew.session_pid_sig import _read_regular_nofollow


class WorkflowMemoryError(RuntimeError):
    """The workflow cannot safely continue under its original identity."""


def _plain_path(path: Path) -> Path:
    """Compare resolved Windows paths without the optional extended prefix.

    realpath may retain this prefix when another thread creates the leaf during
    its final spelling check. Do not resolve again: that could bless a redirect.
    """
    text = str(path)
    if text.startswith("\\\\?\\UNC\\"):
        return type(path)("\\\\" + text[8:])
    if text.startswith("\\\\?\\"):
        return type(path)(text[4:])
    return path


def binding_path(run_id: str) -> Path:
    digest = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    path = config_dir().resolve() / "member-memory-bindings" / "workflows" / digest / "memory.json"
    if _plain_path(path.resolve()) != _plain_path(path):
        raise WorkflowMemoryError("Workflow binding path is redirected")
    return path


def reserve_run_id(run_id: str) -> bool:
    """Burn an ID atomically across gateway instances, without granting a scope.

    Reservations are permanent, including failed/cancelled admission. They
    contain no memory authority; immutable binding publication remains separate.
    """
    binding = binding_path(run_id)
    root = binding.parent.parent / ".reserved"
    reservation = root / binding.parent.name
    if _plain_path(reservation.resolve()) != _plain_path(reservation):
        raise WorkflowMemoryError("Workflow reservation path is redirected")
    for directory in (root.parent.parent, root.parent, root):
        platform_compat.make_owner_only_dir(directory)
        platform_compat.restrict_dir_to_owner(directory)
    if binding.parent.exists():
        return False
    try:
        reservation.mkdir(mode=0o700)
    except FileExistsError:
        return False
    fsync_dir(root)
    # A surviving old binding is never reused, even if written by an older host.
    return not binding.parent.exists()


def read_binding(run_id: str, *, required: bool = False) -> dict[str, Any] | None:
    path = binding_path(run_id)
    raw = _read_regular_nofollow(path)
    if raw is None:
        if required or path.parent.exists():
            raise WorkflowMemoryError("The protected workflow binding is missing or unreadable")
        return None
    try:
        row = json.loads(raw)
        if (
            not isinstance(row, dict)
            or row.get("version") != 1
            or row.get("run_id") != run_id
            or not isinstance(row.get("memory_store"), str)
            or row["memory_store"] == "default"
            or not isinstance(row.get("origin"), str)
        ):
            raise ValueError("invalid workflow identity")
        return row
    except (ValueError, TypeError) as exc:
        raise WorkflowMemoryError("The protected workflow binding is invalid") from exc


def publish_binding(run_id: str, store: str, origin: str) -> None:
    """Called only after trusted gateway admission, never from an HTTP body."""
    path = binding_path(run_id)
    row = {"version": 1, "run_id": run_id, "memory_store": store, "origin": origin}
    for directory in (path.parent.parent.parent, path.parent.parent):
        platform_compat.make_owner_only_dir(directory)
        platform_compat.restrict_dir_to_owner(directory)
    existing = read_binding(run_id)
    if existing is not None:
        if existing != row:
            raise WorkflowMemoryError("Workflow execution identity is immutable")
        return
    stage = Path(tempfile.mkdtemp(prefix=".workflow-", dir=path.parent.parent))
    try:
        platform_compat.restrict_dir_to_owner(stage)
        atomic_write(stage / path.name, json.dumps(row), fsync=True, restrict_to_owner=True)
        fsync_dir(stage)
        try:
            _publish_private_binding_dir(stage, path.parent)
        except FileExistsError:
            if read_binding(run_id, required=True) != row:
                raise WorkflowMemoryError("Workflow execution identity is immutable")
        else:
            fsync_dir(path.parent.parent)
    finally:
        shutil.rmtree(stage, ignore_errors=True)


def private_payload_path(run_id: str) -> Path:
    digest = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    path = config_dir().resolve() / "memory_stores" / ".workflow-runs" / f"{digest}.json"
    if _plain_path(path.resolve()) != _plain_path(path):
        raise WorkflowMemoryError("Private workflow payload path is redirected")
    return path


@dataclass(frozen=True)
class WorkflowScope:
    run_id: str
    store: str
    origin: str

    @property
    def anchor(self) -> str:
        return f"wf-scope:{self.run_id}"

    @classmethod
    async def admit(
        cls,
        run_id: str,
        context: Any,
        *parents: str,
        origin: str | None = None,
        expected_store: str | None = None,
    ) -> WorkflowScope:
        """Freeze verified gateway parent selections before scheduling a run."""
        stores = [
            await asyncio.to_thread(private_memory_store_for_session, key)
            for key in dict.fromkeys(key for key in parents if key)
        ]
        if len(set(stores)) > 1:
            raise WorkflowMemoryError("Workflow caller and delivery memory must match")
        store = stores[0] if stores else ""
        if expected_store is not None and store != expected_store:
            raise WorkflowMemoryError("Workflow caller memory changed during admission")
        scope = cls(
            run_id,
            store,
            origin if origin is not None else next((key for key in parents if key), ""),
        )
        existing = await asyncio.to_thread(read_binding, run_id)
        if existing is not None:
            restored = await cls.restore(run_id)
            if restored != scope:
                raise WorkflowMemoryError("Workflow execution identity is immutable")
            return restored
        if store:
            if context is None or getattr(context, "conversation_log", None) is None:
                raise WorkflowMemoryError("Private workflow context is unavailable")
            await asyncio.to_thread(require_private_memory_execution, session_key=scope.anchor)
            await asyncio.to_thread(require_memory_store, store)
            await asyncio.to_thread(bind_private_session_store, scope.anchor, store)
            await asyncio.to_thread(
                context.conversation_log.update_metadata, scope.anchor, {"memory_store": store}
            )
        await asyncio.to_thread(publish_binding, run_id, store, scope.origin)
        await scope.validate()
        return scope

    @classmethod
    async def restore(cls, run_id: str) -> WorkflowScope:
        row = await asyncio.to_thread(read_binding, run_id, required=True)
        assert row is not None
        scope = cls(run_id, row["memory_store"], row["origin"])
        await scope.validate()
        return scope

    async def validate(self) -> None:
        try:
            row = await asyncio.to_thread(read_binding, self.run_id, required=True)
            if row != {
                "version": 1,
                "run_id": self.run_id,
                "memory_store": self.store,
                "origin": self.origin,
            }:
                raise WorkflowMemoryError("Workflow execution identity changed")
            if self.store:
                await asyncio.to_thread(require_memory_store, self.store)
                await asyncio.to_thread(require_private_memory_execution, session_key=self.anchor)
                actual = await asyncio.to_thread(private_memory_store_for_session, self.anchor)
                if actual != self.store:
                    raise WorkflowMemoryError("Workflow anchor no longer matches its store")
        except Exception as exc:
            raise WorkflowMemoryError(
                "Workflow memory is unavailable; Global V1 was not used"
            ) from exc

    def worker_key(self, label: str) -> str:
        # Hash only a local label. It is not an existing chat/session capability.
        digest = hashlib.sha256(label.encode("utf-8")).hexdigest()
        return f"wf-worker:{self.run_id}:{digest}"

    async def prepare(self, context: Any, key: str) -> str:
        await self.validate()
        if self.store:
            from kiro_crew.context import inherit_session_memory

            actual = await inherit_session_memory(context, self.anchor, key)
            if actual != self.store:
                raise WorkflowMemoryError("Workflow worker memory does not match the run")
        elif await asyncio.to_thread(private_memory_store_for_session, key):
            raise WorkflowMemoryError("Global workflow cannot acquire a private conversation")
        return self.store

    async def prompt(
        self,
        context: Any,
        key: str,
        text: str,
        *,
        is_new: bool,
        agent: str | None,
        cwd: str | None,
        provider: Any,
        resumed: bool = False,
    ) -> str:
        await self.prepare(context, key)
        if not self.store:
            return text
        from kiro_crew.context import prepare_store_vectors
        from kiro_crew.executors import run_in_embed_pool

        await prepare_store_vectors(context, self.store, session_key=key)
        full, _ = await run_in_embed_pool(
            context.build_message,
            text,
            is_new,
            key,
            agent=agent,
            project=cwd,
            memory_store=self.store,
            runtime_source="workflow",
            context_provider=provider,
            resumed=resumed,
        )
        await self.validate()
        return full


async def authorize_run(
    run_id: str,
    caller: str,
    *,
    owner: bool = False,
    required: bool = False,
    require_active: bool = True,
) -> WorkflowScope | None:
    """Authorize before reading run content; absence is only legacy V1."""
    row = await asyncio.to_thread(read_binding, run_id, required=required)
    caller_store = (
        "" if owner else await asyncio.to_thread(private_memory_store_for_session, caller)
    )
    if row is None:
        if caller_store:
            raise WorkflowMemoryError("A private caller cannot access another workflow")
        return None
    scope = WorkflowScope(run_id, row["memory_store"], row["origin"])
    if require_active or not owner:
        await scope.validate()
    if not owner and scope.store != caller_store:
        raise WorkflowMemoryError("The workflow belongs to another memory scope")
    return scope


def admission_errors(function):
    """Keep expected binding refusals in the workflow service response contract."""
    import functools

    from kiro_crew.memory_stores import UnknownMemoryStore

    @functools.wraps(function)
    async def guarded(*args, **kwargs):
        try:
            return await function(*args, **kwargs)
        except (WorkflowMemoryError, UnknownMemoryStore):
            message = "Workflow memory is unavailable; no Global V1 fallback was used."
            return {
                "ok": False,
                "error": message,
                "errors": [message],
                "code": "workflow_memory_unavailable",
                "admission_rejected": True,
            }

    return private_task_operation(guarded)


def task_snapshot_path(public_path: Path) -> Path:
    """Private task state never lives in the agent-readable execution directory."""
    digest = hashlib.sha256(str(public_path.resolve()).encode("utf-8")).hexdigest()
    path = config_dir().resolve() / "memory_stores" / ".task-runs" / f"{digest}.json"
    if _plain_path(path.resolve()) != _plain_path(path):
        raise WorkflowMemoryError("Private task snapshot path is redirected")
    return path


def _private_task_rows(path: Path) -> dict[str, dict]:
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if not isinstance(rows, list) or any(
        not isinstance(row, dict) or not isinstance(row.get("task_id"), str) for row in rows
    ):
        raise WorkflowMemoryError("Private task snapshot is invalid")
    return {row["task_id"]: row for row in rows}


def _task_private_binding(row: dict) -> str | None:
    from kiro_crew.member_memory_auth import read_private_session_store

    task_id = row.get("task_id")
    if not isinstance(task_id, str) or not task_id:
        return None
    return read_private_session_store(f"taskrunner:{task_id}:runtime")


def write_task_snapshot(public_path: Path, payload: str, *, writer: Any = atomic_write) -> None:
    """Persist private rows first, then only references in the public registry.

    Called under TaskRunner's existing snapshot sequence/write lock. Missing
    authority never converts an already-private row into public state.
    """
    private_path = task_snapshot_path(public_path)
    previous = _private_task_rows(private_path)
    private, public = [], []
    for row in json.loads(payload):
        if _task_private_binding(row) is not None or row.get("task_id") in previous:
            private.append(row)
            public.append({"task_id": row["task_id"], "private_payload": True})
        else:
            public.append(row)
    if private or previous:
        platform_compat.make_owner_only_dir(private_path.parent)
        platform_compat.restrict_dir_to_owner(private_path.parent)
        retained = dict(previous)
        retained.update({row["task_id"]: row for row in private})
        writer(
            private_path, json.dumps(list(retained.values())), fsync=True, restrict_to_owner=True
        )
    writer(public_path, json.dumps(public), fsync=True)


def read_task_snapshot(public_path: Path, *, public_payload: str | None = None) -> str:
    """Hydrate private references only while their protected runtime survives."""
    rows = json.loads(
        public_path.read_text(encoding="utf-8") if public_payload is None else public_payload
    )
    private = _private_task_rows(task_snapshot_path(public_path))
    hydrated = []
    for row in rows:
        task_id = row.get("task_id")
        if row.get("private_payload") is True or task_id in private:
            original = private.get(task_id)
            if original is None or _task_private_binding(original) is None:
                raise WorkflowMemoryError("Private task runtime binding is unavailable")
            hydrated.append(original)
        else:
            hydrated.append(row)
    return json.dumps(hydrated)


# Task workers inherit the creator's context, including after the HTTP request
# returns. Only task diagnostics are filtered; owner and public tasks stay intact.

_PRIVATE_DIAGNOSTIC_SCOPE = contextvars.ContextVar("workflow_private_diagnostics", default="")


class _PrivateTaskDiagnostics(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        scope = _PRIVATE_DIAGNOSTIC_SCOPE.get()
        if scope:
            failure = (
                record.exc_info[0].__name__
                if record.exc_info and record.exc_info[0]
                else record.levelname
            )
            record.msg = "Private task %s: %s"
            record.args = (scope, failure)
            record.exc_info = None
            record.exc_text = None
            record.stack_info = None
        return True


_TASK_DIAGNOSTIC_FILTER = _PrivateTaskDiagnostics()
for _logger_name in (
    "taskrunner",
    "task_executor",
    "task_planner",
    "task_reporter",
    "git_coord",
    "workflows.service",
    "workflows.agent_exec",
    "workflows.agent_pool",
    "workflows.runner",
):
    logging.getLogger(f"kiro_crew.{_logger_name}").addFilter(_TASK_DIAGNOSTIC_FILTER)


@contextmanager
def private_task_diagnostics(session_key: str):
    scope = hashlib.sha256(session_key.encode("utf-8")).hexdigest()[:12]
    token = _PRIVATE_DIAGNOSTIC_SCOPE.set(scope)
    try:
        yield
    finally:
        _PRIVATE_DIAGNOSTIC_SCOPE.reset(token)


def private_task_operation(function):
    """Scope task logs from protected identity, never an environment flag."""
    import functools

    from kiro_crew.member_memory_auth import read_private_session_store

    @functools.wraps(function)
    async def wrapped(self, *args, **kwargs):
        origin = (
            kwargs.get("session_key") or kwargs.get("author") or kwargs.get("caller_session", "")
        )
        task_id = kwargs.get("task_id")
        if not task_id and function.__name__ in {"execute_plan", "retry_from_task"} and args:
            task_id = args[0]
        key = f"taskrunner:{task_id}:runtime" if task_id else origin
        if not key and function.__name__ == "rerun_subtree" and args:
            try:
                row = await asyncio.to_thread(read_binding, args[0])
            except WorkflowMemoryError:
                row = {"memory_store": "unavailable"}
            if row is not None and row["memory_store"]:
                key = f"wf-scope:{args[0]}"
        private = False
        if key:
            # Missing/degraded identity must not expose a failure's private text.
            try:
                private = await asyncio.to_thread(read_private_session_store, key) is not None
            except Exception:
                private = True
        if private:
            with private_task_diagnostics(key):
                return await function(self, *args, **kwargs)
        return await function(self, *args, **kwargs)

    return wrapped
