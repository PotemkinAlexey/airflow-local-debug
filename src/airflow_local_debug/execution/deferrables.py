from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict
from typing import Any

from airflow_local_debug.models import DeferrableTaskInfo


def detect_deferrable_tasks(dag: Any, *, backend_hint: str | None = None) -> list[DeferrableTaskInfo]:
    infos = []
    for task in list(getattr(dag, "task_dict", {}).values()):
        if not _looks_deferrable(task):
            continue
        infos.append(
            DeferrableTaskInfo(
                task_id=str(getattr(task, "task_id", "<unknown>")),
                operator=_operator_label(task),
                trigger=_trigger_label(task),
                local_mode=_local_mode(backend_hint),
                reason=_local_reason(backend_hint),
            )
        )
    infos.sort(key=lambda info: info.task_id)
    return infos


def format_deferrable_note(infos: list[DeferrableTaskInfo], *, limit: int = 5) -> str | None:
    if not infos:
        return None

    chunks = []
    for info in infos[:limit]:
        trigger = f", trigger={info.trigger}" if info.trigger else ""
        mode = f", mode={info.local_mode}" if info.local_mode else ""
        chunks.append(f"{info.task_id} ({info.operator}{trigger}{mode})")
    if len(infos) > limit:
        chunks.append(f"... +{len(infos) - limit} more")

    reason = infos[0].reason
    suffix = f" {reason}" if reason else ""
    return "Detected deferrable task(s): " + ", ".join(chunks) + "." + suffix


def deferrable_infos_to_dicts(infos: list[DeferrableTaskInfo]) -> list[dict[str, Any]]:
    return [asdict(info) for info in infos]


def _looks_deferrable(task: Any) -> bool:
    if bool(getattr(task, "deferrable", False)):
        return True
    if bool(getattr(task, "start_from_trigger", False)):
        return True
    start_trigger_args = getattr(task, "start_trigger_args", None)
    return start_trigger_args not in (None, {}, (), [])


def _operator_label(task: Any) -> str:
    task_type = getattr(task, "task_type", None)
    if task_type:
        return str(task_type)
    return str(task.__class__.__name__)


def _trigger_label(task: Any) -> str | None:
    start_trigger_args = getattr(task, "start_trigger_args", None)
    if start_trigger_args not in (None, {}, (), []):
        return _start_trigger_args_label(start_trigger_args)

    trigger = getattr(task, "trigger", None)
    if trigger is None:
        return None
    if isinstance(trigger, str):
        return trigger
    cls = trigger.__class__
    return f"{cls.__module__}.{cls.__qualname__}"


def _start_trigger_args_label(value: Any) -> str | None:
    if isinstance(value, Mapping):
        candidate = value.get("trigger_cls") or value.get("trigger_class")
    else:
        candidate = getattr(value, "trigger_cls", None) or getattr(value, "trigger_class", None)
    if candidate is None:
        return None
    if isinstance(candidate, str):
        return candidate
    module = getattr(candidate, "__module__", None)
    qualname = getattr(candidate, "__qualname__", None)
    if module and qualname:
        return f"{module}.{qualname}"
    return str(candidate)


def _local_mode(backend_hint: str | None) -> str:
    if backend_hint == "dag.test.strict":
        return "inline-trigger"
    if backend_hint == "dag.test":
        return "native-dag-test"
    if backend_hint == "dag.run":
        return "legacy-dag-run"
    return "unsupported"


def _local_reason(backend_hint: str | None) -> str:
    if backend_hint == "dag.test.strict":
        return "Strict local mode runs deferrals inline when Airflow exposes trigger execution support."
    if backend_hint == "dag.test":
        return "Native dag.test handling may depend on Airflow/provider trigger behavior."
    if backend_hint == "dag.run":
        return "Legacy dag.run may leave deferrable tasks deferred; prefer fail_fast=True or mock these tasks."
    return "This Airflow runtime does not expose a supported local execution backend."
