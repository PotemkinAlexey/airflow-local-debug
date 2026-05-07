from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import fnmatch
import json
from pathlib import Path
from types import MethodType
from typing import Any, Iterable, Iterator, Mapping

from airflow_local_debug.models import TaskMockInfo

_MISSING = object()
_NO_ORIGINAL = object()
_SUPPORTED_MODES = {"success"}


@dataclass
class TaskMockRule:
    task_id: str | None = None
    task_id_glob: str | None = None
    operator: str | None = None
    operator_glob: str | None = None
    mode: str = "success"
    xcom: dict[str, Any] = field(default_factory=dict)
    name: str | None = None
    required: bool = True

    @property
    def return_value(self) -> Any:
        return self.xcom.get("return_value")

    @property
    def xcom_keys(self) -> list[str]:
        return sorted(str(key) for key in self.xcom)

    def describe(self) -> str:
        selectors = []
        if self.task_id:
            selectors.append(f"task_id={self.task_id}")
        if self.task_id_glob:
            selectors.append(f"task_id_glob={self.task_id_glob}")
        if self.operator:
            selectors.append(f"operator={self.operator}")
        if self.operator_glob:
            selectors.append(f"operator_glob={self.operator_glob}")
        label = self.name or ", ".join(selectors) or "<unscoped>"
        return f"{label} ({self.mode})"


class TaskMockRegistry:
    def __init__(self) -> None:
        self._applied: dict[str, TaskMockInfo] = {}

    @property
    def mocked_task_ids(self) -> set[str]:
        return set(self._applied)

    @property
    def mock_infos(self) -> list[TaskMockInfo]:
        return [self._applied[task_id] for task_id in sorted(self._applied)]

    def add(self, task_id: str, rule: TaskMockRule) -> None:
        self._applied[task_id] = TaskMockInfo(
            task_id=task_id,
            mode=rule.mode,
            rule_name=rule.name,
            xcom_keys=rule.xcom_keys,
        )


def load_task_mock_rules(path: str | Path) -> list[TaskMockRule]:
    resolved = Path(path).expanduser()
    try:
        raw = resolved.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"Could not read mock file {resolved}: {exc}") from exc

    try:
        payload = _parse_mock_payload(raw, suffix=resolved.suffix.lower())
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Could not parse mock file {resolved}: {exc}") from exc

    try:
        return task_mock_rules_from_payload(payload)
    except ValueError as exc:
        raise ValueError(f"Invalid mock file {resolved}: {exc}") from exc


def task_mock_rules_from_payload(payload: Any) -> list[TaskMockRule]:
    if isinstance(payload, Mapping):
        raw_rules = payload.get("mocks", payload.get("task_mocks"))
    else:
        raw_rules = payload

    if raw_rules is None:
        return []
    if not isinstance(raw_rules, list):
        raise ValueError("mock payload must be a list or an object with a 'mocks' list.")

    rules = [_task_mock_rule_from_mapping(index, item) for index, item in enumerate(raw_rules, start=1)]
    return rules


def task_matches_mock_rule(task: Any, rule: TaskMockRule) -> bool:
    task_id = str(getattr(task, "task_id", "") or "")
    class_candidates = _operator_candidates(task)

    if rule.task_id is not None and task_id != rule.task_id:
        return False
    if rule.task_id_glob is not None and not fnmatch.fnmatchcase(task_id, rule.task_id_glob):
        return False
    if rule.operator is not None and rule.operator not in class_candidates:
        return False
    if rule.operator_glob is not None and not any(
        fnmatch.fnmatchcase(candidate, rule.operator_glob) for candidate in class_candidates
    ):
        return False

    return any((rule.task_id, rule.task_id_glob, rule.operator, rule.operator_glob))


@contextmanager
def local_task_mocks(
    dag: Any,
    rules: Iterable[TaskMockRule] | None,
    *,
    notes: list[str] | None = None,
) -> Iterator[TaskMockRegistry]:
    rule_list = list(rules or [])
    registry = TaskMockRegistry()
    if not rule_list:
        yield registry
        return

    originals: dict[int, Any] = {}
    tasks = list(getattr(dag, "task_dict", {}).values())
    matched_counts = [0 for _ in rule_list]

    try:
        for task in tasks:
            matches = [index for index, rule in enumerate(rule_list) if task_matches_mock_rule(task, rule)]
            if not matches:
                continue
            if len(matches) > 1:
                descriptions = ", ".join(rule_list[index].describe() for index in matches)
                raise ValueError(f"Task {getattr(task, 'task_id', '<unknown>')} matches multiple mock rules: {descriptions}.")
            rule_index = matches[0]
            rule = rule_list[rule_index]
            matched_counts[rule_index] += 1
            originals[id(task)] = task.__dict__.get("execute", _MISSING)
            _patch_execute(task, rule, notes=notes)
            registry.add(str(getattr(task, "task_id", "<unknown>")), rule)

        unmatched = [
            rule.describe()
            for rule, count in zip(rule_list, matched_counts)
            if count == 0 and rule.required
        ]
        if unmatched:
            raise ValueError("Required task mock rule(s) matched no tasks: " + "; ".join(unmatched) + ".")

        yield registry
    finally:
        for task in tasks:
            original = originals.get(id(task), _NO_ORIGINAL)
            if original is _NO_ORIGINAL:
                continue
            if original is _MISSING:
                task.__dict__.pop("execute", None)
            else:
                setattr(task, "execute", original)


def _parse_mock_payload(raw: str, *, suffix: str) -> Any:
    if suffix in {".yaml", ".yml"}:
        try:
            import yaml
        except Exception as exc:
            raise ValueError("YAML mock files require PyYAML to be installed.") from exc
        return yaml.safe_load(raw)

    try:
        return json.loads(raw)
    except json.JSONDecodeError as json_exc:
        try:
            import yaml
        except Exception:
            raise ValueError(
                f"Invalid JSON: {json_exc.msg} at line {json_exc.lineno} column {json_exc.colno}."
            ) from json_exc
        return yaml.safe_load(raw)


def _task_mock_rule_from_mapping(index: int, item: Any) -> TaskMockRule:
    if not isinstance(item, Mapping):
        raise ValueError(f"mock rule #{index} must be an object.")

    mode = str(item.get("mode", "success")).strip().lower()
    if mode not in _SUPPORTED_MODES:
        supported = ", ".join(sorted(_SUPPORTED_MODES))
        raise ValueError(f"mock rule #{index} has unsupported mode {mode!r}; supported modes: {supported}.")

    xcom_payload = item.get("xcom", {})
    if xcom_payload is None:
        xcom_payload = {}
    if not isinstance(xcom_payload, Mapping):
        raise ValueError(f"mock rule #{index} xcom must be an object.")
    xcom = dict(xcom_payload)
    if "return_value" in item:
        xcom["return_value"] = item["return_value"]

    rule = TaskMockRule(
        task_id=_optional_str(item.get("task_id")),
        task_id_glob=_optional_str(item.get("task_id_glob", item.get("task_glob"))),
        operator=_optional_str(item.get("operator", item.get("operator_class"))),
        operator_glob=_optional_str(item.get("operator_glob")),
        mode=mode,
        xcom=xcom,
        name=_optional_str(item.get("name")),
        required=bool(item.get("required", True)),
    )
    if not any((rule.task_id, rule.task_id_glob, rule.operator, rule.operator_glob)):
        raise ValueError(f"mock rule #{index} must define task_id, task_id_glob, operator, or operator_glob.")
    return rule


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _operator_candidates(task: Any) -> set[str]:
    task_type = getattr(task, "task_type", None)
    cls = task.__class__
    return {
        candidate
        for candidate in {
            str(task_type) if task_type else None,
            cls.__name__,
            f"{cls.__module__}.{cls.__qualname__}",
        }
        if candidate
    }


def _patch_execute(task: Any, rule: TaskMockRule, *, notes: list[str] | None) -> None:
    def mocked_execute(bound_self: Any, *args: Any, **kwargs: Any) -> Any:
        context = _extract_context(args, kwargs)
        _push_xcom_values(context, rule, notes=notes)
        return rule.return_value

    task.execute = MethodType(mocked_execute, task)


def _extract_context(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Mapping[str, Any]:
    if args and isinstance(args[0], Mapping):
        return args[0]
    context = kwargs.get("context")
    return context if isinstance(context, Mapping) else {}


def _push_xcom_values(context: Mapping[str, Any], rule: TaskMockRule, *, notes: list[str] | None) -> None:
    if not rule.xcom:
        return
    ti = context.get("ti") or context.get("task_instance")
    push = getattr(ti, "xcom_push", None)
    if not callable(push):
        return

    for key, value in rule.xcom.items():
        try:
            push(key=str(key), value=value)
        except Exception as exc:
            if notes is not None:
                notes.append(f"Could not push mocked XCom key {key!r}: {exc}")
