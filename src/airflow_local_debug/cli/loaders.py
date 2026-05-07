"""
CLI value loaders: turn raw argparse strings into structured runner kwargs.

These helpers intentionally do not depend on argparse — they operate on the
already-parsed values so they can be unit-tested in isolation.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from airflow_local_debug.execution.mocks import TaskMockRule, load_task_mock_rules


def coerce_logical_date(value: str | date | datetime | None) -> datetime | None:
    """Normalise a CLI-provided logical date to a tz-aware UTC datetime."""

    def ensure_aware(value: datetime) -> datetime:
        if value.tzinfo is not None and value.utcoffset() is not None:
            return value
        return value.replace(tzinfo=timezone.utc)

    if value is None:
        return None
    if isinstance(value, datetime):
        return ensure_aware(value)
    if isinstance(value, date):
        return ensure_aware(datetime.combine(value, datetime.min.time()))
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return ensure_aware(datetime.fromisoformat(raw))
    except ValueError:
        if "T" not in raw and " " not in raw:
            return ensure_aware(datetime.fromisoformat(f"{raw}T00:00:00"))
        raise


def load_cli_conf(*, conf_json: str | None = None, conf_file: str | None = None) -> dict[str, Any] | None:
    """Parse `--conf-json` / `--conf-file` into a `dag_run.conf` dict (or None)."""
    if conf_json is not None and conf_file is not None:
        raise ValueError("Use either --conf-json or --conf-file, not both.")

    if conf_file is not None:
        path = Path(conf_file).expanduser()
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ValueError(f"Could not read --conf-file {path}: {exc}") from exc
    elif conf_json is not None:
        raw = conf_json
    else:
        return None

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid DAG run conf JSON: {exc.msg} at line {exc.lineno} column {exc.colno}."
        ) from exc

    if not isinstance(payload, dict):
        raise ValueError("DAG run conf must be a JSON object.")
    return payload


def load_cli_extra_env(values: list[str] | None) -> dict[str, str]:
    """Parse repeated `--env KEY=VALUE` arguments into a flat dict."""
    extra_env: dict[str, str] = {}
    for value in values or []:
        key, separator, raw = value.partition("=")
        key = key.strip()
        if not separator or not key:
            raise ValueError("--env values must use KEY=VALUE format.")
        extra_env[key] = raw
    return extra_env


def load_cli_task_mocks(values: list[str] | None) -> list[TaskMockRule]:
    """Read every `--mock-file` path and concatenate the rules."""
    rules: list[TaskMockRule] = []
    for value in values or []:
        rules.extend(load_task_mock_rules(value))
    return rules


def load_cli_env_files(
    values: list[str] | None,
    *,
    auto_discover: bool = True,
    notes: list[str] | None = None,
) -> dict[str, str]:
    """Load and merge values from `--env-file` paths and (optionally) auto-discovered `.env`.

    Later sources win over earlier ones. The auto-discovered `.env` is the
    lowest priority and is only used when no explicit `--env-file` is given.
    """
    from airflow_local_debug.config.dotenv import discover_dotenv_path, parse_dotenv_file

    layers: list[dict[str, str]] = []
    explicit_paths = list(values or [])

    if not explicit_paths and auto_discover:
        auto_path = discover_dotenv_path()
        if auto_path is not None:
            layers.append(parse_dotenv_file(auto_path))
            if notes is not None:
                notes.append(f"Loaded auto-discovered env file {auto_path}")

    for path in explicit_paths:
        layers.append(parse_dotenv_file(path))
        if notes is not None:
            notes.append(f"Loaded env file {path}")

    merged: dict[str, str] = {}
    for layer in layers:
        merged.update(layer)
    return merged


def load_cli_selector_values(values: Iterable[str] | None, *, option_name: str) -> list[str]:
    """Split repeated comma-or-multi-flag selectors into a flat list of task ids."""
    selectors: list[str] = []
    for value in values or []:
        parts = [part.strip() for part in str(value).split(",")]
        if any(not part for part in parts):
            raise ValueError(f"{option_name} values must not be blank.")
        selectors.extend(parts)
    return selectors
