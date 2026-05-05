from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from typing import Any, Iterator, Mapping

from airflow_local_debug.config_loader import get_default_config_path, load_local_config
from airflow_local_debug.models import LocalConfig

_NON_ENV_CHARS = re.compile(r"[^A-Z0-9_]")


def _env_key(prefix: str, raw_key: str) -> str:
    normalized = _NON_ENV_CHARS.sub("_", raw_key.upper())
    return f"{prefix}{normalized}"


def _serialize_variable(value: Any, *, key: str) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value)
    except TypeError as exc:
        raise TypeError(
            f"Variable {key!r} is not JSON-serializable ({type(value).__name__}). "
            "Convert it to a primitive type or pre-serialize it as a string."
        ) from exc


def _serialize_connection(value: Any, *, conn_id: str) -> str:
    if isinstance(value, str):
        return value
    if not isinstance(value, dict):
        raise TypeError(
            f"Connection {conn_id!r} has unsupported payload type {type(value).__name__}; "
            "expected dict or URI string."
        )

    payload = {key: item for key, item in value.items() if item not in (None, "", [], {})}
    try:
        return json.dumps(payload)
    except TypeError as exc:
        raise TypeError(
            f"Connection {conn_id!r} contains a non-JSON-serializable field. "
            f"Offending payload keys: {sorted(payload)!r}."
        ) from exc


@contextmanager
def bootstrap_airflow_env(
    *,
    config_path: str | None = None,
    config: LocalConfig | None = None,
    extra_env: Mapping[str, str] | None = None,
) -> Iterator[LocalConfig]:
    """
    Inject Connections and Variables into Airflow using standard environment variables.

    This keeps local execution aligned with normal Airflow lookup order:
    - Connections via AIRFLOW_CONN_<CONN_ID>
    - Variables via AIRFLOW_VAR_<KEY>
    """
    if config_path and config is not None:
        raise ValueError("Pass either config_path or config, not both.")

    if config is not None:
        local_config = config
    elif config_path is not None:
        local_config = load_local_config(config_path)
    else:
        default_path = get_default_config_path(required=False)
        local_config = load_local_config(default_path) if default_path else LocalConfig()
    updates: dict[str, str] = {}

    for conn_id, payload in local_config.connections.items():
        updates[_env_key("AIRFLOW_CONN_", conn_id)] = _serialize_connection(payload, conn_id=conn_id)

    for key, value in local_config.variables.items():
        updates[_env_key("AIRFLOW_VAR_", key)] = _serialize_variable(value, key=key)

    # Avoid example DAG noise in local runs.
    updates.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

    if extra_env:
        updates.update(dict(extra_env))

    previous: dict[str, str | None] = {key: os.environ.get(key) for key in updates}
    try:
        for key, value in updates.items():
            os.environ[key] = value
        yield local_config
    finally:
        for key, old_value in previous.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value
