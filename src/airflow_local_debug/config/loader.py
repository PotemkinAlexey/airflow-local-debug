from __future__ import annotations

import json
import os
from collections.abc import Iterable
from pathlib import Path
from types import ModuleType
from typing import Any

from airflow_local_debug._module_loading import load_python_module
from airflow_local_debug.models import LocalConfig

_CONFIG_ENV_VARS = ("AIRFLOW_DEBUG_LOCAL_CONFIG", "RUNBOOK_LOCAL_CONFIG")


def get_default_config_path(*, required: bool = False) -> str | None:
    """
    Resolve the local config file path used for full local DAG runs.

    Priority:
    1. AIRFLOW_DEBUG_LOCAL_CONFIG
    2. RUNBOOK_LOCAL_CONFIG (legacy compatibility)
    """
    for env_name in _CONFIG_ENV_VARS:
        raw = os.getenv(env_name)
        if not raw:
            continue
        resolved = Path(raw).expanduser().resolve()
        if not resolved.exists():
            raise FileNotFoundError(f"{env_name} points to a missing file: {resolved}")
        return str(resolved)

    if required:
        raise FileNotFoundError(
            "Local Airflow config file not found. Set AIRFLOW_DEBUG_LOCAL_CONFIG "
            "or RUNBOOK_LOCAL_CONFIG, or pass an explicit config_path."
        )
    return None


def _load_module_from_path(filepath: str) -> ModuleType:
    return load_python_module(
        filepath,
        module_prefix="airflow_debug_config",
        missing_message="Config file not found: {path}",
        import_error_message="Unable to load module spec for {path}",
    )


def _normalize_connection_dict(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize the legacy runbook connection format into an Airflow-friendly payload.

    Supports:
    - flat extra__namespace__key entries
    - an "extra" field provided as dict or JSON string
    """
    normalized: dict[str, Any] = {}
    extras: dict[str, Any] = {}

    for key, value in raw.items():
        if key.startswith("extra__"):
            parts = key.split("__", 2)
            if len(parts) == 3:
                extras[parts[2]] = value

    if "extra" in raw:
        value = raw["extra"]
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                parsed = value
        else:
            parsed = value
        if isinstance(parsed, dict):
            extras.update(parsed)
        elif parsed not in (None, "", {}):
            normalized["extra"] = parsed

    for key, value in raw.items():
        if key.startswith("extra__") or key == "conn_id":
            continue
        if key == "extra":
            continue
        normalized[key] = value

    if extras:
        normalized["extra"] = extras
    return normalized


def _iter_connection_items(raw_connections: Any) -> Iterable[tuple[str, Any]]:
    if raw_connections is None:
        return []
    if isinstance(raw_connections, dict):
        return list(raw_connections.items())
    if isinstance(raw_connections, list):
        items: list[tuple[str, Any]] = []
        for item in raw_connections:
            if not isinstance(item, dict):
                raise TypeError("Each CONNECTIONS item must be a dict")
            conn_id = item.get("conn_id")
            if not conn_id:
                raise ValueError("Connection entry missing 'conn_id'")
            items.append((str(conn_id), item))
        return items
    raise TypeError("CONNECTIONS must be either a dict or a list of dicts")


def _iter_variable_items(raw_variables: Any) -> Iterable[tuple[str, Any]]:
    if raw_variables is None:
        return []
    if isinstance(raw_variables, dict):
        return list(raw_variables.items())
    if isinstance(raw_variables, list):
        items: list[tuple[str, Any]] = []
        for item in raw_variables:
            if not isinstance(item, dict):
                raise TypeError("Each VARIABLES item must be a dict")
            key = item.get("key")
            if key is None:
                raise ValueError("Variable entry missing 'key'")
            items.append((str(key), item.get("val")))
        return items
    raise TypeError("VARIABLES must be either a dict or a list of dicts")


def _iter_pool_items(raw_pools: Any) -> Iterable[tuple[str, dict[str, Any]]]:
    if raw_pools is None:
        return []
    if isinstance(raw_pools, dict):
        items: list[tuple[str, dict[str, Any]]] = []
        for pool_name, pool_value in raw_pools.items():
            if isinstance(pool_value, dict):
                items.append((str(pool_name), dict(pool_value)))
            else:
                items.append((str(pool_name), {"slots": pool_value}))
        return items
    if isinstance(raw_pools, list):
        items = []
        for item in raw_pools:
            if not isinstance(item, dict):
                raise TypeError("Each POOLS item must be a dict")
            pool_name = item.get("pool")
            if pool_name is None:
                raise ValueError("Pool entry missing 'pool'")
            items.append((str(pool_name), dict(item)))
        return items
    raise TypeError("POOLS must be either a dict or a list of dicts")


def load_local_config(filepath: str | None = None) -> LocalConfig:
    """
    Load local Airflow runtime data from a Python config module.

    Supported module globals:
    - CONNECTIONS
    - VARIABLES
    - POOLS
    """
    resolved_path = filepath or get_default_config_path(required=True)
    if resolved_path is None:
        raise ValueError("No config file path available.")
    module = _load_module_from_path(resolved_path)

    config = LocalConfig(source_path=resolved_path)

    for conn_id, raw_value in _iter_connection_items(getattr(module, "CONNECTIONS", None)):
        if isinstance(raw_value, str):
            config.connections[conn_id] = raw_value
        elif isinstance(raw_value, dict):
            config.connections[conn_id] = _normalize_connection_dict(raw_value)
        else:
            raise TypeError(f"Unsupported connection payload for '{conn_id}': {type(raw_value).__name__}")

    for key, value in _iter_variable_items(getattr(module, "VARIABLES", None)):
        config.variables[key] = value

    for pool_name, pool_payload in _iter_pool_items(getattr(module, "POOLS", None)):
        payload = dict(pool_payload)
        payload.setdefault("pool", pool_name)
        config.pools[pool_name] = payload

    return config
