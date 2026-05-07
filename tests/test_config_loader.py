from __future__ import annotations

from pathlib import Path

import pytest

from airflow_local_debug.config.loader import get_default_config_path, load_local_config


def test_get_default_config_path_prefers_explicit_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = tmp_path / "local_config.py"
    config_path.write_text("CONNECTIONS = {}\nVARIABLES = {}\nPOOLS = {}\n", encoding="utf-8")

    monkeypatch.setenv("AIRFLOW_DEBUG_LOCAL_CONFIG", str(config_path))
    monkeypatch.delenv("RUNBOOK_LOCAL_CONFIG", raising=False)

    assert get_default_config_path(required=True) == str(config_path.resolve())


def test_get_default_config_path_requires_explicit_source(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AIRFLOW_DEBUG_LOCAL_CONFIG", raising=False)
    monkeypatch.delenv("RUNBOOK_LOCAL_CONFIG", raising=False)

    with pytest.raises(FileNotFoundError):
        get_default_config_path(required=True)


def test_load_local_config_normalizes_connections(tmp_path: Path) -> None:
    config_path = tmp_path / "airflow_defaults.py"
    config_path.write_text(
        "\n".join(
            [
                "CONNECTIONS = {",
                "    'demo': {",
                "        'conn_type': 'http',",
                "        'host': 'example.com',",
                "        'extra__http__timeout': 30,",
                "        'extra': {'verify': False},",
                "    },",
                "}",
                "VARIABLES = {'ENV': 'local'}",
                "POOLS = {'default_pool': {'slots': 4}}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    config = load_local_config(str(config_path))

    assert config.source_path == str(config_path)
    assert config.connections["demo"]["conn_type"] == "http"
    assert config.connections["demo"]["extra"] == {"timeout": 30, "verify": False}
    assert config.variables["ENV"] == "local"
    assert config.pools["default_pool"]["slots"] == 4
