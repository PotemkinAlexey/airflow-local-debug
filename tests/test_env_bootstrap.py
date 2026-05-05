from __future__ import annotations

import json
import os

import pytest

from airflow_local_debug.env_bootstrap import bootstrap_airflow_env
from airflow_local_debug.models import LocalConfig


def test_bootstrap_airflow_env_sets_and_restores_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AIRFLOW_CONN_DEMO", raising=False)
    monkeypatch.delenv("AIRFLOW_VAR_ENV", raising=False)
    monkeypatch.delenv("AIRFLOW__CORE__LOAD_EXAMPLES", raising=False)

    config = LocalConfig(
        connections={"demo": {"conn_type": "http", "host": "example.com"}},
        variables={"ENV": "local"},
    )

    with bootstrap_airflow_env(config=config) as bootstrapped:
        assert bootstrapped is config
        assert os.environ["AIRFLOW_VAR_ENV"] == "local"
        payload = json.loads(os.environ["AIRFLOW_CONN_DEMO"])
        assert payload["conn_type"] == "http"
        assert payload["host"] == "example.com"
        assert os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] == "False"

    assert "AIRFLOW_CONN_DEMO" not in os.environ
    assert "AIRFLOW_VAR_ENV" not in os.environ
    assert "AIRFLOW__CORE__LOAD_EXAMPLES" not in os.environ


def test_bootstrap_airflow_env_restores_preexisting_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_ENV", "preexisting")

    config = LocalConfig(variables={"ENV": "local"})

    with bootstrap_airflow_env(config=config):
        assert os.environ["AIRFLOW_VAR_ENV"] == "local"

    assert os.environ["AIRFLOW_VAR_ENV"] == "preexisting"


def test_bootstrap_airflow_env_serializes_non_string_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AIRFLOW_VAR_FLAG", raising=False)

    config = LocalConfig(variables={"FLAG": {"enabled": True, "limit": 10}})

    with bootstrap_airflow_env(config=config):
        assert json.loads(os.environ["AIRFLOW_VAR_FLAG"]) == {"enabled": True, "limit": 10}


def test_bootstrap_airflow_env_extra_env_overrides_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AIRFLOW__CORE__LOAD_EXAMPLES", raising=False)

    with bootstrap_airflow_env(config=LocalConfig(), extra_env={"AIRFLOW__CORE__LOAD_EXAMPLES": "True"}):
        assert os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] == "True"


def test_bootstrap_airflow_env_rejects_both_config_and_path() -> None:
    with pytest.raises(ValueError):
        with bootstrap_airflow_env(config_path="/tmp/x", config=LocalConfig()):
            pass
