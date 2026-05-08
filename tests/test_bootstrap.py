from __future__ import annotations

import warnings
from typing import Any

import pytest

from airflow_local_debug.config import bootstrap


def test_silenced_airflow_bootstrap_warnings_restores_showwarning() -> None:
    original_showwarning = warnings.showwarning

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with bootstrap.silenced_airflow_bootstrap_warnings():
            assert warnings.showwarning is not original_showwarning
            warnings.warn("hidden", DeprecationWarning, stacklevel=1)
            warnings.warn("visible", UserWarning, stacklevel=1)
        assert warnings.showwarning is original_showwarning

    assert [str(item.message) for item in caught] == ["visible"]


def test_silence_airflow_bootstrap_warnings_filters_deprecation_and_future() -> None:
    original_showwarning = warnings.showwarning
    original_filters = list(warnings.filters)
    calls: list[tuple[str, type[Warning]]] = []

    def fake_showwarning(
        message: Warning | str,
        category: type[Warning],
        filename: str,
        lineno: int,
        file: Any = None,
        line: str | None = None,
    ) -> None:
        calls.append((str(message), category))

    try:
        warnings.showwarning = fake_showwarning
        bootstrap.silence_airflow_bootstrap_warnings()

        warnings.showwarning(DeprecationWarning("deprecated"), DeprecationWarning, "demo.py", 1)
        warnings.showwarning(FutureWarning("future"), FutureWarning, "demo.py", 1)
        warnings.showwarning(UserWarning("visible"), UserWarning, "demo.py", 1)

        assert calls == [("visible", UserWarning)]
    finally:
        warnings.showwarning = original_showwarning
        warnings.filters[:] = original_filters


def test_ensure_quiet_airflow_bootstrap_reexecs_with_warning_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, list[str]]] = []

    def fake_execv(executable: str, argv: list[str]) -> None:
        calls.append((executable, argv))
        raise RuntimeError("stop before exec")

    monkeypatch.delenv(bootstrap._QUIET_READY_ENV, raising=False)
    monkeypatch.delenv("PYTHONWARNINGS", raising=False)
    monkeypatch.setattr(bootstrap.sys, "executable", "/tmp/python")
    monkeypatch.setattr(bootstrap.sys, "argv", ["demo.py", "--flag"])
    monkeypatch.setattr(bootstrap.os, "execv", fake_execv)

    with pytest.raises(RuntimeError, match="stop before exec"):
        bootstrap.ensure_quiet_airflow_bootstrap()

    assert calls == [("/tmp/python", ["/tmp/python", "demo.py", "--flag"])]
    assert bootstrap.os.environ[bootstrap._QUIET_READY_ENV] == "1"
    assert bootstrap.os.environ["PYTHONWARNINGS"] == "ignore::DeprecationWarning,ignore::FutureWarning"


def test_ensure_quiet_airflow_bootstrap_preserves_existing_pythonwarnings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_execv(executable: str, argv: list[str]) -> None:
        raise RuntimeError("stop before exec")

    monkeypatch.delenv(bootstrap._QUIET_READY_ENV, raising=False)
    monkeypatch.setenv("PYTHONWARNINGS", "default::UserWarning")
    monkeypatch.setattr(bootstrap.os, "execv", fake_execv)

    with pytest.raises(RuntimeError, match="stop before exec"):
        bootstrap.ensure_quiet_airflow_bootstrap()

    assert bootstrap.os.environ["PYTHONWARNINGS"] == (
        "ignore::DeprecationWarning,ignore::FutureWarning,default::UserWarning"
    )


def test_ensure_quiet_airflow_bootstrap_silences_in_ready_process(monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[bool] = []

    monkeypatch.setenv(bootstrap._QUIET_READY_ENV, "1")
    monkeypatch.setattr(bootstrap, "silence_airflow_bootstrap_warnings", lambda: called.append(True))
    monkeypatch.setattr(bootstrap.os, "execv", lambda *args: pytest.fail("should not exec"))

    bootstrap.ensure_quiet_airflow_bootstrap()

    assert called == [True]
