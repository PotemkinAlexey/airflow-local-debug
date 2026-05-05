"""
Minimal helpers that must be safe to import before Airflow itself.

This module is intentionally small so direct `python my_dag.py ...` entrypoints
can suppress import-time Airflow warnings before importing the rest of the
debug package.
"""

from __future__ import annotations

import os
import sys
import warnings
from contextlib import contextmanager
from typing import Iterator

_QUIET_READY_ENV = "AIRFLOW_DEBUG_BOOTSTRAP_QUIET_READY"


def silence_airflow_bootstrap_warnings() -> None:
    """
    Hide deprecation/future warnings during direct local DAG execution.

    Mutates global `warnings` state for the lifetime of the process. Intended
    for CLI use only (`if __name__ == "__main__":`). Library code should use
    `silenced_airflow_bootstrap_warnings()` instead, which restores prior state.
    """

    original_showwarning = warnings.showwarning

    def quiet_showwarning(message, category, filename, lineno, file=None, line=None):
        if issubclass(category, (DeprecationWarning, FutureWarning)):
            return
        return original_showwarning(message, category, filename, lineno, file=file, line=line)

    warnings.showwarning = quiet_showwarning
    warnings.simplefilter("ignore", DeprecationWarning)
    warnings.simplefilter("ignore", FutureWarning)


@contextmanager
def silenced_airflow_bootstrap_warnings() -> Iterator[None]:
    """
    Context-manager version of `silence_airflow_bootstrap_warnings`.

    Restores the previous `warnings.showwarning` and filter state on exit,
    so it is safe to call from library code without leaking global mutations.
    """

    original_showwarning = warnings.showwarning
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        warnings.simplefilter("ignore", FutureWarning)

        def quiet_showwarning(message, category, filename, lineno, file=None, line=None):
            if issubclass(category, (DeprecationWarning, FutureWarning)):
                return
            return original_showwarning(message, category, filename, lineno, file=file, line=line)

        warnings.showwarning = quiet_showwarning
        try:
            yield
        finally:
            warnings.showwarning = original_showwarning


def ensure_quiet_airflow_bootstrap() -> None:
    """
    Re-exec the current script once with warning suppression enabled from startup.

    This is the most reliable option for direct `python my_dag.py ...` local
    runs because some Airflow warnings are emitted during import/bootstrap.
    """

    if os.environ.get(_QUIET_READY_ENV) != "1":
        existing = os.environ.get("PYTHONWARNINGS", "").strip()
        additions = "ignore::DeprecationWarning,ignore::FutureWarning"
        if existing:
            os.environ["PYTHONWARNINGS"] = f"{additions},{existing}"
        else:
            os.environ["PYTHONWARNINGS"] = additions
        os.environ[_QUIET_READY_ENV] = "1"
        os.execv(sys.executable, [sys.executable, *sys.argv])

    silence_airflow_bootstrap_warnings()
