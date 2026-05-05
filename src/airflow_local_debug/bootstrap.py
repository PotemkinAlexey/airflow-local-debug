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

_QUIET_READY_ENV = "AIRFLOW_DEBUG_BOOTSTRAP_QUIET_READY"


def silence_airflow_bootstrap_warnings() -> None:
    """
    Hide deprecation/future warnings during direct local DAG execution.

    Intended for:

        if __name__ == "__main__":
            from airflow_local_debug.bootstrap import silence_airflow_bootstrap_warnings
            silence_airflow_bootstrap_warnings()
    """

    original_showwarning = warnings.showwarning

    def quiet_showwarning(message, category, filename, lineno, file=None, line=None):
        if issubclass(category, (DeprecationWarning, FutureWarning)):
            return
        return original_showwarning(message, category, filename, lineno, file=file, line=line)

    warnings.showwarning = quiet_showwarning
    warnings.simplefilter("ignore", DeprecationWarning)
    warnings.simplefilter("ignore", FutureWarning)


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
