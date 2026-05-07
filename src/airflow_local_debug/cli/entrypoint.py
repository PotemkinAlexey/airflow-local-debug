from __future__ import annotations

import sys

from airflow_local_debug.config.bootstrap import ensure_quiet_airflow_bootstrap


def main() -> None:
    ensure_quiet_airflow_bootstrap()

    from airflow_local_debug.runner import debug_dag_file_cli

    # debug_dag_file_cli forwards raise_on_failure=True by default, which
    # propagates SystemExit(1) on failure. We catch SystemExit explicitly
    # so we can also enforce a deterministic exit code on success.
    try:
        result = debug_dag_file_cli()
    except SystemExit:
        raise
    sys.exit(0 if result.ok else 1)


if __name__ == "__main__":
    main()
