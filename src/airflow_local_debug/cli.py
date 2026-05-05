from __future__ import annotations

from airflow_local_debug.bootstrap import ensure_quiet_airflow_bootstrap


def main() -> None:
    ensure_quiet_airflow_bootstrap()

    from airflow_local_debug.runner import debug_dag_file_cli

    debug_dag_file_cli()


if __name__ == "__main__":
    main()
