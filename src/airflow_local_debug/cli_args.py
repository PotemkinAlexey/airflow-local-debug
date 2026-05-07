"""
Argparse builders shared by `debug_dag_cli` and `debug_dag_file_cli`.

Both CLI entrypoints share ~18 flags. Defining them once here prevents
the two parsers from drifting (which is what already happened: `--env-file`
and the partial-run selectors used to live in only one CLI).
"""

from __future__ import annotations

import argparse


def add_common_run_args(parser: argparse.ArgumentParser) -> None:
    """Add the flags shared by both CLI entrypoints, in stable --help order."""
    parser.add_argument(
        "--config-path",
        dest="config_path",
        help="Path to local Airflow debug config (connections, variables, pools).",
    )
    parser.add_argument(
        "--logical-date",
        dest="logical_date",
        help="Logical date / execution date for the local DAG run.",
    )
    parser.add_argument(
        "--conf-json",
        dest="conf_json",
        help="JSON object to pass as dag_run.conf.",
    )
    parser.add_argument(
        "--conf-file",
        dest="conf_file",
        help="Path to a JSON object file to pass as dag_run.conf.",
    )
    parser.add_argument(
        "--env",
        dest="env",
        action="append",
        metavar="KEY=VALUE",
        help="Extra environment variable for this local run; may be passed multiple times.",
    )
    parser.add_argument(
        "--env-file",
        dest="env_file",
        action="append",
        help=(
            "Load environment variables from a .env file. May be passed multiple times. "
            "When omitted, a .env file in the current directory is auto-loaded if present."
        ),
    )
    parser.add_argument(
        "--no-auto-env",
        dest="no_auto_env",
        action="store_true",
        help="Disable auto-discovery of a .env file in the current directory.",
    )
    parser.add_argument(
        "--mock-file",
        dest="mock_file",
        action="append",
        help="JSON/YAML task mock file. May be passed multiple times.",
    )
    parser.add_argument(
        "--task",
        dest="task_ids",
        action="append",
        help="Run only this task id. May be passed multiple times or comma-separated.",
    )
    parser.add_argument(
        "--start-task",
        dest="start_task_ids",
        action="append",
        help="Run this task id and all downstream tasks. May be passed multiple times or comma-separated.",
    )
    parser.add_argument(
        "--task-group",
        dest="task_group_ids",
        action="append",
        help="Run tasks inside this TaskGroup id. May be passed multiple times or comma-separated.",
    )
    parser.add_argument(
        "--dump-xcom",
        action="store_true",
        help="Collect final XComs into result.json and xcom.json artifacts.",
    )
    parser.add_argument(
        "--xcom-json-path",
        dest="xcom_json_path",
        help="Write final XCom snapshot to an explicit JSON path.",
    )
    parser.add_argument(
        "--no-trace",
        action="store_true",
        help="Disable live per-task console tracing.",
    )
    parser.add_argument(
        "--no-fail-fast",
        action="store_true",
        help="Keep original task retry settings instead of forcing fail-fast local debug mode.",
    )
    parser.add_argument(
        "--include-graph-in-report",
        action="store_true",
        help="Include the rendered DAG graph in the final report (useful for sharing).",
    )
    parser.add_argument(
        "--report-dir",
        dest="report_dir",
        help=(
            "Directory to write run artifacts: "
            "report.md, result.json, tasks.csv, junit.xml, graph.svg, graph.txt, exception.txt."
        ),
    )
    parser.add_argument(
        "--graph-svg-path",
        dest="graph_svg_path",
        help="Path to write the rendered DAG graph SVG (defaults to graph.svg inside --report-dir).",
    )


def add_watch_args(parser: argparse.ArgumentParser) -> None:
    """Add --watch / --watch-path / --watch-interval to a file-based CLI parser."""
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Re-run the DAG when watched files change. On task failure, retry from the failed task.",
    )
    parser.add_argument(
        "--watch-path",
        dest="watch_path",
        action="append",
        help="Extra file or directory to watch in --watch mode. May be passed multiple times.",
    )
    parser.add_argument(
        "--watch-interval",
        dest="watch_interval",
        type=float,
        default=0.5,
        help="Polling interval in seconds for --watch mode (default: 0.5).",
    )
