from __future__ import annotations

import argparse
import json
import re
import sys
import traceback
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

from airflow_local_debug.bootstrap import ensure_quiet_airflow_bootstrap
from airflow_local_debug.config_loader import get_default_config_path, load_local_config
from airflow_local_debug.env_bootstrap import _serialize_connection, _serialize_variable

DoctorStatus = Literal["ok", "warn", "fail", "skip"]


@dataclass
class DoctorCheck:
    name: str
    status: DoctorStatus
    message: str
    details: list[str] = field(default_factory=list)


@dataclass
class DoctorResult:
    checks: list[DoctorCheck] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not any(check.status == "fail" for check in self.checks)

    @property
    def exit_code(self) -> int:
        return 0 if self.ok else 1


def _version_tuple(version: str | None) -> tuple[int, int, int] | None:
    if not version:
        return None
    match = re.match(r"^\D*(\d+)(?:\.(\d+))?(?:\.(\d+))?", version.strip())
    if not match:
        return None
    return tuple(int(part or 0) for part in match.groups())  # type: ignore[return-value]


def is_supported_airflow_version(version: str | None) -> bool:
    parsed = _version_tuple(version)
    if parsed is None:
        return False
    major, minor, _patch = parsed
    if major == 2:
        return minor >= 10
    if major == 3:
        return minor >= 1
    return False


def check_airflow_import() -> tuple[DoctorCheck, str | None, bool]:
    try:
        import airflow
    except Exception as exc:
        return (
            DoctorCheck(
                name="Airflow import",
                status="fail",
                message=f"Airflow import failed: {exc}",
                details=traceback.format_exception_only(type(exc), exc),
            ),
            None,
            False,
        )

    version = getattr(airflow, "__version__", None)
    if is_supported_airflow_version(version):
        return (
            DoctorCheck(
                name="Airflow version",
                status="ok",
                message=f"apache-airflow {version} is supported.",
            ),
            version,
            True,
        )

    return (
        DoctorCheck(
            name="Airflow version",
            status="fail",
            message=f"apache-airflow {version or '<unknown>'} is not supported.",
            details=["Supported range: apache-airflow>=2.10,!=3.0.*,<4"],
        ),
        version,
        True,
    )


def check_metadata_db(*, airflow_available: bool) -> DoctorCheck:
    if not airflow_available:
        return DoctorCheck(
            name="Metadata DB",
            status="skip",
            message="Skipped because Airflow could not be imported.",
        )

    try:
        from airflow import settings
        from airflow.models.dagrun import DagRun
        from sqlalchemy import select
    except Exception as exc:
        return DoctorCheck(
            name="Metadata DB",
            status="fail",
            message=f"Unable to import Airflow metadata DB helpers: {exc}",
        )

    session_factory = getattr(settings, "Session", None)
    if session_factory is None:
        return DoctorCheck(
            name="Metadata DB",
            status="fail",
            message="Airflow settings.Session is not configured.",
            details=["Call `airflow db migrate` or initialize Airflow before local debug runs."],
        )

    session = session_factory()
    try:
        session.execute(select(DagRun.run_id).limit(1)).first()
    except Exception as exc:
        return DoctorCheck(
            name="Metadata DB",
            status="fail",
            message=f"Metadata DB is not ready: {exc}",
            details=["Run `airflow db migrate` against the same AIRFLOW_HOME / sql_alchemy_conn."],
        )
    finally:
        close = getattr(session, "close", None)
        if callable(close):
            close()

    return DoctorCheck(
        name="Metadata DB",
        status="ok",
        message="Metadata DB is reachable and core tables are queryable.",
    )


def _resolve_config_path(config_path: str | None, *, require_config: bool) -> str | None:
    if config_path:
        resolved = Path(config_path).expanduser().resolve()
        if not resolved.exists():
            raise FileNotFoundError(f"Config file not found: {resolved}")
        return str(resolved)
    return get_default_config_path(required=require_config)


def check_local_config(config_path: str | None = None, *, require_config: bool = False) -> DoctorCheck:
    try:
        resolved_path = _resolve_config_path(config_path, require_config=require_config)
    except Exception as exc:
        return DoctorCheck(
            name="Local config",
            status="fail",
            message=str(exc),
        )

    if resolved_path is None:
        return DoctorCheck(
            name="Local config",
            status="warn",
            message="No local config file configured; current Airflow environment will be used.",
            details=["Set AIRFLOW_DEBUG_LOCAL_CONFIG or pass --config-path to validate local runtime data."],
        )

    try:
        config = load_local_config(resolved_path)
        for conn_id, payload in config.connections.items():
            _serialize_connection(payload, conn_id=conn_id)
        for key, value in config.variables.items():
            _serialize_variable(value, key=key)
        for pool_name, payload in config.pools.items():
            int(payload.get("slots", 1))
            include_deferred = payload.get("include_deferred")
            if include_deferred is not None and not isinstance(include_deferred, bool):
                raise TypeError(f"Pool {pool_name!r} include_deferred must be a bool when provided.")
    except Exception as exc:
        return DoctorCheck(
            name="Local config",
            status="fail",
            message=f"Config validation failed: {exc}",
            details=[f"path: {resolved_path}"],
        )

    return DoctorCheck(
        name="Local config",
        status="ok",
        message=(
            f"Loaded {len(config.connections)} connection(s), "
            f"{len(config.variables)} variable(s), {len(config.pools)} pool(s)."
        ),
        details=[f"path: {resolved_path}"],
    )


def _airflow_major(version: str | None) -> int | None:
    parsed = _version_tuple(version)
    return parsed[0] if parsed is not None else None


def _serialize_dag_for_airflow3(dag: Any) -> None:
    try:
        from airflow.serialization.serialized_objects import DagSerialization  # type: ignore[attr-defined]
    except ImportError:
        from airflow.serialization.serialized_objects import (  # type: ignore[attr-defined,assignment]
            SerializedDAG as DagSerialization,  # type: ignore[no-redef]
        )

    encoded = DagSerialization.serialize_dag(dag)
    DagSerialization.deserialize_dag(encoded)


def check_dag_file(
    dag_path: str | None = None,
    *,
    dag_id: str | None = None,
    airflow_version: str | None = None,
    airflow_available: bool = True,
) -> DoctorCheck:
    if not dag_path:
        return DoctorCheck(
            name="DAG file",
            status="skip",
            message="No DAG file provided; DAG import and serialization checks skipped.",
        )
    if not airflow_available:
        return DoctorCheck(
            name="DAG file",
            status="skip",
            message="Skipped because Airflow could not be imported.",
        )

    try:
        from airflow_local_debug.runner import _load_module_from_file, _resolve_dag_from_module

        module = _load_module_from_file(dag_path)
        dag = _resolve_dag_from_module(module, dag_id=dag_id)
    except Exception as exc:
        return DoctorCheck(
            name="DAG file",
            status="fail",
            message=f"DAG import failed: {exc}",
            details=[f"path: {Path(dag_path).expanduser()}"],
        )

    details = [
        f"dag_id: {getattr(dag, 'dag_id', '<unknown>')}",
        f"task_count: {len(getattr(dag, 'task_dict', {}) or {})}",
    ]

    if _airflow_major(airflow_version) != 3:
        return DoctorCheck(
            name="DAG file",
            status="ok",
            message="DAG imported successfully.",
            details=details,
        )

    try:
        _serialize_dag_for_airflow3(dag)
    except Exception as exc:
        return DoctorCheck(
            name="Airflow 3 DAG serialization",
            status="fail",
            message=f"DAG serialization failed: {exc}",
            details=details,
        )

    return DoctorCheck(
        name="Airflow 3 DAG serialization",
        status="ok",
        message="DAG imports and serializes successfully.",
        details=details,
    )


def run_doctor(
    *,
    config_path: str | None = None,
    require_config: bool = False,
    dag_path: str | None = None,
    dag_id: str | None = None,
) -> DoctorResult:
    checks: list[DoctorCheck] = []

    airflow_check, airflow_version, airflow_available = check_airflow_import()
    checks.append(airflow_check)
    checks.append(check_metadata_db(airflow_available=airflow_available))
    checks.append(check_local_config(config_path, require_config=require_config))
    checks.append(
        check_dag_file(
            dag_path,
            dag_id=dag_id,
            airflow_version=airflow_version,
            airflow_available=airflow_available,
        )
    )

    return DoctorResult(checks=checks)


def format_doctor_report(result: DoctorResult) -> str:
    labels = {
        "ok": "OK",
        "warn": "WARN",
        "fail": "FAIL",
        "skip": "SKIP",
    }
    lines = ["airflow-local-debug doctor"]
    for check in result.checks:
        lines.append(f"[{labels[check.status]}] {check.name}: {check.message}")
        for detail in check.details:
            lines.append(f"    {detail.rstrip()}")
    lines.append(f"Verdict: {'OK' if result.ok else 'FAIL'}")
    return "\n".join(lines)


def doctor_result_to_dict(result: DoctorResult) -> dict[str, object]:
    return {
        "ok": result.ok,
        "exit_code": result.exit_code,
        "checks": [asdict(check) for check in result.checks],
    }


def format_doctor_json(result: DoctorResult) -> str:
    return json.dumps(doctor_result_to_dict(result), indent=2, sort_keys=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate local airflow-local-debug prerequisites.")
    parser.add_argument("--config-path", help="Local Airflow config file to validate.")
    parser.add_argument(
        "--require-config",
        action="store_true",
        help="Fail when no local config file is configured.",
    )
    parser.add_argument("--dag-file", help="Optional DAG file to import and validate.")
    parser.add_argument("--dag-id", help="DAG ID to select when --dag-file contains multiple DAGs.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON instead of the text report.")
    return parser


def main(argv: list[str] | None = None) -> None:
    ensure_quiet_airflow_bootstrap()
    args = build_parser().parse_args(argv)
    result = run_doctor(
        config_path=args.config_path,
        require_config=args.require_config,
        dag_path=args.dag_file,
        dag_id=args.dag_id,
    )
    print(format_doctor_json(result) if args.json else format_doctor_report(result))
    sys.exit(result.exit_code)


if __name__ == "__main__":
    main()
