# Changelog

All notable changes to `airflow-local-debug` are documented here.
The format is loosely based on Keep a Changelog and the project follows SemVer.

## [Unreleased]

No changes yet.

## [0.2.0] - 2026-05-07

### Compatibility
- Airflow range pinned to `>=2.10,!=3.0.*,<4`. Validated on Airflow 2.10+,
  3.1.3, 3.2.1. Airflow 3.0.x is excluded because its local execution API
  returns HTTP 422 even for `EmptyOperator` on 3.0.6, so `dag.test()`-style
  runs cannot complete. This is an upstream limitation, not a bug in this
  package; the exclusion stays until / unless Airflow 3.0 receives a fix.

### Added
- Documentation set under `docs/` covering CLI usage, library APIs, local
  config, report artifacts, plugins, and Airflow compatibility.
- `airflow-debug-doctor` command for validating Airflow import/version support,
  metadata DB readiness, local config shape, DAG import, and Airflow 3
  serialization.
- `airflow-debug-doctor --json`, plus `doctor_result_to_dict()` and
  `format_doctor_json()` for CI/script consumption.
- File runner DAG discovery via `airflow-debug-run <file> --list-dags`,
  `list_dags_from_file()`, `format_dag_list()`, and `DagFileInfo`.
- CLI DAG run conf support via `--conf-json` and `--conf-file`.
- CLI one-off environment overrides via repeatable `--env KEY=VALUE`.
- Report artifact support via `--report-dir` / `report_dir=...`, including
  `report.md`, `result.json`, `tasks.csv`, `junit.xml`, `graph.svg`,
  `graph.txt`, and `exception.txt`.
- Explicit `--graph-svg-path` / `graph_svg_path=...` for writing DAG graph SVGs.
- `write_run_artifacts()` helper for library callers.
- `TaskRunInfo.duration_seconds`, task state summaries, and task durations in
  the final run report.
- `silenced_airflow_bootstrap_warnings()` context manager for library callers
  that need to suppress import-time Airflow warnings without leaking global state.
- `RepeatedProblemWarningError` exported from the package root so callers can
  catch warning-loop aborts produced by `ProblemLogPlugin`.
- `LocalConfig`, `RunResult`, `TaskRunInfo`, and `DagFileInfo` exported from
  the package root.
- `--include-graph-in-report` flag for both `debug_dag_cli` and
  `debug_dag_file_cli`.
- Shared `airflow_local_debug.topology` module used by the runner and graph
  renderer (replaces the duplicated topological-sort implementations).
- `py.typed` marker so downstream type checkers pick up the package's
  annotations.
- LICENSE file (Proprietary).
- Safety cap on the strict `dag.test` loop to prevent infinite-loop hangs when
  the dagrun state machine fails to advance.
- ASCII (`format_dag_graph`) and SVG (`render_dag_svg`) renderers gain explicit
  task-count limits (`ASCII_MAX_TASKS = 500`, `SVG_MAX_TASKS = 200`); larger
  DAGs return a placeholder pointing to the alternative renderer.
- Tests: 103 tests covering `topology`, `models`, `traceback_utils`, `plugins`,
  `live_trace`, `env_bootstrap`, `runner`, `report`, `doctor`, and `compat`.

### Changed
- `RunResult.state` is now normalized at construction time (lowercase, no
  `DagRunState.` prefix) so consumers see a stable string regardless of
  the underlying Airflow / Python combination.
- `RunResult.ok` requires `state == "success"` and no exception (previously
  returned True for `state in {None, "running", ...}` when no exception was set).
- `_build_plugin_manager` no longer silently duplicates default plugins
  (`TaskContextPlugin`, `ProblemLogPlugin`, `ConsoleTracePlugin`) when the
  caller passes one of those types via `plugins=[...]`.
- `live_task_trace._wrap_task` is idempotent; repeated wraps caused by
  `render_template_fields` no longer overwrite the original method handles.
- `live_task_trace.begin_task` now wraps tasks added to the DAG after
  `__enter__` (e.g. mapped clones, dynamically-extended TaskGroups).
- `_classify_problem` now classifies HTTP 504 and 524 as `timeout`.
- `_classify_problem` documented in code: priority order is
  `timeout > rate-limit > network > auth > json > io > perm > generic-http
  > airflow > key > value > unknown`.
- `_normalize_result` consistently uses `_state_token` so enum-repr states
  like `"DagRunState.FAILED"` are recognized.
- `silence_airflow_bootstrap_warnings()` documented as deprecated in favor
  of `silenced_airflow_bootstrap_warnings()` for library use.
- `bootstrap_airflow_env` raises descriptive `TypeError` (with the offending
  variable / connection name) when a payload is not JSON-serializable.
- `compat.build_dag_test_kwargs` falls back to `logical_date` (not
  `execution_date`) when the signature is unreadable, matching Airflow 3.
- `cli.main` exits with `sys.exit(0 if result.ok else 1)` for a deterministic
  CLI exit code.
- `RunResult.graph_svg_path` is now populated when graph SVG generation is
  requested, so report artifacts and JSON snapshots can point to the SVG.

### Fixed
- `_strict_dag_test` no longer hangs forever when the dagrun state machine
  stalls; loop is bounded at `max(50, len(tasks) * 10)` iterations.
- `runner.py` re-raises `SystemExit(0)` cleanly without marking the run
  as a failure.
- `run_full_dag_from_file` no longer applies `extra_env` twice via nested
  `bootstrap_airflow_env` calls.
- `_best_effort_last_dagrun` logs swallowed exceptions at DEBUG level instead
  of silently returning `None`.
- Auth classification keywords narrowed to avoid false positives on stray
  `token` / `login` / `identity` substrings.
- `live_task_trace` accepts only one of `plugins` / `plugin_manager` and
  raises `ValueError` when both are passed.
- `config_loader` uses an explicit `raise ValueError` instead of `assert`
  (previously silently bypassed under `python -O`).

### Tooling
- Migrated to `src/`-layout (`src/airflow_local_debug/`).
- CI matrix expanded across Python 3.10, 3.11, 3.12 and Airflow 2.10.5,
  2.11.2, 3.1.3, 3.2.1.
- CI smoke script validates doctor, strict success runs, strict fail-fast
  failure normalization, and package build behavior.
- CI installs `build` explicitly before `python -m build`.
- `MANIFEST.in` includes `docs/*.md` in source distributions.
- `make check` runs tests plus build; `make smoke` runs the Airflow runtime
  smoke script in a temporary `AIRFLOW_HOME`.
- `Makefile clean` also removes root-level `*.egg-info`.
- README expanded with documentation navigation, plugins, `RunResult`,
  fail-fast mode, and graph renderers.

## [0.1.0] - 2026-04-30

Initial extraction from the airflow_dags monorepo.
