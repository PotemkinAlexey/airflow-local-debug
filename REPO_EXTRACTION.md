# Repository Extraction Checklist

This document describes how to move `airflow-local-debug` from the current
monorepo into a dedicated Git repository with minimal risk.

## 1. Create the new repository

Create a new empty repository, for example:

- `airflow-local-debug`

Do not copy the whole monorepo. Only move the standalone package contents.

## 2. Copy the package payload

Copy these files and directories into the new repository root:

- `README.md`
- `REPO_EXTRACTION.md`
- `.gitignore`
- `pyproject.toml`
- `src/airflow_local_debug/`
- `tests/`

The source of truth is currently:

- `packages/airflow-local-debug/README.md`
- `packages/airflow-local-debug/REPO_EXTRACTION.md`
- `packages/airflow-local-debug/.gitignore`
- `packages/airflow-local-debug/pyproject.toml`
- `packages/airflow-local-debug/src/airflow_local_debug/`
- `packages/airflow-local-debug/tests/`

## 3. Validate the new repository locally

From the new repository root:

```bash
python -m pip install -e .[dev]
python -m pytest -q tests
```

Expected result:

- editable install succeeds
- package imports as `airflow_local_debug`
- tests pass

## 4. Decide packaging policy

Before publishing or sharing internally, decide these points explicitly:

- supported Airflow range
  Current package metadata is `apache-airflow>=2.10,<4`
- license
  Current placeholder is `LicenseRef-Proprietary`
- versioning policy
  Recommended: SemVer starting at `0.1.0`
- publishing model
  Internal Git dependency, private package index, or direct editable install

## 5. Add CI in the new repository

Minimum CI should run:

```bash
python -m pip install -e .[dev]
python -m pytest -q tests
```

Optional next steps:

- Ruff or flake8
- mypy
- build wheel/sdist

## 6. Switch the DAG repository to the external package

After the new repository exists and passes tests, update this DAG repository.

### 6.1 Remove the local bridge

Remove:

- `airflow_local_debug` symlink at repo root

Current local bridge path:

- `/Users/alexpotemkin/IdeaProjects/airflow_dags/airflow_local_debug`

### 6.2 Add the dependency

Choose one of these models:

#### Option A: editable install in local dev

```bash
pip install -e /path/to/airflow-local-debug
```

#### Option B: Git dependency

Use a pinned commit or tag from the new repository.

#### Option C: private package index

Publish `airflow-local-debug` and install it as a normal dependency.

## 7. Remove compatibility wrappers from the DAG repository

After all consumers import the external package successfully, delete:

- `dags/common/airflow_debug/`
- `dags/common/airflow_debug_bootstrap.py`
- `dags/common/airflow_debug_cli.py`

These are currently compatibility layers only.

## 8. Update DAG imports

Ensure DAGs use:

```python
from airflow_local_debug import debug_dag_cli
from airflow_local_debug.bootstrap import ensure_quiet_airflow_bootstrap
```

Avoid old imports from:

- `dags.common.airflow_debug`
- `dags.common.airflow_debug_bootstrap`

## 9. Re-run real smoke checks in the DAG repository

At minimum re-run:

- direct DAG file run
- file-based CLI run
- mapped task tracing
- fail-fast behavior
- plugin behavior

Suggested pilot:

- `dags/azure_dags/logiwa/logiwa.py`

## 10. Final cleanup in the DAG repository

After the external dependency is stable:

- remove package duplication
- remove compatibility wrappers
- update documentation to reference the new repository

## Recommended order

1. Create new repository
2. Copy package payload
3. Run tests there
4. Add CI there
5. Connect DAG repo to new dependency
6. Re-run DAG smoke tests
7. Remove wrappers from DAG repo

## Current package root

The current extraction source is:

- `/Users/alexpotemkin/IdeaProjects/airflow_dags/packages/airflow-local-debug`
