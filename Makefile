PYTHON ?= python
COVERAGE_MIN ?= 40

.PHONY: install-dev lint typecheck test coverage build check smoke clean

install-dev:
	$(PYTHON) -m pip install -e .[dev]

lint:
	$(PYTHON) -m ruff check src tests

typecheck:
	$(PYTHON) -m mypy

test:
	$(PYTHON) -m pytest -q tests

coverage:
	$(PYTHON) -m pytest tests \
		--cov=airflow_local_debug \
		--cov-report=term-missing \
		--cov-report=xml \
		--cov-fail-under=$(COVERAGE_MIN)

build:
	$(PYTHON) -m build

check: lint typecheck test build

smoke:
	tmp_dir=$$(mktemp -d); \
	trap 'rm -rf "$$tmp_dir"' EXIT; \
	AIRFLOW_HOME=$$tmp_dir AIRFLOW__CORE__LOAD_EXAMPLES=False $(PYTHON) -m airflow db migrate; \
	AIRFLOW_HOME=$$tmp_dir AIRFLOW__CORE__LOAD_EXAMPLES=False $(PYTHON) tests/smoke_airflow_runtime.py

clean:
	rm -rf build dist *.egg-info src/*.egg-info .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
