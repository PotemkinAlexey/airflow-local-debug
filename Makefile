PYTHON ?= python

.PHONY: install-dev test build clean

install-dev:
	$(PYTHON) -m pip install -e .[dev]

test:
	$(PYTHON) -m pytest -q tests

build:
	$(PYTHON) -m build

clean:
	rm -rf build dist src/*.egg-info .pytest_cache .mypy_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
