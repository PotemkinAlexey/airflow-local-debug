from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from airflow_local_debug import _module_loading


def _module_name(path: Path, prefix: str) -> str:
    resolved = str(path.expanduser().resolve())
    return f"{prefix}_{hashlib.md5(resolved.encode()).hexdigest()}"


def test_load_python_module_imports_under_stable_path_hash(tmp_path: Path) -> None:
    module_path = tmp_path / "demo_module.py"
    module_path.write_text('VALUE = "fresh"\n', encoding="utf-8")
    module_name = _module_name(module_path, "demo")
    stale_module = ModuleType(module_name)
    stale_module.VALUE = "stale"
    sys.modules[module_name] = stale_module

    try:
        module = _module_loading.load_python_module(
            str(module_path),
            module_prefix="demo",
            missing_message="Missing: {path}",
            import_error_message="Bad import: {path}",
        )

        assert module.__name__ == module_name
        assert module.VALUE == "fresh"
        assert sys.modules[module_name] is module
    finally:
        sys.modules.pop(module_name, None)


def test_load_python_module_formats_missing_file_message(tmp_path: Path) -> None:
    module_path = tmp_path / "missing.py"

    with pytest.raises(FileNotFoundError, match=f"Missing: {module_path}"):
        _module_loading.load_python_module(
            str(module_path),
            module_prefix="demo",
            missing_message="Missing: {path}",
            import_error_message="Bad import: {path}",
        )


def test_load_python_module_formats_import_error_when_spec_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module_path = tmp_path / "demo_module.py"
    module_path.write_text("VALUE = 1\n", encoding="utf-8")

    monkeypatch.setattr(_module_loading.importlib.util, "spec_from_file_location", lambda *args: None)

    with pytest.raises(ImportError, match=f"Bad import: {module_path}"):
        _module_loading.load_python_module(
            str(module_path),
            module_prefix="demo",
            missing_message="Missing: {path}",
            import_error_message="Bad import: {path}",
        )


def test_load_python_module_formats_import_error_when_loader_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module_path = tmp_path / "demo_module.py"
    module_path.write_text("VALUE = 1\n", encoding="utf-8")

    monkeypatch.setattr(
        _module_loading.importlib.util,
        "spec_from_file_location",
        lambda *args: SimpleNamespace(loader=None),
    )

    with pytest.raises(ImportError, match=f"Bad import: {module_path}"):
        _module_loading.load_python_module(
            str(module_path),
            module_prefix="demo",
            missing_message="Missing: {path}",
            import_error_message="Bad import: {path}",
        )


def test_load_python_module_removes_failed_import_from_module_cache(tmp_path: Path) -> None:
    module_path = tmp_path / "bad_module.py"
    module_path.write_text('raise RuntimeError("boom")\n', encoding="utf-8")
    module_name = _module_name(module_path, "demo")

    with pytest.raises(RuntimeError, match="boom"):
        _module_loading.load_python_module(
            str(module_path),
            module_prefix="demo",
            missing_message="Missing: {path}",
            import_error_message="Bad import: {path}",
        )

    assert module_name not in sys.modules
