from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def load_python_module(
    path: str,
    *,
    module_prefix: str,
    missing_message: str,
    import_error_message: str,
) -> ModuleType:
    """Import a Python file under a stable path-derived module name."""
    resolved = str(Path(path).expanduser().resolve())
    if not Path(resolved).exists():
        raise FileNotFoundError(missing_message.format(path=resolved))

    module_name = f"{module_prefix}_{hashlib.md5(resolved.encode()).hexdigest()}"
    sys.modules.pop(module_name, None)

    spec = importlib.util.spec_from_file_location(module_name, resolved)
    if spec is None or spec.loader is None:
        raise ImportError(import_error_message.format(path=resolved))

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module
