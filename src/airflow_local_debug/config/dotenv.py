"""
Minimal .env file parser.

Supports the common subset used by twelve-factor style configs:

- `KEY=VALUE` lines (whitespace around `=` is allowed)
- single- or double-quoted values, with backslash escapes inside double quotes
- `#` comments at the start of a line or after an unquoted value
- empty lines
- optional `export ` prefix (compatible with shell-sourced `.env`)

Keys must match `[A-Za-z_][A-Za-z0-9_]*`. Values may be empty. Quoted values
preserve embedded whitespace; unquoted values are stripped.

Intentionally tiny so it can ship without a `python-dotenv` dependency.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path

_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_dotenv_text(text: str, *, source: str = "<dotenv>") -> dict[str, str]:
    """Parse `.env`-style text into a dict. Last assignment for a key wins."""
    values: dict[str, str] = {}
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export ") or line.startswith("export\t"):
            line = line[len("export") :].lstrip()

        eq = line.find("=")
        if eq <= 0:
            raise ValueError(f"{source}:{lineno}: expected KEY=VALUE, got: {raw!r}")

        key = line[:eq].strip()
        if not _KEY_RE.match(key):
            raise ValueError(f"{source}:{lineno}: invalid key {key!r}")

        rhs = line[eq + 1 :].lstrip()
        value = _parse_value(rhs, source=source, lineno=lineno)
        values[key] = value
    return values


def parse_dotenv_file(path: str | Path) -> dict[str, str]:
    """Read and parse a `.env` file. Raises if the file cannot be read."""
    resolved = Path(path).expanduser()
    try:
        text = resolved.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"Could not read env file {resolved}: {exc}") from exc
    return parse_dotenv_text(text, source=str(resolved))


def discover_dotenv_path(start: str | Path | None = None) -> Path | None:
    """Return the first `.env` file found in `start` (default: cwd) or `None`."""
    base = Path(start).expanduser() if start is not None else Path.cwd()
    candidate = base / ".env"
    return candidate if candidate.is_file() else None


def merge_env_layers(layers: Iterable[dict[str, str] | None]) -> dict[str, str]:
    """Merge multiple env dicts; later layers override earlier ones."""
    merged: dict[str, str] = {}
    for layer in layers:
        if not layer:
            continue
        merged.update(layer)
    return merged


def _parse_value(rhs: str, *, source: str, lineno: int) -> str:
    if not rhs:
        return ""
    quote = rhs[0]
    if quote in ('"', "'"):
        return _parse_quoted(rhs, quote=quote, source=source, lineno=lineno)
    # Strip inline `#` comment for unquoted values.
    hash_index = rhs.find(" #")
    if hash_index < 0:
        hash_index = rhs.find("\t#")
    if hash_index >= 0:
        rhs = rhs[:hash_index]
    return rhs.strip()


def _parse_quoted(rhs: str, *, quote: str, source: str, lineno: int) -> str:
    body = rhs[1:]
    if quote == "'":
        end = body.find("'")
        if end < 0:
            raise ValueError(f"{source}:{lineno}: unterminated single-quoted value")
        return body[:end]

    out: list[str] = []
    i = 0
    while i < len(body):
        ch = body[i]
        if ch == quote:
            return "".join(out)
        if ch == "\\" and i + 1 < len(body):
            nxt = body[i + 1]
            out.append(_unescape(nxt))
            i += 2
            continue
        out.append(ch)
        i += 1
    raise ValueError(f"{source}:{lineno}: unterminated double-quoted value")


def _unescape(ch: str) -> str:
    return {"n": "\n", "t": "\t", "r": "\r", "\\": "\\", '"': '"', "'": "'"}.get(ch, ch)
