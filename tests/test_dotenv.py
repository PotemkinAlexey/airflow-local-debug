from __future__ import annotations

from pathlib import Path

import pytest

from airflow_local_debug.dotenv import (
    discover_dotenv_path,
    merge_env_layers,
    parse_dotenv_file,
    parse_dotenv_text,
)


def test_parse_simple_assignments() -> None:
    text = "FOO=bar\nBAZ=qux\n"
    assert parse_dotenv_text(text) == {"FOO": "bar", "BAZ": "qux"}


def test_parse_skips_blank_and_comment_lines() -> None:
    text = """
    # leading comment
    FOO=bar

    # another comment
    BAZ=qux
    """
    assert parse_dotenv_text(text) == {"FOO": "bar", "BAZ": "qux"}


def test_parse_supports_export_prefix() -> None:
    assert parse_dotenv_text("export FOO=bar") == {"FOO": "bar"}


def test_parse_double_quoted_value_preserves_spaces() -> None:
    assert parse_dotenv_text('GREETING="hello world"') == {"GREETING": "hello world"}


def test_parse_single_quoted_value_preserves_dollar_signs() -> None:
    assert parse_dotenv_text("SECRET='$raw$value'") == {"SECRET": "$raw$value"}


def test_parse_double_quoted_value_handles_escapes() -> None:
    assert parse_dotenv_text('LINE="a\\nb\\tc"') == {"LINE": "a\nb\tc"}


def test_parse_strips_inline_comment_for_unquoted_values() -> None:
    assert parse_dotenv_text("FOO=bar # trailing comment") == {"FOO": "bar"}


def test_parse_keeps_inline_hash_inside_quotes() -> None:
    assert parse_dotenv_text('TOKEN="abc#def"') == {"TOKEN": "abc#def"}


def test_parse_rejects_invalid_key() -> None:
    with pytest.raises(ValueError, match="invalid key"):
        parse_dotenv_text("1FOO=bar")


def test_parse_rejects_missing_equals() -> None:
    with pytest.raises(ValueError, match="expected KEY=VALUE"):
        parse_dotenv_text("just_a_word")


def test_parse_rejects_unterminated_quote() -> None:
    with pytest.raises(ValueError, match="unterminated"):
        parse_dotenv_text('FOO="oops')


def test_parse_dotenv_file_reads_from_disk(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("DB_PASSWORD=s3cret\nAPI_KEY=abc\n")

    values = parse_dotenv_file(env_file)

    assert values == {"DB_PASSWORD": "s3cret", "API_KEY": "abc"}


def test_parse_dotenv_file_missing_path_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Could not read env file"):
        parse_dotenv_file(tmp_path / "missing.env")


def test_discover_dotenv_path_returns_existing_file(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("FOO=bar")
    assert discover_dotenv_path(tmp_path) == tmp_path / ".env"


def test_discover_dotenv_path_returns_none_when_absent(tmp_path: Path) -> None:
    assert discover_dotenv_path(tmp_path) is None


def test_merge_env_layers_later_layers_win() -> None:
    merged = merge_env_layers(
        [
            {"A": "1", "B": "2"},
            {"B": "override", "C": "3"},
            None,
        ]
    )
    assert merged == {"A": "1", "B": "override", "C": "3"}
