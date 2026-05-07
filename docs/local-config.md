# Local Config

Local config lets you define Airflow connections, variables, and pools without editing your normal Airflow deployment.

## Resolution Order

The runner resolves a config path in this order:

1. explicit `--config-path` or `config_path=...`
2. `AIRFLOW_DEBUG_LOCAL_CONFIG`
3. `RUNBOOK_LOCAL_CONFIG`

If no config is found, the current Airflow environment is used.

## Config File Shape

The config file is a Python module. All globals are optional:

```python
CONNECTIONS = {
    "demo_http": {
        "conn_type": "http",
        "host": "example.com",
        "schema": "https",
        "login": "user",
        "password": "secret",
        "port": 443,
        "extra": {"timeout": 30, "verify": False},
    },
}

VARIABLES = {
    "ENV": "local",
    "FEATURES": {"experimental_pipeline": True},
}

POOLS = {
    "default_pool": {"slots": 4},
    "api_pool": {"slots": 2, "description": "Local API pool"},
}
```

## Connections

Each `CONNECTIONS` entry is serialized into an Airflow connection environment variable:

```text
AIRFLOW_CONN_<CONN_ID>
```

Connection payloads should be JSON-serializable. Common fields:

- `conn_type`
- `host`
- `schema`
- `login`
- `password`
- `port`
- `extra`

## Variables

Each `VARIABLES` entry is serialized into:

```text
AIRFLOW_VAR_<KEY>
```

String values are passed as-is. Non-string values are JSON-encoded.

## Pools

Pools are bootstrapped into the Airflow metadata DB before the local run. Supported fields:

- `slots`
- `description`
- `include_deferred`

`include_deferred` must be a boolean when provided.

## Extra Environment

Use `--env KEY=VALUE` for one-off values:

```bash
airflow-debug-run /absolute/path/to/my_dag.py \
  --env FEATURE_FLAG=local \
  --env API_BASE_URL=http://localhost:8080
```

Programmatic API:

```python
debug_dag(dag, extra_env={"FEATURE_FLAG": "local"})
```

`extra_env` is applied only for the local run and restored afterward.

## .env Files

Pull credentials and feature flags from a `.env` file instead of duplicating
them in `airflow_defaults.py`. Both CLI entrypoints accept `--env-file`
(repeatable) and auto-discover a `.env` in the current directory:

```bash
airflow-debug-run /abs/path/to/my_dag.py \
  --config-path /abs/path/to/airflow_defaults.py \
  --env-file ./creds.env \
  --env-file ./local.env
```

`airflow_defaults.py` can then reference these values via `os.getenv`:

```python
import os

CONNECTIONS = {
    "warehouse": {
        "conn_type": "snowflake",
        "host": "acme.snowflakecomputing.com",
        "login": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
    },
}
```

Resolution order (later wins):

1. auto-discovered `./.env` (skipped when `--env-file` is given or
   `--no-auto-env` is set)
2. each `--env-file PATH` in the order they appear
3. each `--env KEY=VALUE`
4. existing process environment is preserved when none of the above set the
   key

### Supported syntax

The bundled parser handles the common subset of `.env` syntax:

- `KEY=VALUE` lines, with whitespace allowed around `=` and around the value.
- Keys must match `[A-Za-z_][A-Za-z0-9_]*`.
- Empty lines and `#` comments at the start of a line.
- Inline `# comment` after an unquoted value is stripped.
- Single-quoted values: literal text, no escapes — `'a$b'` stays as `a$b`.
- Double-quoted values: `\n`, `\t`, `\r`, `\\`, `\"` escapes are interpreted.
- Optional `export ` prefix (compatible with shell-sourced `.env`).
- Empty values are allowed: `KEY=` parses as `{"KEY": ""}`.
- Last assignment for a key wins.

```env
# auth
DB_USER=root
DB_PASSWORD="s3cret # not a comment"
API_HOST = "https://api.example.com"
GREETING="line one\nline two"
LITERAL='no $expansion here'
export FEATURE_FLAG=local
```

### Not supported (intentional)

To stay dependency-free, the parser intentionally omits the more permissive
features of `python-dotenv` and shell semantics:

- **Variable expansion** (`KEY=$OTHER`, `KEY=${OTHER:-default}`) — values are
  treated as literal text.
- **Multiline values** (heredocs or triple-quoted blocks).
- **Backtick command substitution** (`KEY=$(command)`) — taken literally.
- **Inline comments inside double-quoted values** are preserved as data
  (only unquoted values strip ` #...`).

If you need these, parse the file with `python-dotenv` yourself and pass the
result to `extra_env=` programmatically.

The library API exposes the same parser:

```python
from airflow_local_debug import parse_dotenv_file

values = parse_dotenv_file(".env")
debug_dag(dag, extra_env=values)
```

## Validation

Use doctor before a run:

```bash
airflow-debug-doctor \
  --config-path /absolute/path/to/airflow_defaults.py \
  --require-config
```

Doctor validates that connection and variable payloads can be serialized and that pool definitions have usable fields.
