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

## Validation

Use doctor before a run:

```bash
airflow-debug-doctor \
  --config-path /absolute/path/to/airflow_defaults.py \
  --require-config
```

Doctor validates that connection and variable payloads can be serialized and that pool definitions have usable fields.
