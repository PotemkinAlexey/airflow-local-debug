CONNECTIONS = {
    "warehouse": {
        "conn_type": "postgres",
        "host": "localhost",
        "schema": "analytics",
        "login": "airflow",
        "password": "airflow",
        "port": 5432,
        "extra": {"sslmode": "disable"},
    },
    "orders_api": {
        "conn_type": "http",
        "host": "localhost",
        "port": 8080,
        "extra": {"timeout": 5, "verify": False},
    },
}

VARIABLES = {
    "ENV": "local",
    "ORDER_LIMIT": 2,
    "FEATURES": {"load_warehouse": False},
}

POOLS = {
    "default_pool": {"slots": 4},
    "warehouse_pool": {"slots": 1, "description": "Local warehouse calls"},
}
