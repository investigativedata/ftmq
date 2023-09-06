import os

STORE_URI = os.environ.get("STORE_URI", "memory:///")
DB_STORE_URI = os.environ.get(
    "FTM_STORE_URI",
    os.environ.get("NOMENKLATURA_DB_URL", "sqlite:///followthemoney.store"),
)
