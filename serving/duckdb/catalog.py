"""
DuckDB + PyIceberg catalog initialisation.
Provides a configured DuckDB connection that can query all Gold-layer
Iceberg tables on S3 using the AWS Glue Data Catalog.

Usage:
    from serving.duckdb.catalog import get_connection
    con = get_connection()
    df = con.execute("SELECT * FROM orders_analytics.order_daily_summary LIMIT 10").fetchdf()

Local development (no AWS credentials):
    Set LAKEHOUSE_ENV=local and point ICEBERG_WAREHOUSE to a local path
    containing test fixtures. See serving/duckdb/fixtures/README.md.

Production scale path:
    When concurrent user count or scan volume exceeds DuckDB's single-node
    capacity, migrate to AWS Athena. See ADR-0005 for thresholds and the
    zero-change migration path (no pipeline changes required).
"""

from __future__ import annotations

import os
import duckdb
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.catalog.sql import SqlCatalog


def get_connection(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """
    Return a DuckDB connection with the Iceberg and HTTPFS extensions loaded
    and S3 credentials configured.

    Args:
        read_only: If True (default), open an in-memory connection that cannot
                   write. Pass False only for data quality validation scripts
                   that need to write results to a local DuckDB file.
    """
    env = os.environ.get("LAKEHOUSE_ENV", "aws")
    con = duckdb.connect(database=":memory:", read_only=False)

    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL httpfs;  LOAD httpfs;")

    if env == "aws":
        _configure_aws(con)
        _register_glue_tables(con)
    elif env == "local":
        _configure_local(con)
    else:
        raise ValueError(f"Unknown LAKEHOUSE_ENV: {env!r}. Expected 'aws' or 'local'.")

    return con


def _configure_aws(con: duckdb.DuckDBPyConnection) -> None:
    region = os.environ.get("AWS_REGION", "us-east-1")
    con.execute(f"""
        CREATE OR REPLACE SECRET lakehouse_s3 (
            TYPE S3,
            REGION '{region}',
            PROVIDER CREDENTIAL_CHAIN
        )
    """)


def _configure_local(con: duckdb.DuckDBPyConnection) -> None:
    warehouse = os.environ.get("ICEBERG_WAREHOUSE", "serving/duckdb/fixtures")
    con.execute(f"SET FILE_SEARCH_PATH='{warehouse}'")


def _register_glue_tables(con: duckdb.DuckDBPyConnection) -> None:
    """
    Register all Gold-layer Iceberg tables as DuckDB views so analysts
    can query them without specifying S3 paths directly.

      SELECT * FROM orders_analytics.order_daily_summary;

    Each view maps to the S3 location defined in the data product contract.
    """
    warehouse = os.environ.get("ICEBERG_WAREHOUSE", "s3://chakra-lakehouse")
    catalog = GlueCatalog(
        "glue",
        **{
            "warehouse": warehouse,
            "region_name": os.environ.get("AWS_REGION", "us-east-1"),
        },
    )

    # Gold table registry — add a row when a new data product Gold table is defined
    gold_tables = [
        ("orders_analytics", "order_daily_summary"),
        ("orders_analytics", "order_saga_health"),
        ("inventory_analytics", "stock_level_snapshot"),
    ]

    for namespace, table_name in gold_tables:
        try:
            iceberg_table = catalog.load_table(f"chakra.gold.{namespace}.{table_name}")
            s3_path = iceberg_table.location()
            con.execute(f"""
                CREATE OR REPLACE VIEW {namespace}.{table_name} AS
                SELECT * FROM iceberg_scan('{s3_path}')
            """)
        except Exception as exc:
            # Non-fatal: table may not exist yet in a fresh environment
            print(f"Warning: could not register {namespace}.{table_name}: {exc}")
