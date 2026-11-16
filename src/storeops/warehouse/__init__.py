from storeops.warehouse.base import OPS_TABLES, RAW_META, Warehouse, ensure_ops

__all__ = ["OPS_TABLES", "RAW_META", "Warehouse", "ensure_ops", "connect"]


def connect(settings, secrets):
    if settings.warehouse == "bigquery":
        from storeops.warehouse.bigquery import BigQueryWarehouse

        return BigQueryWarehouse(settings.bq_project, settings.bq_location)
    from storeops.warehouse.postgres import PostgresWarehouse

    return PostgresWarehouse.from_settings(settings, secrets.get(f"{settings.pg_user}_password"))
