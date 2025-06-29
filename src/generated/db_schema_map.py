from src.utils.db_schema_map_interface import (
    DBSchemaMapInterface,
    DbKey,
    AnalyticsSchemaKey,
    AppSchemaKey,
)


class DBSchemaMap(DBSchemaMapInterface):
    # db_map_tuple = (
    #     {DbKey.ANALYTICS.value: "ANALYTICS_DEV"},
    #     {DbKey.APP.value: "APP_DEV"},
    # )
    db_schema_simple_map = (
        {"analytics_db": "analytics_db_schema"},
        {"app_db": "app_db_schema"},
    )
    analytics_db = {DbKey.ANALYTICS.value: "ANALYTICS_DEV"}
    app_db = {DbKey.APP.value: "APP_DEV"}

    analytics_db_schema = (
        {AnalyticsSchemaKey.PUBLIC.value: "PUBLIC_DEV"},
        {AnalyticsSchemaKey.STAGING.value: "STAGING_DEV"},
    )

    app_db_schema = (
        {AppSchemaKey.PUBLIC.value: "PUBLIC_DEV"},
        {AppSchemaKey.INTERNAL.value: "INTERNAL_DEV"},
    )

    # db_schema_simple_map = (
    #     {analytics_db: analytics_db_schema},
    #     {app_db: app_db_schema},
    # )

    # db_schema_map = ()

    # db_schema_map = (
    #     {DbKey.ANALYTICS.value: "ANALYTICS_DEV", "PUBLIC": "PUBLIC_DEV"},
    #     {DbKey.ANALYTICS.value: "ANALYTICS_DEV", "STAGING": "STAGING_DEV"},
    #     {DbKey.APP.value: "APP_DEV", "PUBLIC": "PUBLIC_DEV"},
    #     {DbKey.APP.value: "APP_DEV", "INTERNAL": "INTERNAL_DEV"},
    # )

    def __init__(self):
        self.db_schema_map = self.combine_db_schema()

    # @classmethod
    def combine_db_schema(self):
        combined = tuple()
        for db_schema_simple in self.db_schema_simple_map:
            combined += self._combine_single_db_schema(db_schema_simple)
        return combined

    def _combine_single_db_schema(self, db_schema_simple):
        combined = tuple()
        for k, v in db_schema_simple.items():
            db_map = getattr(self, k)
            schema_map_tuple = getattr(self, v)
            combined += self._merge_db_and_schema(db_map, schema_map_tuple)
        return combined

    def _merge_db_and_schema(self, db_map, schema_map_tuple):
        combined = tuple()
        for schema in schema_map_tuple:
            combined += ({**db_map, **schema},)
        return combined