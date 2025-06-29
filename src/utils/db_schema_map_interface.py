class DBSchemaMapInterface:

    # dbschema = (
    #     {"ANALYTICS": "ANALYTICS_DEV", "PUBLIC": "PUBLIC_DEV"},
    #     {"ANALYTICS": "ANALYTICS_DEV", "STAGING": "STAGING_DEV"},
    #     {"APP": "APP_DEV", "PUBLIC": "PUBLIC_DEV"},
    #     {"APP": "APP_DEV", "INTERNAL": "INTERNAL_DEV"},
    # )

    db_schema_map = tuple()

    def find_combination_db_schema(self, db_key: str, schema_key: str) -> set:
        for d in self.db_schema_map:
            if db_key in d and schema_key in d:
                return {d[db_key], d[schema_key]}


from enum import Enum


class DbKey(Enum):
    """
    Enum for database keys.
    """

    ANALYTICS = "ANALYTICS"
    APP = "APP"
    # PUBLIC = "PUBLIC"
    # STAGING = "STAGING"
    # INTERNAL = "INTERNAL"


class AnalyticsSchemaKey(Enum):
    """
    Enum for analytics schema keys.
    """

    PUBLIC = "PUBLIC"
    STAGING = "STAGING"


class AppSchemaKey(Enum):
    """Enum for app schema keys."""

    PUBLIC = "PUBLIC"
    INTERNAL = "INTERNAL"
