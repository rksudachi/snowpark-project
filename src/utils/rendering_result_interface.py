class DBSchemaDictInterface:

    # dbschema = (
    #     {"ANALYTICS": "ANALYTICS_DEV", "PUBLIC": "PUBLIC_DEV"},
    #     {"ANALYTICS": "ANALYTICS_DEV", "STAGING": "STAGING_DEV"},
    #     {"APP": "APP_DEV", "PUBLIC": "PUBLIC_DEV"},
    #     {"APP": "APP_DEV", "INTERNAL": "INTERNAL_DEV"},
    # )

    dbschema = tuple()

    @classmethod
    def find_combination_dbschema(cls, db_key: str, schema_key: str) -> set:
        for d in cls.dbschema:
            if db_key in d and schema_key in d:
                return {d[db_key], d[schema_key]}
