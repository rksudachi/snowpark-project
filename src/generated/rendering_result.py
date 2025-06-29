# try:
from src.utils.rendering_result_interface import DBSchemaDictInterface

# except ImportError:
#     from rendering_result_interface import RenderingResultInterface


class DBSchemaDict(DBSchemaDictInterface):

    # 
    dbschema = (
        {"ANALYTICS": "ANALYTICS_DEV", "PUBLIC": "PUBLIC_DEV"},
        {"ANALYTICS": "ANALYTICS_DEV", "STAGING": "STAGING_DEV"},
        {"APP": "APP_DEV", "PUBLIC": "PUBLIC_DEV"},
        {"APP": "APP_DEV", "INTERNAL": "INTERNAL_DEV"},
    )