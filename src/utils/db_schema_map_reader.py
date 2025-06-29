# from src.utils.config import settings
from src.utils.db_schema_map_interface import DBSchemaMapInterface
from src.utils.generate_module import generate_module


class DbSchemaMapReader:
    module_path: str = "src/generated/db_schema_map.py"
    class_name: str = "DBSchemaMap"

    def __init__(self):
        self.db_schema_map_class: DBSchemaMapInterface = generate_module(
            self.module_path, self.class_name
        )
        self.db_schema = self.db_schema_map_class()


singleton = None


# def get_db_schema_search_instance():
#     global singleton
#     if singleton is None:
#         singleton = DbSchemaMapReader()
#     return singleton


if __name__ == "__main__":
    db_schema_search = DbSchemaMapReader()
    db_schema_instance: DBSchemaMapInterface = (
        db_schema_search.db_schema  # インスタンスを取得
    )
    print(
        db_schema_instance.find_combination_db_schema("ANALYTICS", "PUBLIC")
    )  # インスタンスの属性を表示
