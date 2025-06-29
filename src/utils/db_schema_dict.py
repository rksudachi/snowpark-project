# from src.utils.config import settings
from src.utils.rendering_result_interface import DBSchemaDictInterface
from src.utils.generate_module import generate_module


class DbSchemaSearch:
    module_path: str = "src/generated/rendering_result.py"
    class_name: str = "DBSchemaDict"

    def __init__(self):
        self.db_schema_class: DBSchemaDictInterface = generate_module(
            self.module_path, self.class_name
        )


if __name__ == "__main__":
    db_schema_search = DbSchemaSearch()
    db_schema_instance = db_schema_search.db_schema_class()
    print(
        db_schema_instance.find_combination_dbschema("ANALYTICS", "PUBLIC")
    )  # インスタンスの属性を表示
