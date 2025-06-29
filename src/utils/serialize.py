# from dataclasses import dataclass
# import yaml
# import pickle
# from pydantic_settings import BaseSettings
# from typing import Tuple, Dict


# # @dataclass(frozen=True)
# class Schema:
#     """
#     データベーススキーマを表すクラス
#     """

#     # PUBLIC: str = None
#     # STAGING: str = None
#     # INTERNAL: str = None

#     def __init__(self, **kwargs):
#         for key, value in kwargs.items():
#             setattr(self, key, value)

#     def __repr__(self):
#         return f"Schema({', '.join(f'{k}={v!r}' for k, v in self.__dict__.items())})"

#     def __eq__(self, other):
#         if not isinstance(other, Schema):
#             return NotImplemented
#         return self.__dict__ == other.__dict__

#     def __dict__(self):
#         return self.__dict__

#     def __len__(self):
#         return len(self.__dict__)


# class Database:
#     # database_key: str = None
#     # database_name: str = None
#     def __init__(self, **kwargs):
#         for key, value in kwargs.items():
#             setattr(self, key, value)

#     def __repr__(self):
#         return f"Database({', '.join(f'{k}={v!r}' for k, v in self.__dict__.items())})"

#     def __eq__(self, other):
#         if not isinstance(other, Database):
#             return NotImplemented
#         return self.__dict__ == other.__dict__

#     def __dict__(self):
#         return self.__dict__

#     def __len__(self):
#         return len(self.__dict__)

#     # schema = Schema("analytics", "analytics")


# class DbSchemas:
#     """
#     データベーススキーマの集合を表すクラス
#     """

#     def __init__(self, **kwargs):
#         for key, value in kwargs.items():
#             setattr(self, key, value)

#     def __repr__(self):
#         return f"DbSchemas({', '.join(f'{k}={v!r}' for k, v in self.__dict__.items())})"

#     def __eq__(self, other):
#         if not isinstance(other, DbSchemas):
#             return NotImplemented
#         return self.__dict__ == other.__dict__

#     def __dict__(self):
#         return self.__dict__

#     def __len__(self):
#         return len(self.__dict__)


# class DbSchemaDict:
#     """
#     (db_key, schema_key) → (db_value, schema_value) の対応を管理する辞書風クラス
#     """

#     def __init__(self):
#         self._map: Dict[Tuple[str, str], Tuple[str, str]] = {}

#     def set(
#         self, db_key: str, schema_key: str, db_value: str, schema_value: str
#     ):
#         self._map[(db_key, schema_key)] = (db_value, schema_value)

#     def get(self, db_key: str, schema_key: str):
#         return self._map.get((db_key, schema_key))

#     def __getitem__(self, keys: Tuple[str, str]):
#         return self._map[keys]

#     def __setitem__(self, keys: Tuple[str, str], values: Tuple[str, str]):
#         self._map[keys] = values

#     def items(self):
#         return self._map.items()


# class DBSchemaDict:

#     flag = "{{flag}}"
#     dbschema = (
#         {"ANALYTICS": "{{ANALYSIS}}", "PUBLIC": "{{PUBLIC}}"},
#         {"ANALYTICS": "{{ANALYSIS}}", "STAGING": "{{STAGING}}"},
#         {"APP": "{{APP}}", "PUBLIC": "{{PUBLIC}}"},
#         {"APP": "{{APP}}", "INTERNAL": "{{INTERNAL}}"},
#     )


# def load_db_schemas_from_yaml(yaml_path: str, mode: str) -> Database:
#     """
#     YAMLファイルから指定モードのDatabaseインスタンスを生成
#     """
#     with open(yaml_path, encoding="utf-8") as f:
#         data = yaml.safe_load(f)
#     # modeで切り替え（例: dev, prod など）
#     db = data[mode]["DB"]
#     schemas = data[mode]["SCHEMAS"]

#     dbs = tuple()

#     for db_k, schema_kv in schemas.items():
#         db_v = db[db_k]
#         for schema_k, schema_v in schema_kv.items():
#             dbs = dbs + ({db_k: db_v, schema_k: schema_v},)

#     print(dbs)

#     # db_schemas = Database(**db)
#     db_sch = DbSchemas(**{k: Schema(**v) for k, v in schemas.items()})
#     return db_sch


# def serialize_db_schemas(db_schemas: Database, file_path: str):
#     """
#     Databaseインスタンスをシリアライズしてファイル出力
#     """
#     with open(file_path, "wb") as f:
#         pickle.dump(db_schemas, f)


# def deserialize_db_schemas(file_path: str) -> Database:
#     """
#     シリアライズファイルからDatabaseインスタンスを復元
#     """
#     with open(file_path, "rb") as f:
#         return pickle.load(f)


# # --- 使い方例 ---
# # 1. YAML例
# # dev:
# #   ANALYTICS:
# #     PUBLIC: public_dev
# #     STAGING: staging_dev
# #   APP:
# #     PUBLIC: public_dev
# #     INTERNAL: internal_dev
# # prod:
# #   ANALYTICS:
# #     PUBLIC: public
# #     STAGING: staging
# #   APP:
# #     PUBLIC: public
# #     INTERNAL: internal

# # 2. インスタンス生成
# # db_schemas = load_db_schemas_from_yaml("db_schemas.yaml", mode="dev")

# # 3. シリアライズ
# # serialize_db_schemas(db_schemas, "db_schemas.pkl")

# # 4. デシリアライズ
# # loaded = deserialize_db_schemas("db_schemas.pkl")
# # print(loaded.APP.INTERNAL)

# if __name__ == "__main__":
#     # YAMLファイルからデータベーススキーマをロード
#     db_schemas = load_db_schemas_from_yaml(
#         "src/resources/config.yml", mode="DEV"
#     )
#     print(db_schemas)

#     # シリアライズしてファイルに保存
#     serialize_db_schemas(db_schemas, "db_schemas.pkl")

#     # デシリアライズしてインスタンスを復元
#     loaded_db_schemas = deserialize_db_schemas("db_schemas.pkl")
#     print(loaded_db_schemas)
