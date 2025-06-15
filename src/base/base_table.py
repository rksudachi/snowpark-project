from abc import ABC, abstractmethod
from enum import Enum, auto
from snowflake.snowpark import Session, DataFrame


class WriteMode(Enum):
    INSERT = auto()
    UPDATE = auto()
    MERGE = auto()
    TRUNCATE_INSERT = auto()
    DELETE = auto()


class BaseTable(ABC):
    TABLE_NAME: str = ""
    ALLOWED_WRITE_MODES = set()

    def __init__(self, session: Session, database: str, schema: str):
        self.session = session
        self.database = database
        self.schema = schema

    @property
    def table_identifier(self) -> str:
        """フルテーブル名を返す（DB.SCHEMA.TABLE）"""
        return f"{self.database}.{self.schema}.{self.TABLE_NAME}"

    def write(
        self,
        df: DataFrame,
        mode: WriteMode = WriteMode.INSERT,
        *args,
        **kwargs,
    ) -> DataFrame:
        """
        データを書き込む（modeでinsert, update, merge, truncate+insert, deleteを切り替え）
        戻り値は結果セットまたはテーブル（DataFrame）
        """
        if mode not in self.ALLOWED_WRITE_MODES:
            raise ValueError(f"{mode} はこのテーブルでは許可されていません")
        if mode == WriteMode.INSERT:
            return self._insert(df, *args, **kwargs)
        elif mode == WriteMode.UPDATE:
            return self._update(*args, **kwargs)
        elif mode == WriteMode.MERGE:
            return self._merge(*args, **kwargs)
        elif mode == WriteMode.TRUNCATE_INSERT:
            return self._truncate_insert(df, *args, **kwargs)
        elif mode == WriteMode.DELETE:
            return self._delete(*args, **kwargs)
        else:
            raise ValueError(f"Unknown write mode: {mode}")

    # 以下、必要なものだけサブクラスで実装すればOK
    def _insert(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        raise NotImplementedError("INSERTは未実装です")

    def _update(self, *args, **kwargs) -> DataFrame:
        raise NotImplementedError("UPDATEは未実装です")

    def _merge(self, *args, **kwargs) -> DataFrame:
        raise NotImplementedError("MERGEは未実装です")

    def _truncate_insert(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        raise NotImplementedError("TRUNCATE_INSERTは未実装です")

    def _delete(self, *args, **kwargs) -> DataFrame:
        raise NotImplementedError("DELETEは未実装です")
