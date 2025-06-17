from enum import Enum, auto
from snowflake.snowpark import Session, DataFrame, Column
from typing import Optional, Dict, Any


class WriteMode(Enum):
    INSERT = auto()
    UPDATE = auto()
    MERGE = auto()
    TRUNCATE_INSERT = auto()
    DELETE = auto()


class BaseTable:
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

    def read(self) -> DataFrame:
        """テーブルからデータを読み込む"""
        return self.session.table(self.table_identifier)

    def write(
        self,
        source_df: DataFrame,
        mode: WriteMode = WriteMode.INSERT,
        statement_params: Optional[Dict[str, Any]] = None,
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
            return self._insert(
                source_df, statement_params=statement_params, *args, **kwargs
            )
        elif mode == WriteMode.UPDATE:
            return self._update(
                source_df,
                statement_params=statement_params,
                *args,
                **kwargs,
            )
        elif mode == WriteMode.MERGE:
            return self._merge(
                source_df, statement_params=statement_params, *args, **kwargs
            )
        elif mode == WriteMode.TRUNCATE_INSERT:
            return self._truncate_insert(
                source_df, statement_params=statement_params, *args, **kwargs
            )
        elif mode == WriteMode.DELETE:
            return self._delete(
                source_df,
                statement_params=statement_params,
                *args,
                **kwargs,
            )
        else:
            raise ValueError(f"Unknown write mode: {mode}")

    # 以下、必要なものだけサブクラスで実装すればOK
    def _insert(
        self,
        source_df: DataFrame,
        statement_params: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs,
    ) -> DataFrame:
        raise NotImplementedError("INSERTは未実装です")

    def _update(
        self,
        source_df: DataFrame,
        statement_params: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs,
    ) -> DataFrame:
        raise NotImplementedError("UPDATEは未実装です")

    def _merge(
        self,
        source_df: DataFrame,
        statement_params: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs,
    ) -> DataFrame:
        raise NotImplementedError("MERGEは未実装です")

    def _truncate_insert(
        self,
        source_df: DataFrame,
        statement_params: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs,
    ) -> DataFrame:
        raise NotImplementedError("TRUNCATE_INSERTは未実装です")

    def _delete(
        self,
        source_df: DataFrame,
        statement_params: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs,
    ) -> DataFrame:
        raise NotImplementedError("DELETEは未実装です")
