from src.base.base_table import BaseTable, WriteMode
from snowflake.snowpark import Session, DataFrame


class CustomerMasterTable(BaseTable):
    TABLE_NAME = "customer_master"
    ALLOWED_WRITE_MODES = {WriteMode.INSERT, WriteMode.MERGE}

    def __init__(self, session: Session, database: str, schema: str):
        super().__init__(session, database, schema)
        self.table_identifier = (
            f"{self.database}.{self.schema}.{self.TABLE_NAME}"
        )

    def read(self) -> DataFrame:
        """テーブルからデータを読み込む"""
        return self.session.table(self.table_identifier)

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
        table = self.session.table(self.table_name)
        if mode == WriteMode.INSERT:
            return df.write.mode("append").save_as_table(self.table_name)
        elif mode == WriteMode.UPDATE:
            # kwargs例: assignments, condition
            assignments = kwargs.get("assignments")
            condition = kwargs.get("condition")
            if assignments is None or condition is None:
                raise ValueError("UPDATEにはassignmentsとconditionが必要です")
            return table.update(assignments=assignments, condition=condition)
        elif mode == WriteMode.MERGE:
            # kwargs例: source, join_expression, assignments,
            #           target_alias, source_alias
            source = kwargs.get("source")
            join_expression = kwargs.get("join_expression")
            assignments = kwargs.get("assignments")
            if (
                source is None
                or join_expression is None
                or assignments is None
            ):
                raise ValueError(
                    "MERGEにはsource, join_expression, assignmentsが必要です"
                )
            return table.merge(
                source=source,
                join_expression=join_expression,
                assignments=assignments,
                target_alias=kwargs.get("target_alias"),
                source_alias=kwargs.get("source_alias"),
            )
        elif mode == WriteMode.TRUNCATE_INSERT:
            self.session.sql(f"TRUNCATE TABLE {self.table_name}").collect()
            return df.write.mode("append").save_as_table(self.table_name)
        elif mode == WriteMode.DELETE:
            # kwargs例: condition
            condition = kwargs.get("condition")
            if condition is None:
                raise ValueError("DELETEにはconditionが必要です")
            return table.delete(condition=condition)
        else:
            raise ValueError(f"Unknown write mode: {mode}")

    def _insert(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        return df.write.mode("append").save_as_table(self.table_name)

    def _merge(self, *args, **kwargs) -> DataFrame:
        table = self.session.table(self.table_name)
        source = kwargs.get("source")
        join_expression = kwargs.get("join_expression")
        assignments = kwargs.get("assignments")
        if source is None or join_expression is None or assignments is None:
            raise ValueError(
                "MERGEにはsource, join_expression, assignmentsが必要です"
            )
        return table.merge(
            source=source,
            join_expression=join_expression,
            assignments=assignments,
            target_alias=kwargs.get("target_alias"),
            source_alias=kwargs.get("source_alias"),
        )
