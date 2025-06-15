from src.base.base_table import BaseTable, WriteMode
from snowflake.snowpark import Session, DataFrame


class OrderTable(BaseTable):
    TABLE_NAME = "order"
    ALLOWED_WRITE_MODES = {WriteMode.INSERT, WriteMode.MERGE}

    def __init__(self, session: Session, database: str, schema: str):
        super().__init__(session, database, schema)

    def read(self) -> DataFrame:
        """注文テーブルからデータを読み込む"""
        return self.session.table(self.table_name)

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
