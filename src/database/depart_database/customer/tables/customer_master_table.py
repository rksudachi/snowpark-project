from src.base.base_table import BaseTable, WriteMode
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import (
    col,
    WhenMatchedClause,
    WhenNotMatchedClause,
)
from src.utils.db_schema_map_reader import DbSchemaMapReader


class CustomerMasterTable(BaseTable):
    TABLE_NAME = "CUSTOMER_MASTER"
    ALLOWED_WRITE_MODES = {WriteMode.INSERT, WriteMode.MERGE}

    @property
    def database_key(self) -> str:
        return "ANALYTICS"

    @property
    def schema_key(self) -> str:
        return "PUBLIC"

    def __init__(self, session: Session, database: str, schema: str):
        super().__init__(session, database, schema)

    def _insert(
        self,
        source_df: DataFrame,
        statement_params=None,
        *args,
        **kwargs,
    ) -> DataFrame:
        # appendでテーブルに追加
        return source_df.write.mode("append").save_as_table(
            self.table_identifier, statement_params=statement_params
        )

    def _merge(
        self,
        source_df: DataFrame,
        statement_params=None,
        *args,
        **kwargs,
    ) -> DataFrame:
        table = self.session.table(self.table_identifier)
        # 主キーIDで結合
        join_expr = table["CUSTOMER_ID"] == source_df["CUSTOMER_ID"]
        clauses = [
            WhenMatchedClause(
                update={"CUSTOMER_NAME": source_df["CUSTOMER_NAME"]}
            ),
            WhenNotMatchedClause(
                insert={
                    "CUSTOMER_ID": source_df["CUSTOMER_ID"],
                    "CUSTOMER_NAME": source_df["CUSTOMER_NAME"],
                }
            ),
        ]
        return table.merge(
            source=source_df,
            join_expr=join_expr,
            clauses=clauses,
            statement_params=statement_params,
            block=True,
        )
