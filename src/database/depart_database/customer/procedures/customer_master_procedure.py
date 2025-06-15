from snowflake.snowpark import Session
from snowflake.snowpark.functions import sproc
from src.database.depart_database.customer.logic.customer_master_logic import (
    CustomerMasterLogic,
)
from src.database.depart_database.customer.tables.customer_master_table import (
    CustomerMasterTable,
)
from src.database.depart_database.order.tables.order import OrderTable
from src.base.base_table import WriteMode


def merge_customer_master_impl(
    session: Session, database: str, schema: str
) -> str:
    """
    ビジネスロジック層で集計したアクティブ顧客リストをcustomer_masterテーブルにMERGEするストアドプロシージャ
    トランザクション管理付き
    """
    try:
        session.sql("BEGIN").collect()

        # テーブルクラスでDataFrame取得
        customer_master_table = CustomerMasterTable(session, database, schema)
        order_table = OrderTable(session, database, schema)
        customers_df = customer_master_table.read()
        orders_df = order_table.read()

        # ビジネスロジック層でDataFrameを加工
        active_customers_df = CustomerMasterLogic.aggregate_active_customers(
            customers_df, orders_df
        )

        # MERGE用の条件・割当
        target_table = session.table(customer_master_table.table_name)
        join_expression = (
            target_table["customer_id"] == active_customers_df["customer_id"]
        )
        assignments = {"customer_name": active_customers_df["customer_name"]}

        # テーブルクラスでMERGE
        customer_master_table.write(
            df=active_customers_df,
            mode=WriteMode.MERGE,
            source=active_customers_df,
            join_expression=join_expression,
            assignments=assignments,
        )

        session.sql("COMMIT").collect()
        return (
            f"Merged active customers into {customer_master_table.table_name}"
        )

    except Exception as e:
        session.sql("ROLLBACK").collect()
        raise e


merge_customer_master = sproc(
    merge_customer_master_impl, name="merge_customer_master"
)
