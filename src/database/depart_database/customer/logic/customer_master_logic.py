from snowflake.snowpark import DataFrame


class CustomerMasterLogic:
    @staticmethod
    def aggregate_active_customers(
        customers: DataFrame, orders: DataFrame
    ) -> DataFrame:
        """
        注文テーブルと結合し、直近1年で注文があるアクティブ顧客のリストを作成
        """
        from snowflake.snowpark.functions import col, current_date, datediff

        # 直近1年の注文のみ抽出
        recent_orders = orders.filter(
            datediff("day", col("ORDER_DATE"), current_date()) <= 365
        )

        # 顧客と注文を結合し、アクティブ顧客を抽出
        joined = customers.join(
            recent_orders,
            customers["CUSTOMER_ID"] == recent_orders["CUSTOMER_ID"],
        )
        active_customers = joined.select(
            customers["CUSTOMER_ID"], customers["CUSTOMER_NAME"]
        ).distinct()

        return active_customers
