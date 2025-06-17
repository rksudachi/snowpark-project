from snowflake.snowpark import Session
from snowflake.snowpark.functions import sproc

# import shutil

# # 必要なファイル・ディレクトリだけ一時フォルダにコピー
# shutil.copytree("mypkg", "tmp_mypkg", ignore=shutil.ignore_patterns("test_*", "*.md", "*.txt", "sample_data", "__pycache__"))
# shutil.make_archive("my_lib", "zip", "tmp_mypkg")

# import os
# import shutil

# # コピー元とコピー先のルート
# src_root = "src"
# dst_root = "dst"

# # コピーしたいファイルのリスト（src_rootからの相対パス）
# file_list = [
#     "utils.py",
#     "core/main.py",
#     "core/sub/subutil.py"
# ]

# for rel_path in file_list:
#     # フルパス作成
#     src_path = os.path.join(src_root, rel_path)
#     dst_path = os.path.join(dst_root, rel_path)
#     # コピー先ディレクトリを作成（なければ）
#     os.makedirs(os.path.dirname(dst_path), exist_ok=True)
#     # ファイルをコピー
#     shutil.copy2(src_path, dst_path)

# print("コピー完了！")

# import sys

# def use_modules_from_multiple_paths():
#     paths_to_add = [
#         "/my_project/utils/special",
#         "/my_project/legacy",
#         "/my_project/experimental"
#     ]
#     # 元のsys.pathをバックアップ
#     original_sys_path = sys.path.copy()
#     try:
#         # 追加したいパスを先頭にまとめて追加
#         for p in reversed(paths_to_add):  # 逆順で先頭に挿入
#             sys.path.insert(0, p)
#         # 各パスにあるモジュールをimport
#         import my_special        # /special
#         import legacy_tool       # /legacy
#         import experimental_mod  # /experimental

#         # ここでモジュール利用
#         my_special.do_something()
#         legacy_tool.do_other()
#         experimental_mod.try_this()
#     finally:
#         # 終了時にsys.pathを元に戻す
#         sys.path = original_sys_path

# use_modules_from_multiple_paths()


def merge_customer_master_impl(
    session: Session, database: str, schema: str
) -> str:
    """
    ビジネスロジック層で集計したアクティブ顧客リストをcustomer_masterテーブルにMERGEするストアドプロシージャ
    トランザクション管理付き
    """
    # 必要なクラスは関数内でimport
    from src.database.depart_database.customer.logic.customer_master_logic import (
        CustomerMasterLogic,
    )
    from src.database.depart_database.customer.tables.customer_master_table import (
        CustomerMasterTable,
    )
    from src.database.depart_database.order.tables.order import OrderTable
    from src.base.base_table import WriteMode

    try:
        session.sql("BEGIN").collect()

        # テーブルクラスでDataFrame取得
        customer_master_table = CustomerMasterTable(session, database, schema)
        order_table = OrderTable(session, database, schema)
        customers_df = customer_master_table.read()
        orders_df = order_table.read()

        # ビジネスロジック層でDataFrameを加工（カラムはすべて大文字）
        active_customers_df = CustomerMasterLogic.aggregate_active_customers(
            customers_df, orders_df
        )

        # テーブルクラスでMERGE（カラム名は大文字で統一）
        customer_master_table.write(
            source_df=active_customers_df,
            mode=WriteMode.MERGE,
        )

        session.sql("COMMIT").collect()
        return (
            f"Merged active customers into {customer_master_table.TABLE_NAME}"
        )

    except Exception as e:
        session.sql("ROLLBACK").collect()
        raise e


def register_merge_customer_master_sproc(stage_location: str):
    return sproc(
        merge_customer_master_impl,
        name="merge_customer_master",
        packages=["snowflake-snowpark-python"],
        imports=[
            "src/database/depart_database/customer/logic/customer_master_logic.py",
            "src/database/depart_database/customer/tables/customer_master_table.py",
            "src/database/depart_database/order/tables/order.py",
            "src/base/base_table.py",
        ],
        stage_location=stage_location,
    )


# 使い方例（必要なタイミングで呼び出し、stageを指定）
# merge_customer_master = register_merge_customer_master_sproc("@your_stage")
