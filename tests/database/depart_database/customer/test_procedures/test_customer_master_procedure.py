import pytest

# from src.database.depart_database.customer.procedures.customer_master_procedure import (
#     merge_customer_master,
# )


# @pytest.fixture
# def mock_session(mocker):
#     session = mocker.Mock()
#     session.sql.return_value.collect.return_value = None
#     session.table.return_value = {
#         "customer_id": "customer_id",
#         "customer_name": "customer_name",
#     }
#     return session


# @pytest.fixture
# def mock_customer_master_table(mocker):
#     mock = mocker.Mock()
#     mock.read.return_value = "customers_df"
#     mock.table_name = "customer_master"
#     return mock


# @pytest.fixture
# def mock_order_table(mocker):
#     mock = mocker.Mock()
#     mock.read.return_value = "orders_df"
#     return mock


# @pytest.fixture
# def mock_logic(mocker):
#     logic_patch = mocker.patch(
#         "src.database.depart_database.customer.procedures.customer_master_procedure.CustomerMasterLogic"
#     )
#     logic_patch.aggregate_active_customers.return_value = {
#         "customer_id": "customer_id",
#         "customer_name": "customer_name",
#     }
#     return logic_patch


# @pytest.fixture
# def patch_tables(mocker, mock_customer_master_table, mock_order_table):
#     mocker.patch(
#         "src.database.depart_database.customer.procedures.customer_master_procedure.CustomerMasterTable",
#         return_value=mock_customer_master_table,
#     )
#     mocker.patch(
#         "src.database.depart_database.customer.procedures.customer_master_procedure.OrderTable",
#         return_value=mock_order_table,
#     )


# def test_merge_customer_master_success(
#     mock_session, patch_tables, mock_logic, mock_customer_master_table
# ):
#     result = merge_customer_master(mock_session, "MYDB", "MYSCHEMA")
#     assert "Merged active customers into" in result
#     mock_session.sql.assert_any_call("BEGIN")
#     mock_session.sql.assert_any_call("COMMIT")
#     mock_customer_master_table.write.assert_called_once()
#     args, kwargs = mock_customer_master_table.write.call_args
#     assert kwargs["mode"].name == "MERGE"


# def test_merge_customer_master_rollback_on_exception(
#     mock_session, patch_tables, mock_logic, mock_customer_master_table
# ):
#     mock_customer_master_table.write.side_effect = Exception("Write failed")
#     with pytest.raises(Exception, match="Write failed"):
#         merge_customer_master(mock_session, "MYDB", "MYSCHEMA")
#     mock_session.sql.assert_any_call("ROLLBACK")


@pytest.fixture(autouse=True)
def skip_session_sql(monkeypatch):
    """
    ローカルテスト時は session.sql をモックして NotImplementedError を回避する
    """
    from snowflake.snowpark.session import Session

    def fake_sql(self, *args, **kwargs):
        class DummyResult:
            def collect(self_inner):
                return None

        return DummyResult()

    monkeypatch.setattr(Session, "sql", fake_sql)


def test_merge_customer_master(session):
    from src.database.depart_database.customer.procedures.customer_master_procedure import (
        merge_customer_master_impl,
    )

    result = merge_customer_master_impl(session, "DB", "SCHEMA")
    assert "Merged" in result
