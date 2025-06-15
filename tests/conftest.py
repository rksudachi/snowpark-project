import pytest
from snowflake.snowpark import Session

# from src.utils.config import settings


def pytest_addoption(parser):
    parser.addoption("--snowflake-session", action="store", default="local")


@pytest.fixture(scope="module")
def session(request) -> Session:
    if request.config.getoption("--snowflake-session") == "local":
        # ローカルテスト用セッション
        session = Session.builder.config("local_testing", True).create()
        return session
    else:
        #     # 環境変数や.envから接続情報を取得
        #     connection_parameters = settings.snowflake.connection.dict()
        #     return Session.builder.configs(connection_parameters).create()
        pass
