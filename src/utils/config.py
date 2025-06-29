import os
from pathlib import Path
import yaml
from dotenv import load_dotenv

# from pydantic_settings import BaseSettings
from dataclasses import dataclass
from src.utils.rendering_db_schema_map import db_schema_map_template_rendering


# .env から CONFIG_FILE_PATH も読み込む
dotenv_path = Path("src/.env")  # 環境変数ファイルのパス
# dotenv_path = Path(__file__).parents[1] / ".env"
load_dotenv(dotenv_path=dotenv_path)
mode = os.getenv("MODE", "DEV")
# resources_path = os.getenv(
#     "RESOURCES_PATH", "src/resources"
# )  # リソースディレクトリのパス
# utils_path = os.getenv(
#     "UTILS_PATH", "src/utils"
# )  # ユーティリティディレクトリのパス
# templates_path = os.getenv(
#     "TEMPLATES_PATH", "src/templates"
# )  # テンプレートディレクトリのパス
# deploy_path = os.getenv("DEPLOY_PATH", "src")  # デプロイディレクトリのパス

# # 生成されたファイルのパス
# generated_path = os.getenv(
#     "GENERATED_PATH", "src/generated"
# )  # 生成されたファイルのパス


@dataclass(frozen=True)
class Settings:

    @dataclass(frozen=True)
    class ConnectionSettings:
        snowflake_account: str
        snowflake_user: str
        snowflake_role: str
        snowflake_warehouse: str
        snowflake_database: str
        snowflake_schema: str
        snowflake_private_key_path: str
        # snowflake_private_key_passphrase: str = None

    @dataclass(frozen=True)
    class LoggingSettings:
        log_dir: str
        log_max_bytes: int
        log_backup_count: int
        log_min_level: str
        log_file: str
        log_format: str

    connection: ConnectionSettings
    logging: LoggingSettings
    # resources_path: str = resources_path
    # utils_path: str = utils_path
    # templates_path: str = templates_path
    # deploy_path: str = deploy_path
    # generated_path: str = generated_path


def load_config(mode: str = "DEV") -> Settings:
    # 1) まず環境変数から設定ファイルパスを取得
    cfg_env = os.getenv("CONFIG_FILE_PATH")
    if cfg_env:
        cfg_path = Path(cfg_env)
    else:
        # 環境変数がセットされていなければ従来のデフォルト位置を使う
        cfg_path = Path(__file__).parents[1] / "resources" / "config.yml"

    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")

    # 2) YAML 読み込み
    with cfg_path.open(encoding="utf-8") as f:
        full_cfg = yaml.safe_load(f)

    # 3) 環境（dev|stg|prd）ごとのセクションを取り出し
    # env = os.getenv("MODE", mode)
    env_cfg = full_cfg.get(mode)
    if env_cfg is None:
        raise ValueError(f"Unknown MODE: {mode}")

    # 4) 接続設定とロギング設定を分けて読み込む
    connection_cfg = env_cfg.get("CONNECTION", {})
    logging_cfg = env_cfg.get("LOGGING", {})

    # 小文字キーのみで上書き
    def env_or_default(key, env_name=None, default=None, cast=None):
        env_val = os.getenv(env_name or key.upper())
        if env_val is not None and env_val != "":
            return cast(env_val) if cast else env_val
        return default

    # 必ず小文字キーでセット
    for key, env_name in [
        ("snowflake_account", "SNOWFLAKE_ACCOUNT"),
        ("snowflake_user", "SNOWFLAKE_USER"),
        ("snowflake_role", "SNOWFLAKE_ROLE"),
        ("snowflake_warehouse", "SNOWFLAKE_WAREHOUSE"),
        ("snowflake_database", "SNOWFLAKE_DATABASE"),
        ("snowflake_schema", "SNOWFLAKE_SCHEMA"),
        ("snowflake_private_key_path", "SNOWFLAKE_PRIVATE_KEY_PATH"),
    ]:
        connection_cfg[key] = env_or_default(
            key, env_name, connection_cfg.get(key)
        )

    connection_cfg["snowflake_warehouse"] = "hoge"
    connection_cfg["snowflake_database"] = "fuga"
    connection_cfg["snowflake_schema"] = "piyo"

    # 不要な大文字キーを削除
    for k in list(connection_cfg.keys()):
        if k.isupper():
            del connection_cfg[k]

    # logging_cfgも同様に小文字キーのみでセット

    logging_cfg_dict = dict()

    logging_cfg_dict["log_dir"] = env_or_default(
        "log_dir", "LOG_DIR", logging_cfg.get("LOG_DIR")
    )
    logging_cfg_dict["log_max_bytes"] = env_or_default(
        "log_max_bytes",
        "LOG_MAX_BYTES",
        logging_cfg.get("LOG_MAX_BYTES"),
        cast=int,
    )
    logging_cfg_dict["log_backup_count"] = env_or_default(
        "log_backup_count",
        "LOG_BACKUP_COUNT",
        logging_cfg.get("LOG_BACKUP_COUNT"),
        cast=int,
    )
    logging_cfg_dict["log_min_level"] = env_or_default(
        "log_min_level", "LOG_MIN_LEVEL", logging_cfg.get("LOG_MIN_LEVEL")
    )
    logging_cfg_dict["log_file"] = env_or_default(
        "log_file", "LOG_FILE", logging_cfg.get("LOG_FILE")
    )
    logging_cfg_dict["log_format"] = env_or_default(
        "log_format", "LOG_FORMAT", logging_cfg.get("LOG_FORMAT")
    )

    env_cfg["connection"] = connection_cfg
    env_cfg["logging"] = logging_cfg_dict

    # 必須値チェック
    required_keys = [
        "snowflake_account",
        "snowflake_user",
        "snowflake_role",
        "snowflake_warehouse",
        "snowflake_database",
        "snowflake_schema",
        "snowflake_private_key_path",
    ]
    for k in required_keys:
        if not connection_cfg.get(k):
            raise ValueError(
                f"connectionの必須項目 {k} が未設定です: {connection_cfg}"
            )

    return Settings(
        connection=Settings.ConnectionSettings(**connection_cfg),
        logging=Settings.LoggingSettings(**logging_cfg_dict),
    )


# 一度だけロード
settings = load_config(mode=mode)
db_schema_map_template_rendering(
    # settings=settings
)  # DBスキーマのレンダリングを実行
