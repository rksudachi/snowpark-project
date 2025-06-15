import os
from pathlib import Path
import yaml
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

# .env から CONFIG_FILE_PATH も読み込む
dotenv_path = Path(__file__).parents[1] / ".env"
load_dotenv(dotenv_path=dotenv_path)
mode = os.getenv("MODE", "DEV")


class Settings(BaseSettings):
    class ConnectionSettings(BaseSettings):
        snowflake_account: str
        snowflake_user: str
        snowflake_role: str
        snowflake_warehouse: str
        snowflake_database: str
        snowflake_schema: str
        snowflake_private_key_path: str
        # snowflake_private_key_passphrase: str = None

    class LoggingSettings(BaseSettings):
        log_dir: str
        log_max_bytes: int
        log_backup_count: int
        log_min_level: str
        log_file: str
        log_format: str


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

    # 不要な大文字キーを削除
    for k in list(connection_cfg.keys()):
        if k.isupper():
            del connection_cfg[k]

    # logging_cfgも同様に小文字キーのみでセット

    logging_cfg["log_dir"] = env_or_default(
        "log_dir", "LOG_DIR", logging_cfg.get("log_dir")
    )
    logging_cfg["log_max_bytes"] = env_or_default(
        "log_max_bytes",
        "LOG_MAX_BYTES",
        logging_cfg.get("log_max_bytes"),
        cast=int,
    )
    logging_cfg["log_backup_count"] = env_or_default(
        "log_backup_count",
        "LOG_BACKUP_COUNT",
        logging_cfg.get("log_backup_count"),
        cast=int,
    )
    logging_cfg["log_min_level"] = env_or_default(
        "log_min_level", "LOG_MIN_LEVEL", logging_cfg.get("log_min_level")
    )
    logging_cfg["log_file"] = env_or_default(
        "log_file", "LOG_FILE", logging_cfg.get("log_file")
    )
    logging_cfg["log_format"] = env_or_default(
        "log_format", "LOG_FORMAT", logging_cfg.get("log_format")
    )

    env_cfg["connection"] = connection_cfg
    env_cfg["logging"] = logging_cfg

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
        connection=Settings.ConnectionSettings(**env_cfg["connection"]),
        logging=Settings.LoggingSettings(**env_cfg["logging"]),
    )


# 一度だけロード
settings = load_config(mode=mode)
