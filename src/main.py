import importlib
import inspect
import os
import sys
import importlib
from src.utils.config import settings
from src.utils.logger import Logger


# Sprocデコレータの定義例
def Sproc(func):
    func._is_sproc = True
    return func


def find_sproc_methods(module):
    sproc_methods = []
    for name, obj in inspect.getmembers(module):
        if inspect.isfunction(obj) and getattr(obj, "_is_sproc", False):
            sproc_methods.append((name, obj))
    return sproc_methods


def main():
    # ロガーの初期化
    logger = Logger(settings.logging).get_logger()
    logger.info("Sprocメソッド一覧を取得します。")

    # モジュールのインポート
    # モジュールのパスを追加
    module_path = os.path.join(
        os.path.dirname(__file__), "..", "customer", "procedures"
    )
    sys.path.append(module_path)

    # ログディレクトリの設定
    log_dir = settings.logging.log_dir
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 対象モジュール名を指定（例: sproc_module.py）
    module_name = "sproc_module"
    try:
        module = importlib.import_module(module_name)
    except ImportError:
        logger.error(f"Module '{module_name}' not found.")
        return

    sproc_methods = find_sproc_methods(module)
    if not sproc_methods:
        logger.info("No Sproc-decorated methods found.")
        return

    print("実行可能なSprocメソッド一覧:")
    for idx, (name, _) in enumerate(sproc_methods):
        print(f"{idx}: {name}")

    try:
        choice = int(input("実行したいメソッドの番号を入力してください: "))
        method = sproc_methods[choice][1]
        logger.info(f"Executing {method.__name__}...")
        result = method()
        logger.info(f"Result: {result}")
    except (ValueError, IndexError):
        logger.error("無効な選択です。")
    except Exception as e:
        logger.error(f"実行中にエラーが発生しました: {e}")


if __name__ == "__main__":
    main()
