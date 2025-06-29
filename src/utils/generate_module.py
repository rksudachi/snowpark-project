import importlib.util
import os


def generate_module(module_path: str, class_name: str):
    """
    指定したパスにモジュールが存在すればimportし、クラスからインスタンスを生成する
    """
    if not os.path.exists(module_path):
        raise FileNotFoundError(f"モジュールが見つかりません: {module_path}")

    module_name = os.path.splitext(os.path.basename(module_path))[0]
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    cls = getattr(module, class_name)
    return cls
