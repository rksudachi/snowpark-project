import sys
from jinja2 import Environment, FileSystemLoader, Template
from snowflake.snowpark import DataFrame as Df
import inspect

from typing import Callable, Any


def proc_x_impl():
    temp = "{{DB_NAME}}.{{SCHEMA_NAME}}.{{TABLE_NAME}}"
    print(f"テンプレート変数の値: {temp}")
    print(f"{temp}のproc_a_implが実行されました。")
    return temp


def create_proc_x(func):
    print(func())


def render_template_x(func, variables):
    """
    指定されたファイルパスのテンプレートを変数でレンダリングします。

    Args:
        file_path (str): テンプレートファイルのパス。
        variables (dict): テンプレートに渡す変数。

    Returns:
        str: レンダリングされたテンプレートの内容。
    """
    # env = Environment(loader=FileSystemLoader(searchpath="./"))
    func_str = inspect.getsource(func)
    template = Template(func_str)
    return func.__name__, template.render(variables)


def render_template(template_path, variables):
    env = Environment(loader=FileSystemLoader(searchpath="./"))
    template = env.get_template(template_path)
    return template.render(variables)


def deploy_procedure(file_path, sproc_name, code) -> Callable[..., Any]:
    """
    指定されたファイルパスからSprocをデプロイします。

    Args:
        file_path (str): デプロイするSprocのファイルパス。
        sproc_name (str): デプロイするSprocの名前。

    Returns:
        str: デプロイされたSprocのファイルパス。
    """
    # import importlib.util

    # spec = importlib.util.spec_from_file_location(sproc_name, file_path)
    # user_func = importlib.util.module_from_spec(spec)
    # spec.loader.exec_module(user_func)
    # # spec.loader.exec_module(user_func)
    # sproc_func = getattr(user_func, sproc_name, None)
    # hoge = sproc_func()
    import types

    module = types.ModuleType("temp_module")
    exec(code, module.__dict__)
    sproc_func = None
    sproc_func = module.__dict__["proc_a_impl"]  # sprocデコレータ済み関数
    # for v in module.__dict__.values():
    #     if hasattr(v, "proc_a_impl"):  # sprocデコレータ済み関数
    #         sproc_func = v
    #         break
    # if sproc_func is None:
    #     raise Exception("No sproc function found!")
    print(type(sproc_func))
    hoge = sproc_func()
    print(f"{sproc_name}の実行結果: {hoge}")
    # print(f"Deployed {sproc_name} from {file_path}")
    return sproc_func


def main():

    hoge = {"hoge": "1", "piyo": "2", "fuga": 3}

    for k, v in hoge.items():
        print(f"{k=},{v=}")

    df = Df(hoge)
    print(df)


def prt(hoge="hoge"):
    print(f"{hoge}が呼ばれました。")


if __name__ == "__main__":
    # if len(sys.argv) != 3:
    #     print("Usage: python deploy_proc.py <file_path> <sproc_name>")
    #     sys.exit(1)

    # file_path = sys.argv[1]
    # sproc_name = sys.argv[2]

    # main()

    file_path = "src/database/depart_database/hoge/procedures/proc_a.py"
    sproc_name = "proc_a_impl"
    create_sproc = "create_proc_a"
    database = "depart_database"
    schema = "hoge"
    table = "proc_a_table"
    variables = {
        "DB_NAME": database,
        "SCHEMA_NAME": schema,
        "TABLE_NAME": table,
    }
    code = render_template(file_path, variables)

    codex_name, codex = render_template_x(proc_x_impl, variables)
    print(f"Rendered code: {codex_name}, {codex}")
    import importlib.util
    import os

    func_x_namespace = {}
    exec(codex, func_x_namespace)
    func_x = func_x_namespace[codex_name]

    module_name = os.path.splitext(os.path.basename(file_path))[0]
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # 関数を取得して呼び出し
    func = getattr(module, create_sproc)
    sproc = deploy_procedure(file_path, sproc_name, code)
    func(sproc)
