import yaml
from jinja2 import Environment, FileSystemLoader


def db_schema_map_template_rendering():
    """
    Jinja2テンプレートを使用してDBスキーマのレンダリング結果を生成する関数
    """

    template_render_file = "db_schema_map.py"
    template_file = f"{template_render_file}.j2"

    # conf.ymlを読み込む
    with open(f"src/resources/config.yml", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # DBとSCHEMASから自動でレンダリング用辞書を生成
    render_dict = {}
    # mode
    mode = config["DEV"]

    # DB部分
    for db_key, db_val in mode["DB"].items():
        render_dict[db_key] = db_val
    # SCHEMAS部分（DBごとにスキーマを展開）
    for db_key, schemas in mode["SCHEMAS"].items():
        for schema_key, schema_val in schemas.items():
            # 例: "ANALYTICS_PUBLIC": "PUBLIC_DEV"
            render_dict[f"{db_key}_{schema_key}"] = schema_val
            # 例: "PUBLIC": "PUBLIC_DEV"（単純名も必要なら上書きされるが追加）
            render_dict[schema_key] = schema_val

    # Jinja2テンプレートを読み込む
    env = Environment(
        loader=FileSystemLoader("src/templates")
    )  # テンプレートファイルと同じディレクトリ
    template = env.get_template(f"{template_file}")

    # テンプレートをレンダリング
    rendered = template.render(**render_dict)

    # Pythonファイルとして出力
    with open(
        f"src/generated/{template_render_file}",
        "w",
        encoding="utf-8",
    ) as f:
        f.write(rendered)


if __name__ == "__main__":
    db_schema_map_template_rendering()
    print("DBスキーマのレンダリング結果を生成しました。")
