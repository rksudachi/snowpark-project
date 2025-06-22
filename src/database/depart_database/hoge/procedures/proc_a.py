def proc_a_impl():
    temp = "{{DB_NAME}}.{{SCHEMA_NAME}}.{{TABLE_NAME}}"
    print(f"テンプレート変数の値: {temp}")
    print(f"{temp}のproc_a_implが実行されました。")
    return temp


def create_proc_a(func):
    print(func())
