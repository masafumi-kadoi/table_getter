import cx_Oracle
import json
import os

HOST = "localhost"
PORT = 1521
SVC_NM = "hoge.example.com"
USER = "USER"
PASS = "PASS"
TABLE_LIST = ["A", "B"]

ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
DUMP_DIR = os.path.join(ROOT_DIR, "dump")


def write_json(data_dict: dict | list, write_dir_path: str, file_name: str) -> None:
    os.makedirs(write_dir_path, exist_ok=True)
    with open(os.path.join(write_dir_path, file_name), 'w+') as f:
        json.dump(data_dict, f, ensure_ascii=False, indent=4)


# 接続記述子の生成
dsn = cx_Oracle.makedsn(HOST, PORT, service_name = SVC_NM)

# コネクションの確立
with cx_Oracle.connect(USER, PASS, dsn, encoding = "UTF-8") as connection:
    # カーソル生成
    with connection.cursor() as cursor:

        for table_name in TABLE_LIST:
            sql = F"select * from {table_name}"

            # SQL発行
            cursor.execute(sql)

            # データ取得
            rows = cursor.fetchall()
            write_json(rows, DUMP_DIR, F"{table_name}.json")
