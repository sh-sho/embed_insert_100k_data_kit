import os

import oracledb
from dotenv import load_dotenv, find_dotenv
import table_detail as td

_ = load_dotenv(find_dotenv())
oracledb.init_oracle_client()

UN = os.environ.get("UN")
PW = os.environ.get("PW")
DSN = os.environ.get("DSN")

if __name__ == "__main__":
    print("Start Creating Table")
    try:
        with oracledb.connect(user=UN, password=PW, dsn=DSN) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    CREATE TABLE {td.source_table}
                    (	
                        {td.name_column} VARCHAR2(100 BYTE),
                        {td.code_column} VARCHAR2(100 BYTE),
                        {td.source_column} VARCHAR2(100 BYTE)
                    )
                """)
            connection.commit()
            print("End Creating Table")
    except Exception as e:
        print("Error create_table:", e)
