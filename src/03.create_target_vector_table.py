import os
import oracledb
from dotenv import load_dotenv, find_dotenv

import table_detail as td

_ = load_dotenv(find_dotenv())
oracledb.init_oracle_client()

UN = os.environ.get("UN")
PW = os.environ.get("PW")
DSN = os.environ.get("DSN")

def add_vector_column():
    try:
        with oracledb.connect(user=UN, password=PW, dsn=DSN) as connection:
            with connection.cursor() as cursor:
                create_table_sql = f"""
                    CREATE TABLE {td.target_table}
                    (
                        {td.name_column} VARCHAR2(100 BYTE),
                        {td.code_column} VARCHAR2(100 BYTE),
                        {td.source_column} VARCHAR2(100 BYTE),
                        {td.vector_column} VECTOR
                    )
                """
                cursor.execute(create_table_sql)
            connection.commit()
            print(f"Create VECTOR type Table.")
    except Exception as e:
        print("Error creating vector table:", e)
        
if __name__ == "__main__":
    add_vector_column()
