import os
import oracledb
from dotenv import load_dotenv, find_dotenv

import table_detail as td

_ = load_dotenv(find_dotenv())
oracledb.init_oracle_client()

UN = os.environ.get("UN")
PW = os.environ.get("PW")
DSN = os.environ.get("DSN")

def save_texts():
    print("Start Insert Data")
    try:
        with oracledb.connect(user=UN, password=PW, dsn=DSN) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    DELETE from {td.source_table}
                """)
                sample_data = [("Product " + str(i), "Product Code " + str(i) ,"Search Keyword " + str(i)) for i in range(1, 100001)]
                insert_sql = f"""
                    INSERT INTO {td.source_table} ({td.name_column}, {td.code_column}, {td.source_column})
                    VALUES (:1, :2, :3)
                """
                cursor.setinputsizes(None, oracledb.STRING)
                cursor.executemany(insert_sql, sample_data)
            connection.commit()
            print("End Insert Data")
    except Exception as e:
        print("Error Insert Data:", e)

if __name__ == "__main__":
    
    save_texts()
