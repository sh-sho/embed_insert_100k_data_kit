import os
import sys
import oracledb
import array
from dotenv import load_dotenv, find_dotenv

import utils
import table_detail as td

_ = load_dotenv(find_dotenv())
oracledb.init_oracle_client()

UN = os.environ.get("UN")
PW = os.environ.get("PW")
DSN = os.environ.get("DSN")

def query_text(text: str):
    embed_text = array.array('f', utils.embed_documents([text])[0])

    print(f"Start Vector Search")
    try:
        with oracledb.connect(user=UN, password=PW, dsn=DSN) as connection:
            with connection.cursor() as cursor:
                cursor.setinputsizes(oracledb.DB_TYPE_VECTOR)
                select_sql = f"""
                    SELECT
                        {td.name_column},
                        {td.source_column},
                        VECTOR_DISTANCE({td.vector_column}, :1, COSINE) as distance
                    FROM
                        {td.target_table}
                    ORDER BY distance
                    FETCH FIRST 3 ROWS ONLY
                """
                cursor.execute(select_sql, [embed_text])

                print(f"============Results============")
                index = 1
                for row in cursor:
                    print(f"{index}: {row}")
                    index += 1

            connection.commit()
    except Exception as e:
        print("Error Vector Search:", e)
        
    print(f"End Vector Search")


if __name__ == "__main__":
    args = sys.argv
    query = args[1]
    print(f"Search Text：{query}")
    query_text(query)
