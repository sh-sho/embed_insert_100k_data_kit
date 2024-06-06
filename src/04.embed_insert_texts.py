import os
import oci
import sys
import oracledb
from dotenv import load_dotenv, find_dotenv
import multiprocessing as mp
import time
import csv
from functools import wraps
import shutil
from typing import Any, Callable, TypeVar
import warnings
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import glob

import utils
import table_detail as td

warnings.simplefilter('ignore', UserWarning)
warnings.simplefilter('ignore', FutureWarning)

F = TypeVar('F', bound=Callable[..., Any])
_ = load_dotenv(find_dotenv())
oracledb.init_oracle_client()

UN = os.environ.get("UN")
PW = os.environ.get("PW")
DSN = os.environ.get("DSN")

OCI_COMPARTMENT_ID = os.environ["OCI_COMPARTMENT_ID"]
CSV_DIRECTORY_PATH = os.environ["CSV_DIRECTORY_PATH"]

NO_OF_PROCESSORS = mp.cpu_count()
BATCH_SIZE = 96

pool = oracledb.create_pool(user=UN, password=PW, dsn=DSN, min=NO_OF_PROCESSORS, max=NO_OF_PROCESSORS)
def timer(func: F) -> None:
    """Any functions wrapper for calculate execution time"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start
        print(f"{func.__name__} took a {elapsed_time}s")
        return result
    return wrapper

def auth_check() -> None:
    config = oci.config.from_file(file_location=os.environ["OCI_CONFIG_FILE"])
    compute_client = oci.core.ComputeClient(config)
    auth_response = compute_client.list_instances(OCI_COMPARTMENT_ID)
    if auth_response.status != 200:
        print(f"Auth Failed\n Error Status:{auth_response.status}")
        sys.exit
    else:
        print(f"Auth Success\n Status:{auth_response.status}")

def csv_dir_check() -> None:
    if os.path.exists(CSV_DIRECTORY_PATH):
        try:
            delete_csv_files(CSV_DIRECTORY_PATH)
            print("success delete csv")
        except FileNotFoundError:
            print("no target csv")
        except Exception as e:
            print("Error delete csv", e)
    else:
        try:
            os.makedirs(CSV_DIRECTORY_PATH, exist_ok=True)
        except Exception as e:
            print("Error make directory ", e)

def delete_csv_files(csv_directory: str) -> None:
    files = os.listdir(csv_directory)
    try:
        for file in files:
            if file.endswith(".csv"):
                os.remove(os.path.join(csv_directory, file))
    except FileNotFoundError:
        print("no target csv")
    except Exception as e:
        print("Error delete csv", e)

@timer
def extract_all_data() -> np.ndarray:
    """Load all data from source table"""
    with pool.acquire() as connection:
        with connection.cursor():
            engine = create_engine("oracle+oracledb://", creator=lambda: connection)
            df = pd.read_sql(f"SELECT * FROM {td.source_table}", engine)
            if df.size == 0:
                print(f"{td.source_table} doesn't contain any data.")
                exit(1)
            batches = np.array_split(df, len(df)//BATCH_SIZE + 1)
            print(f"Fetched all data: {len(df)}")
            return batches

@timer
def embed_texts(texts: np.ndarray) -> list:
    """Embed texts"""
    texts_vector = utils.embed_documents(texts)
    return texts_vector

@timer
def to_csv(batch: np.ndarray) -> None:
    """Dump data to CSV"""
    pd.DataFrame(batch).to_csv(f"{CSV_DIRECTORY_PATH}/insert-data-{time.time()}.csv", header=False, index=False)
    
@timer
def transform_and_dump_to_csv(batch: np.ndarray) -> None:
    """Transform and load data to sink table"""
    response = embed_texts(batch[str.lower(td.source_column)].tolist())
    batch[td.vector_column] = response
    to_csv(batch=batch)

@timer
def finalizer(exception: Exception, cursor: oracledb.Cursor, connection: oracledb.Connection) -> None:
    """Close some resources(cursor, connection) and exit this task."""
    print(exception)
    print(f"Finalize these resources. cursor: {cursor}, connection: {connection}")
    cursor.close()
    connection.close()
    exit(1)

@timer
def flush(data: list) -> None:
    """Flush the on-memory data"""
    with pool.acquire() as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.setinputsizes(None, oracledb.DB_TYPE_VECTOR)

            insert_sql = f"""
                INSERT INTO {td.target_table} ({td.name_column}, {td.code_column}, {td.source_column}, {td.vector_column}) 
                VALUES(:1, :2, :3, :4)
                """
            try:
                cursor.executemany(statement=insert_sql, parameters=data, batcherrors=True, arraydmlrowcounts=True)
                print(f"Insert rows: {len(cursor.getarraydmlrowcounts())}")
            except oracledb.DatabaseError as e:
                finalizer(exception=e, cursor=cursor, connection=connection)
            except KeyboardInterrupt as e:
                finalizer(exception=e, cursor=cursor, connection=connection)
@timer
def bulk_insert() -> None:
    """Bulk insert to sink table"""
    files = glob.glob(f"{CSV_DIRECTORY_PATH}/*.csv")
    insert_data = []
    for file in files:
        with open(file, "r") as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            for line in csv_reader:
                insert_data.append(tuple(line))
                if (len(insert_data)) >= 10_000:
                    flush(data=insert_data)
                    insert_data = []
    if insert_data:
        flush(data=insert_data)

def data_checks() -> None:
    """Check Embedded data"""
    with oracledb.connect(user=UN, password=PW, dsn=DSN) as conn:
        with conn.cursor() as cursor:
            check_sql = f"""
                SELECT {td.name_column}, {td.source_column}, {td.vector_column}
                FROM {td.target_table}
                WHERE ROWNUM <= 2
                ORDER BY {td.source_column} DESC
            """
            try:
                cursor.execute(check_sql)
                print(f"check data {cursor.fetchall()}")
            except Exception as e:
                print("Error connect db checksql", e)
        conn.close()

def delete_dir() -> None:
    """Delete csv directory"""
    if os.path.exists(CSV_DIRECTORY_PATH):
        try:
            shutil.rmtree(CSV_DIRECTORY_PATH)
            print("success delete csv directory")
        except Exception as e:
            print("Error delete directory", e)

if __name__ == "__main__":
    start_time = time.time()
    auth_check()
    csv_dir_check()
    batches = extract_all_data()
    
    with mp.Pool(NO_OF_PROCESSORS) as mappool:
        mappool.starmap(transform_and_dump_to_csv, zip(batches))
    
    bulk_insert()
    data_checks()
    end_time = time.time()
    execution_time = end_time - start_time
    print("Data vectorization process is complete.")
    print("Total Run Time ", execution_time)
    delete_dir()
