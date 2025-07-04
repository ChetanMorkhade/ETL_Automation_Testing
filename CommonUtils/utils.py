# Utility Functions for ETL validations

import pandas as pd
from sqlalchemy import create_engine
import oracledb
import logging

from Configuration.config import *    #importing all database configurations from config file

oracledb.init_oracle_client(lib_dir=r"C:\oracle\instantclient_19_11\instantclient-basic-windows.x64-23.8.0.25.04\instantclient_23_8")  #to support with oracle sql developer
oracle_engine = create_engine(f"oracle+oracledb://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_DATABASE}")
mysql_engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE_STAGING}")

logging.basicConfig(
    filename='LogFiles/Target_Loading.log',
    filemode = 'a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

#To read diff files and validation
def Verify_expected_file_data_vs_actual_stag_table_data(file_path,file_type,db_query,db_engine):
    if file_type =="csv":
        df_expected = pd.read_csv(file_path)
    elif file_type =="json":
        df_expected = pd.read_json(file_path)
    elif file_type =="xml":
        df_expected = pd.read_xml(file_path,xpath=".//item")
    else:
        raise ValueError(f"Unsupported file passed as input parameter{file_type}")
        logger.info(f"The expected data in the file is :{df_expected}")
        df_actual = pd.read_sql(db_query,db_engine)
        logger.info(f"The actual data in the stag table is :{df_actual}")
        assert df_actual.equals(df_expected),f"expected data in {file_path} does not match the actual data from stag table {db_query}"

#To read database and validation
def Verify_expected_source_table_data_vs_actual_stag_table_data(query_expected,db_engine_expected,query_actual,db_engine_actual):
    df_expected = pd.read_sql(query_expected, db_engine_expected)
    logger.info(f"The expected data in the source table is :{df_expected}")
    df_actual = pd.read_sql(query_actual,db_engine_actual)
    logger.info(f"The actual data in the stag table is :{df_actual}")
    assert df_actual.equals(df_expected),f"expected data from source table {query_expected} does not match the actual data from stag table {query_actual}"
