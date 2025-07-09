# Utility Functions for ETL validations
import os.path #used while testing file existance at path
import boto3  #used when interact with AWS like S3
from io import StringIO  #used while testing file from cloud and simulating input/output like object
import pandas as pd
from sqlalchemy import create_engine
import oracledb
import logging

from Configuration.config import *    #importing all database configurations from config file

oracledb.init_oracle_client(lib_dir=r"C:\oracle\instantclient_19_11\instantclient-basic-windows.x64-23.8.0.25.04\instantclient_23_8")  #to support with oracle sql developer
oracle_engine = create_engine(f"oracle+oracledb://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_DATABASE}")
mysql_engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE_STAGING}")

logging.basicConfig(
    filename='LogFiles/Data_Quality.log',
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
    df_expected = pd.read_sql(query_expected, db_engine_expected).astype(str)   #.astype(str) is used to convert all datatypes to string in case of diff datatypes for source and target
    logger.info(f"The expected data in the source table is :{df_expected}")
    df_actual = pd.read_sql(query_actual,db_engine_actual).astype(str)
    logger.info(f"The actual data in the stag table is :{df_actual}")
    assert df_actual.equals(df_expected),f"expected data from source table {query_expected} does not match the actual data from stag table {query_actual}"

'''
#To get data from linux server and read
def getDataFromLinuxBox():
    try:
        logger.info("Linux  connection is being establish")
        # connect to ssh
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        # to conenct to linux server
        ssh_client.connect(hostname,username=username,password=password)
        sftp = ssh_client.open_sftp()
        # download the file from linux server to local
        sftp.get(remote_file_path,local_file_path)
        logger.info("The file from Linux is downloaded to local")
    except Exception as e:
        logger.error(f"Error while connecting Linux {e}")


# Initialize a session using Boto3
s3 = boto3.client('s3')
# Read the file from S3 and return dataframe
def read_csv_from_s3(bucket_name, file_key):
    try:
        # Fetch the CSV file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        # Read the content of the file and load it into a Pandas DataFrame
        csv_content = response['Body'].read().decode('utf-8')  # Decode content to string
        data = StringIO(csv_content)  # Use StringIO to simulate a file-like object
        # Read the CSV data into a Pandas DataFrame
        df = pd.read_csv(data)
        # Return the DataFrame
        return df
    except Exception as e:
        print(f"Error reading file from S3: {e}")
        return None
#Read source file from S3 and compare with target table 
def verify_expected_as_S3_to_actual_as_db(bucket_name,file_key,db_engine_actual,query_actual):
    # The desired path and file name in the S3 bucket
    # Call the function to read the CSV file from S3
    df_expected = read_csv_from_s3(bucket_name, file_key)
    logger.info(f"The expected data is the database is: {df_expected}")
    df_actual = pd.read_sql(query_actual, db_engine_actual)
    logger.info(f"The actual data is the database is: {df_actual}")
    assert df_actual.equals(df_expected), f"expected does not match with expected data in{query_actual}"
'''
# data quality(DQ) checks related functions
def check_file_exists(file_path):
    try:
        if os.path.isfile(file_path):
            return True
        else:
            return False
    except  Exception as e:
        logger.error(f"File :{file_path} does not exists{e}")

def check_file_size(file_path):
    try:
        if os.path.getsize(file_path) != 0:
            return True
        else:
            return False
    except  Exception as e:
        logger.error(f"File :{file_path} is zero byte file{e}")

def check_for_duplicate_across_the_columns(file_path,file_type):
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        elif file_type == "xml":
            df = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"unsupported file passed as input parameter{file_type}")

        logger.info(f"The expected data in the file is :{df}")
        if df.duplicated().any():
            return False
        else:
            return True
    except Exception as e:
        logger.error(f"Error while reading the file {file_path}:{e}")

def check_for_duplicate_for_specific_column(file_path, file_type,column_name):
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        elif file_type == "xml":
            df = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"unsupported file passed as input parameter{file_type}")
        logger.info(f"The expected data in the file is :{df}")
        if df[column_name].duplicated().any():
            return False
        else:
            return True
    except Exception as e:
        logger.error(f"Error while reading the file {file_path}:{e}")

def check_for_null_values(file_path, file_type):
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        elif file_type == "xml":
            df = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"unsupported file passed as input parameter{file_type}")
        logger.info(f"The expected data in the file is :{df}")
        if df.isnull().values.any():
            return False
        else:
            return True
    except Exception as e:
        logger.error(f"Error while reading the file {file_path}:{e}")

def check_table_count(query_expected, db_engine_expected):
    try:
        df = pd.read_sql(query_expected, db_engine_expected)
        return not df.empty
    except Exception as e:
        logger.error(f"Table does not have data: {e}")
        return False

def check_table_duplicates(query_expected, db_engine_expected):
    try:
        df = pd.read_sql(query_expected, db_engine_expected)
        return not df.duplicated().any()
    except Exception as e:
        logger.error(f"Table having duplicate data: {e}")
        return False

def check_table_null(query_expected, db_engine_expected):
    try:
        df = pd.read_sql(query_expected, db_engine_expected)
        return not df.isnull().values.any()
    except Exception as e:
        logger.error(f"Table having null data: {e}")
        return False