#Script for testing DQ checks
import pandas as pd
import pytest
from sqlalchemy import create_engine
import oracledb
import logging

from sqlalchemy.testing import fixture

from CommonUtils.utils import Verify_expected_file_data_vs_actual_stag_table_data, \
    Verify_expected_source_table_data_vs_actual_stag_table_data, \
    check_file_exists, check_file_size, check_for_duplicate_across_the_columns, check_for_duplicate_for_specific_column, \
    check_for_null_values, \
    check_table_count, check_table_duplicates, check_table_null, oracle_engine, mysql_engine
from Configuration.config import *

logging.basicConfig(
    filename='LogFiles/Data_Quality.log',
    filemode = 'a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

#DQ test cases for source files(json and csv)
@pytest.mark.smoke
@pytest.mark.regression
def test_DataQuality_supplier_data_file_availability():
    logger.info(f"Test case execution for supplier_data_file availability check initiated....")
    try:
       assert check_file_exists("TestData/supplier_data.json"),"supplier_data_file does not exist at the location"
    except Exception as e:
        logger.error(f"Error while checking the supplier_data_file existance {e}")
        pytest.fail("Test case execution for supplier_data_file availability check has failed")
    logger.info(f"Test case execution supplier_data_file availability check  has completed....")

@pytest.mark.smoke
@pytest.mark.regression
def test_DataQuality_supplier_data_file_size():
    logger.info(f"Test case execution for supplier_data_file size check initiated....")
    try:
       assert check_file_size("TestData/supplier_data.json"),"supplier_data_file is zero byte"
    except Exception as e:
        logger.error(f"Error while checking the supplier_data_file size {e}")
        pytest.fail("Test case execution for supplier_data_file size check has failed")
    logger.info(f"Test case execution supplier_data_file size check  has completed....")

def test_DataQuality_supplier_data_duplicate_across_the_columns():
    logger.info(f"Test case execution for supplier_data_file duplicate_across_the_columns check initiated....")
    try:
       assert check_for_duplicate_across_the_columns("TestData/supplier_data.json","json"),"supplier_data_file having duplicate records"
    except Exception as e:
        logger.error(f"Error while checking the supplier_data_file duplicate_across_the_columns {e}")
        pytest.fail("Test case execution for supplier_data_file duplicate_across_the_columns check has failed")
    logger.info(f"Test case execution supplier_data_file duplicate_across_the_columns check  has completed....")

def test_DataQuality_supplier_data_duplicate_for_specific_column():
    logger.info(f"Test case execution for supplier_data_file duplicate_for_specific_column check initiated....")
    try:
       assert check_for_duplicate_for_specific_column("TestData/supplier_data.json","json","supplier_id"),"supplier_data_file having duplicate records"
    except Exception as e:
        logger.error(f"Error while checking the supplier_data_file duplicate_for_specific_column {e}")
        pytest.fail("Test case execution for supplier_data_file duplicate_for_specific_column check has failed")
    logger.info(f"Test case execution supplier_data_file duplicate_for_specific_column check  has completed....")

def test_DataQuality_supplier_data_null_values():
    logger.info(f"Test case execution for supplier_data_file null_values check initiated....")
    try:
       assert check_for_null_values("TestData/supplier_data.json","json"),"supplier_data_file having null_values"
    except Exception as e:
        logger.error(f"Error while checking the supplier_data_file null_values {e}")
        pytest.fail("Test case execution for supplier_data_file null_values check has failed")
    logger.info(f"Test case execution supplier_data_file null_values check  has completed....")

def test_DataQuality_product_data_file_availability():
    logger.info(f"Test case execution for product_data_file availability check initiated....")
    try:
       assert check_file_exists("TestData/product_data.csv"),"product_data_file does not exist at the location"
    except Exception as e:
        logger.error(f"Error while checking the product_data_file existance {e}")
        pytest.fail("Test case execution for product_data_file availability check has failed")
    logger.info(f"Test case execution product_data_file availability check  has completed....")

def test_DataQuality_product_data_file_size():
    logger.info(f"Test case execution for product_data_file size check initiated....")
    try:
       assert check_file_size("TestData/product_data.csv"),"product_data_file is zero byte"
    except Exception as e:
        logger.error(f"Error while checking the product_data_file size {e}")
        pytest.fail("Test case execution for product_data_file size check has failed")
    logger.info(f"Test case execution product_data_file size check  has completed....")

def test_DataQuality_product_data_duplicate_across_the_columns():
    logger.info(f"Test case execution for product_data_file duplicate_across_the_columns check initiated....")
    try:
       assert check_for_duplicate_across_the_columns("TestData/product_data.csv","csv"),"product_data_file having duplicate records"
    except Exception as e:
        logger.error(f"Error while checking the product_data_file duplicate_across_the_columns {e}")
        pytest.fail("Test case execution for product_data_file duplicate_across_the_columns check has failed")
    logger.info(f"Test case execution product_data_file duplicate_across_the_columns check  has completed....")

def test_DataQuality_product_data_duplicate_for_specific_column():
    logger.info(f"Test case execution for product_data_file duplicate_for_specific_column check initiated....")
    try:
       assert check_for_duplicate_for_specific_column("TestData/product_data.csv","csv","product_id"),"product_data_file having duplicate records"
    except Exception as e:
        logger.error(f"Error while checking the product_data_file duplicate_for_specific_column {e}")
        pytest.fail("Test case execution for product_data_file duplicate_for_specific_column check has failed")
    logger.info(f"Test case execution product_data_file duplicate_for_specific_column check  has completed....")

def test_DataQuality_product_data_null_values():
    logger.info(f"Test case execution for product_data_file null_values check initiated....")
    try:
       assert check_for_null_values("TestData/product_data.csv","csv"),"product_data_file having null_values"
    except Exception as e:
        logger.error(f"Error while checking the product_data_file null_values {e}")
        pytest.fail("Test case execution for product_data_file null_values check has failed")
    logger.info(f"Test case execution product_data_file null_values check  has completed....")

#DQ test cases for source table(oracle)
def test_DataQuality_stores_table_data_count(connect_to_oracle_database):
    logger.info(f"Test case execution for stores_table_data_count check initiated....")
    try:
       query_expected = """select * from stores"""
       assert check_table_count(query_expected,oracle_engine),"stores_table_data_count is zero"
    except Exception as e:
        logger.error(f"Error while checking the stores_table_data_count {e}")
        pytest.fail("Test case execution for stores_table_data_count check has failed")
    logger.info(f"Test case execution stores_table_data_count check  has completed....")
    logger.info(f"Fetched {len(query_expected)} rows from query.")

def test_DataQuality_stores_table_data_duplicates(connect_to_oracle_database):
    logger.info(f"Test case execution for stores_table_data_duplicates check initiated....")
    try:
       query_expected = """select * from stores"""
       assert check_table_duplicates(query_expected,oracle_engine),"stores_table having duplicates data"
    except Exception as e:
        logger.error(f"Error while checking the stores_table_data_duplicates {e}")
        pytest.fail("Test case execution for stores_table_data_duplicates check has failed")
    logger.info(f"Test case execution stores_table_data_duplicates check  has completed....")

def test_DataQuality_stores_table_data_null(connect_to_oracle_database):
    logger.info(f"Test case execution for stores_table_data_null check initiated....")
    try:
       query_expected = """select * from stores"""
       assert check_table_null(query_expected,oracle_engine),"stores_table having null data"
    except Exception as e:
        logger.error(f"Error while checking the stores_table_data_null {e}")
        pytest.fail("Test case execution for stores_table_data_null check has failed")
    logger.info(f"Test case execution stores_table_data_null check  has completed....")



