#Test data Extraction Script
import pytest
import logging

#importing common utility functions from util.py file
from CommonUtils.utils import Verify_expected_file_data_vs_actual_stag_table_data, \
    Verify_expected_source_table_data_vs_actual_stag_table_data, mysql_engine

logging.basicConfig(
    filename='LogFiles/Extraction.log',
    filemode = 'a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

#Test cases for validation of data extraction for diff tables and files
def test_DataExtraction_from_sales_data_file_to_staging():
    logger.info(f"Test case execution for sales_data extraction has started....")
    try:
        query_actual = """select * from staging_sales"""
        Verify_expected_file_data_vs_actual_stag_table_data("TestData/sales_data_Linux_remote.csv","csv",query_actual,mysql_engine) #using from utility functions
    except Exception as e:
        logger.error(f"test case Execution for sales data extraction has failed{e}")
        pytest.fail("test case Execution for sales data extraction has failed")
    logger.info(f"Test case execution for sales_data extraction has completed....")

def test_DataExtraction_from_product_data_file_to_staging():
    logger.info(f"Test case execution for product_data extraction has started....")
    try:
        query_actual = """select * from staging_product"""
        Verify_expected_file_data_vs_actual_stag_table_data("TestData/product_data.csv","csv",query_actual,mysql_engine)  #using from utility functions
    except Exception as e:
        logger.error(f"test case Execution for product data extraction has failed{e}")
        pytest.fail("test case Execution for product data extraction has failed")
    logger.info(f"Test case execution for product_data extraction has completed....")

def test_DataExtraction_from_supplier_data_file_to_staging():
    logger.info(f"Test case execution for supplier_data extraction has started....")
    try:
        query_actual = """select * from staging_supplier"""
        Verify_expected_file_data_vs_actual_stag_table_data("TestData/supplier_data.json","json",query_actual,mysql_engine)  #using from utility functions
    except Exception as e:
        logger.error(f"test case Execution for supplier data extraction has failed{e}")
        pytest.fail("test case Execution for supplier data extraction has failed")
    logger.info(f"Test case execution for supplier_data extraction has completed....")

def test_DataExtraction_from_inventory_data_file_to_staging():
    logger.info(f"Test case execution for inventory_data extraction has started....")
    try:
        query_actual = """select * from staging_inventory"""
        Verify_expected_file_data_vs_actual_stag_table_data("TestData/inventory_data.xml","xml",query_actual,mysql_engine)  #using from utility functions
    except Exception as e:
        logger.error(f"test case Execution for inventory data extraction has failed{e}")
        pytest.fail("test case Execution for inventory data extraction has failed")
    logger.info(f"Test case execution for inventory_data extraction has completed....")

def test_DataExtraction_from_Oracle_stores_table_to_staging(connect_to_oracle_database,connect_to_mysql_database_staging):  #using DB connectors fron fixtures in conftest.py file
    logger.info(f"Test case execution for sales_data extraction has started....")
    try:
        query_expected = """select * from stores"""
        query_actual = """select * from staging_stores"""
        Verify_expected_source_table_data_vs_actual_stag_table_data(query_expected,connect_to_oracle_database,query_actual,connect_to_mysql_database_staging)  #using from utility functions
    except Exception as e:
        logger.error(f"test case Execution for stores data extraction has failed{e}")
        pytest.fail("test case Execution for stores data extraction has failed")
    logger.info(f"Test case execution for stores data extraction has completed....")
