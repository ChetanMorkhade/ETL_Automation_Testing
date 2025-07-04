#Test Target data loading Script
import pytest
import logging

#importing common utility functions from util.py file
from CommonUtils.utils import Verify_expected_source_table_data_vs_actual_stag_table_data

logging.basicConfig(
    filename='LogFiles/Target_Loading.log',
    filemode = 'a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

#Test cases for validation of target table data loading
#@pytest.mark.skip
def test_DataLoad_for_Monthly_sales_summary_table_check(connect_to_mysql_database_staging,connect_to_mysql_database_target):  #using DB connectors fron fixtures in conftest.py file
    logger.info(f"Test case execution for Monthly_sales_summary_table_check has started....")
    try:
        query_expected = """select * from monthly_sales_summary_source"""
        query_actual = """select * from monthly_sales_summary"""
        Verify_expected_source_table_data_vs_actual_stag_table_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_target)  #using from utility functions
    except Exception as e:
        logger.error(f"test case Execution for Monthly_sales_summary_table_check has failed{e}")
        pytest.fail("test case Execution for Monthly_sales_summary_table_check has failed")
    logger.info(f"Test case execution for Monthly_sales_summary_table_check has completed....")

#@pytest.mark.skip
def test_DataLoad_for_fact_sales_table_check(connect_to_mysql_database_staging,connect_to_mysql_database_target):  #using DB connectors fron fixtures in conftest.py file
    logger.info(f"Test case execution for fact_sales_table_check has started....")
    try:
        query_expected = """select sales_id,product_id,store_id,quantity,total_sales, sale_date from sales_with_details"""
        query_actual = """select sales_id, product_id, store_id, quantity,total_sales,sale_date from fact_sales"""
        Verify_expected_source_table_data_vs_actual_stag_table_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_target)  #using from utility functions
    except Exception as e:
        logger.error(f"test case Execution for fact_sales_table_check has failed{e}")
        pytest.fail("test case Execution for fact_sales_table_check has failed")
    logger.info(f"Test case execution for fact_sales_table_check has completed....")

#@pytest.mark.skip
def test_DataLoad_for_fact_inventory_table_check(connect_to_mysql_database_staging,connect_to_mysql_database_target):  #using DB connectors fron fixtures in conftest.py file
    logger.info(f"Test case execution for fact_inventory_table_check has started....")
    try:
        query_expected = """select * from staging_inventory"""
        query_actual = """select * from fact_inventory"""
        Verify_expected_source_table_data_vs_actual_stag_table_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_target)  #using from utility functions
    except Exception as e:
        logger.error(f"test case Execution for fact_inventory_table_check has failed{e}")
        pytest.fail("test case Execution for fact_inventory_table_check has failed")
    logger.info(f"Test case execution for fact_inventory_table_check has completed....")

def test_DataLoad_for_inventory_level_stores_table_check(connect_to_mysql_database_staging,connect_to_mysql_database_target):  #using DB connectors fron fixtures in conftest.py file
    logger.info(f"Test case execution for inventory_level_stores_table_check has started....")
    try:
        query_expected = """select store_id,total_inventory from aggregated_inventory_level"""
        query_actual = """select store_id,cast(total_inventory as Double) as total_inventory from inventory_levels_by_store"""
        Verify_expected_source_table_data_vs_actual_stag_table_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_target)  #using from utility functions
    except Exception as e:
        logger.error(f"test case Execution for inventory_level_stores_table_check has failed{e}")
        pytest.fail("test case Execution for inventory_level_stores_table_check has failed")
    logger.info(f"Test case execution for inventory_level_stores_table_check has completed....")