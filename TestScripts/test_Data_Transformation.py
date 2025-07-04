#Test data Transformation Script
import pytest
import logging

#importing common utility functions from util.py file
from CommonUtils.utils import Verify_expected_source_table_data_vs_actual_stag_table_data

logging.basicConfig(
    filename='LogFiles/Transformation.log',
    filemode = 'a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

#Test cases for validation of data Transformation
@pytest.mark.smoke
@pytest.mark.regression   #markers to distinguished diff test case
def test_DataTransformation_SaleDate_Filter_check(connect_to_mysql_database_staging):  #using DB connectors fron fixtures in conftest.py file
    logger.info(f"Test case execution for SaleDate_Filter has started....")
    try:
        query_expected = """select * from staging_sales where sale_date>='2024-09-10'"""
        query_actual = """select * from filtered_sales_data"""
        Verify_expected_source_table_data_vs_actual_stag_table_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_staging)  #using from utility functions
    except Exception as e:
        logger.error(f"test case Execution for SaleDate_Filter check has failed{e}")
        pytest.fail("test case Execution for SaleDate_Filter check has failed")
    logger.info(f"Test case execution for SaleDate_Filter check has completed....")

@pytest.mark.smoke
@pytest.mark.regression
def test_DataTransformation_Router_HighSales_check(connect_to_mysql_database_staging):  #using DB connectors fron fixtures in conftest.py file
    logger.info(f"Test case execution for Router_HighSales check has started....")
    try:
        query_expected = """select * from filtered_sales_data where region = 'High'"""
        query_actual = """select * from high_sales"""
        Verify_expected_source_table_data_vs_actual_stag_table_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_staging)  #using from utility functions
    except Exception as e:
        logger.error(f"test case Execution for  Router_HighSales check has failed{e}")
        pytest.fail("test case Execution for  Router_HighSales check has failed")
    logger.info(f"Test case execution for  Router_HighSales check has completed....")

@pytest.mark.smoke
@pytest.mark.regression
def test_DataTransformation_Router_LowSales_check(connect_to_mysql_database_staging):  #using DB connectors fron fixtures in conftest.py file
    logger.info(f"Test case execution for Router_LowSales check has started....")
    try:
        query_expected = """select * from filtered_sales_data where region = 'Low'"""
        query_actual = """select * from low_sales"""
        Verify_expected_source_table_data_vs_actual_stag_table_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_staging)  #using from utility functions
    except Exception as e:
        logger.error(f"test case Execution for  Router_LowSales check has failed{e}")
        pytest.fail("test case Execution for  Router_LowSales check has failed")
    logger.info(f"Test case execution for  Router_LowSales check has completed....")

@pytest.mark.smoke
@pytest.mark.regression
def test_DataTransformation_Aggregated_sales_summary_data_check(connect_to_mysql_database_staging):  #using DB connectors fron fixtures in conftest.py file
    logger.info(f"Test case execution for Aggregated_sales_summary_data_check has started....")
    try:
        query_expected = """select f.product_id,month(f.sale_date) as month ,year(f.sale_date) as year, sum(f.price*f.quantity) as total_sales
                            from filtered_sales_data as f group by f.product_id,month(f.sale_date),year(f.sale_date)
                            order by product_id;"""
        query_actual = """select * from monthly_sales_summary_source"""
        Verify_expected_source_table_data_vs_actual_stag_table_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_staging)  #using from utility functions
    except Exception as e:
        logger.error(f"test case Execution for Aggregated_sales_summary_data_check has failed{e}")
        pytest.fail("test case Execution for Aggregated_sales_summary_data_check has failed")
    logger.info(f"Test case execution for  Aggregated_sales_summary_data_check  has completed....")

@pytest.mark.smoke
@pytest.mark.regression
def test_DataTransformation_Aggregated_inventory_level_data_check(connect_to_mysql_database_staging):  #using DB connectors fron fixtures in conftest.py file
    logger.info(f"Test case execution for Aggregated_inventory_level_data_check has started....")
    try:
        query_expected = """select store_id,sum(quantity_on_hand) as total_inventory from staging_inventory group by store_id"""
        query_actual = """select * from aggregated_inventory_level"""
        Verify_expected_source_table_data_vs_actual_stag_table_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_staging)  #using from utility functions
    except Exception as e:
        logger.error(f"test case Execution for Aggregated_inventory_level_data_check has failed{e}")
        pytest.fail("test case Execution for Aggregated_inventory_level_data_check has failed")
    logger.info(f"Test case execution for  Aggregated_inventory_level_data_check  has completed....")

@pytest.mark.smoke
@pytest.mark.regression
@pytest.mark.post_release_check
def test_DataTransformation_Joiner_sales_products_stores_data_check(connect_to_mysql_database_staging):  #using DB connectors fron fixtures in conftest.py file
    logger.info(f"Test case execution for Joiner_sales_products_stores_data_check has started....")
    try:
        query_expected = """select fs.sales_id,fs.sale_date,fs.price,fs.quantity,fs.price*fs.quantity as total_sales,
                    p.product_id,p.product_name,s.store_id,s.store_name
                     from filtered_sales_data as fs
                    inner join staging_product as p on fs.product_id = p.product_id
                    inner join staging_stores as s on s.store_id = fs.store_id"""
        query_actual = """select * from sales_with_details"""
        Verify_expected_source_table_data_vs_actual_stag_table_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_staging)  #using from utility functions
    except Exception as e:
        logger.error(f"test case Execution for Joiner_sales_products_stores_data_check has failed{e}")
        pytest.fail("test case Execution for Joiner_sales_products_stores_data_check has failed")
    logger.info(f"Test case execution for  Joiner_sales_products_stores_data_check  has completed....")

