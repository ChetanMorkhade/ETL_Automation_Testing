#Prerequisites before running test cases
from sqlalchemy import create_engine
import logging
import pytest

from Configuration.config import *  #importing all database configurations from config file

logging.basicConfig(
    filename='LogFiles/Target_Loading.log',
    filemode = 'a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

#Fixtures to connect with databases
@pytest.fixture()
def connect_to_oracle_database():
    logger.info("Oracle connection is getting established")
    oracle_engine = create_engine(
        f"oracle+oracledb://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_DATABASE}").connect()
    logger.info("Oracle connection has been established")
    yield oracle_engine
    oracle_engine.close()
    logger.info("Oracle connection has been closed")

@pytest.fixture()
def connect_to_mysql_database_staging():
    logger.info("mysql connection is getting established")
    mysql_engine = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE_STAGING}").connect()
    logger.info("mysql connection has been established")
    yield mysql_engine
    mysql_engine.close()
    logger.info("mysql connection has been closed")

@pytest.fixture()
def connect_to_mysql_database_target():
    logger.info("mysql connection is getting established")
    mysql_engine_target = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE_TARGET}").connect()
    logger.info("mysql connection has been established")
    yield mysql_engine_target
    mysql_engine_target.close()
    logger.info("mysql connection has been closed")
