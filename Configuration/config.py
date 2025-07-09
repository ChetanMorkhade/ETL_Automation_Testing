# oracle data configuration
ORACLE_USER = 'SYSTEM'
ORACLE_PASSWORD = 'SYS'
ORACLE_HOST = 'localhost'
ORACLE_PORT = '1521'
ORACLE_DATABASE = 'xe'

# MYSQL data configuration
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'root'
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3306'
MYSQL_DATABASE_STAGING = 'stag_retaildwh'
MYSQL_DATABASE_TARGET= 'retaildwh'

'''
# Linux Setup SSH connection details
hostname = '192.168.0.111'  # Remote server's hostname or IP address
username = 'etltesting'  # SSH username
password = 'root'  # SSH password or use key-based authentication
remote_file_path = '/home/etltesting/sales_data.csv'  # Full path to the file on the remote server
local_file_path = 'TestData/sales_data_Linux_remote.csv'  # Local path to save the file
'''