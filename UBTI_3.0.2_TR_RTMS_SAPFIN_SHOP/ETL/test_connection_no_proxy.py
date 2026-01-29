from Snowflake_connection import *

# Test connection without proxy - include private key
connection_string = "jdbc:snowflake://SPPL-NONPROD.snowflakecomputing.com/?private_key_file=snowflake_dev_rsakey1.p8&private_key_file_pwd=snowflakeqlik54321!&CLIENT_RESULT_COLUMN_CASE_INSENSITIVE=true"

connection = snowflake_connection_using_connection_string(connection_string)

if connection:
    print("Connection successful!")
    print(f"Current database: {connection.database}")
    print(f"Current schema: {connection.schema}")
    print(f"Current user: {connection.user}")
    print(f"Current warehouse: {connection.warehouse}")
else:
    print("Connection failed.")