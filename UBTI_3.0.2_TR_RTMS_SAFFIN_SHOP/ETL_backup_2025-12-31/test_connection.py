from Snowflake_connection import *

connection_string = "jdbc:snowflake://SPPL-NONPROD.snowflakecomputing.com/?private_key_file=/home/minhhoang/Snowflake/privatekey/snowflake_dev_rsakey1.p8&private_key_file_pwd=snowflakeqlik54321!&useProxy=true&proxyHost=10.208.100.10&proxyPort=8080&CLIENT_RESULT_COLUMN_CASE_INSENSITIVE=true"

connection = snowflake_connection_using_connection_string(connection_string)

if connection:
    print("Connection successful!")
else:
    print("Connection failed.")