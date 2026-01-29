"""
Documentation for write_to_snowflake function.
This function writes a pandas DataFrame to a Snowflake table.
It checks if the table exists, truncates it if it does, or creates it if it doesn't.
"""
import logging

logger = logging.getLogger(__name__)

from snowflake.connector.pandas_tools import write_pandas
# Write the final DataFrame to Snowflake
def write_to_snowflake(dataframe, connection, database, schema, table_name, procdate):
    try:
        # Show current context
        cur = connection.cursor()
        cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
        result = cur.fetchone()
        print(f"Current context - Database: {result[0]}, Schema: {result[1]}, Warehouse: {result[2]}")
        logger.info(f"Current context - Database: {result[0]}, Schema: {result[1]}, Warehouse: {result[2]}")
        # Set the database and schema
        cur.execute(f"USE DATABASE {database}")
        cur.execute(f"USE SCHEMA {schema}")
        print(f"Switched to Database: {database}, Schema: {schema}")
        logger.info(f"Switched to Database: {database}, Schema: {schema}")
        table_name_upper = table_name.upper()

        # Check if table exists
        cur.execute(f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{table_name_upper}'
        """)
        table_exists = cur.fetchone()[0] > 0

        if table_exists:
            print(f"Table {database}.{schema}.{table_name_upper} exists. Clearing existing data...")
            logger.info(f"Table {database}.{schema}.{table_name_upper} exists. Clearing existing data...")
            try:
                cur.execute(f"DELETE FROM {database}.{schema}.{table_name_upper} WHERE PROCDATE = '{procdate}';")
                print(f"Successfully deleted records for PROCDATE = '{procdate}'")
                logger.info(f"Successfully deleted records for PROCDATE = '{procdate}'")
            except Exception as delete_error:
                print(f"Warning: Could not delete existing records for PROCDATE = '{procdate}': {delete_error}")
                logger.warning(f"Could not delete existing records for PROCDATE = '{procdate}': {delete_error}")
                print("Proceeding with insert - duplicates may occur.")
                logger.warning("Proceeding with insert - duplicates may occur.")


            # Insert new data into existing table
            print("Inserting new data into existing table...")
            logger.info("Inserting new data into existing table...")
            success, nchunks, nrows, _ = write_pandas(
                conn=connection,
                df=dataframe,
                table_name=table_name_upper,
                schema=schema,
                database=database,
                overwrite=False,  # Don't overwrite table structure
                auto_create_table=False  # Table already exists
            )
        else:
            print(f"Table {database}.{schema}.{table_name_upper} does not exist. Creating new table with pandas data types...")
            logger.info(f"Table {database}.{schema}.{table_name_upper} does not exist. Creating new table with pandas data types...")
            # Create new table and insert data using pandas inferred types
            success, nchunks, nrows, _ = write_pandas(
                conn=connection,
                df=dataframe,
                table_name=table_name_upper,
                schema=schema,
                database=database,
                overwrite=True,  # Create new table
                auto_create_table=True  # Let pandas create the table
            )

        # Verify data was written
        cur.execute(f"SELECT COUNT(*) FROM {database}.{schema}.{table_name.upper()} WHERE PROCDATE = '{procdate}';")
        count_result = cur.fetchone()
        print(f"Verification: {count_result[0]} rows found in {database}.{schema}.{table_name.upper()}")
        logger.info(f"Verification: {count_result[0]} rows found in {database}.{schema}.{table_name.upper()}")

        connection.commit()
        print(f"Successfully written {nrows} rows to {database}.{schema}.{table_name.upper()}")
        print(f"Number of chunks: {nchunks}, Success: {success}")
        logger.info(f"Successfully written {nrows} rows to {database}.{schema}.{table_name.upper()}")
        logger.info(f"Number of chunks: {nchunks}, Success: {success}")

    except Exception as e:
        print(f"Error writing to Snowflake: {e}")
        logger.error(f"Error writing to Snowflake: {e}",exc_info=True)
        import traceback
        traceback.print_exc()