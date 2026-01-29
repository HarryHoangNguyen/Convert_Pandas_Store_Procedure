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
        cur.execute(f"USE DATABASE {result[0]}")
        cur.execute(f"USE SCHEMA {result[1]}")
        print(f"Switched to Database: {result[0]}, Schema: {result[1]}")
        logger.info(f"Switched to Database: {result[0]}, Schema: {result[1]}")
        table_name_upper = table_name.upper()

        # Check if table exists
        cur.execute(f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{result[1]}'
            AND TABLE_NAME = '{table_name_upper}'
        """)
        table_exists = cur.fetchone()[0] > 0

        if table_exists:
            print(f"Table {result[0]}.{result[1]}.{table_name_upper} exists. Clearing existing data...")
            logger.info(f"Table {result[0]}.{result[1]}.{table_name_upper} exists. Clearing existing data...")
            try:
                cur.execute(f"DELETE FROM {result[0]}.{result[1]}.{table_name_upper} WHERE PROCDATE = '{procdate}';")
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
                schema=result[1],
                database=result[0],
                overwrite=False,  # Don't overwrite table structure
                auto_create_table=False  # Table already exists
            )
        else:
            print(f"Table {result[0]}.{result[1]}.{table_name_upper} does not exist. Creating new table with pandas data types...")
            logger.info(f"Table {result[0]}.{result[1]}.{table_name_upper} does not exist. Creating new table with pandas data types...")
            # Create new table and insert data using pandas inferred types
            success, nchunks, nrows, _ = write_pandas(
                conn=connection,
                df=dataframe,
                table_name=table_name_upper,
                schema=result[1],
                database=result[0],
                overwrite=True,  # Create new table
                auto_create_table=True  # Let pandas create the table
            )

        # Verify data was written
        cur.execute(f"SELECT COUNT(*) FROM {result[0]}.{result[1]}.{table_name.upper()} WHERE PROCDATE = '{procdate}';")
        count_result = cur.fetchone()
        print(f"Verification: {count_result[0]} rows found in {result[0]}.{result[1]}.{table_name.upper()}")
        logger.info(f"Verification: {count_result[0]} rows found in {result[0]}.{result[1]}.{table_name.upper()}")

        connection.commit()
        print(f"Successfully written {nrows} rows to {result[0]}.{result[1]}.{table_name.upper()}")
        print(f"Number of chunks: {nchunks}, Success: {success}")
        logger.info(f"Successfully written {nrows} rows to {result[0]}.{result[1]}.{table_name.upper()}")
        logger.info(f"Number of chunks: {nchunks}, Success: {success}")

    except Exception as e:
        print(f"Error writing to Snowflake: {e}")
        logger.error(f"Error writing to Snowflake: {e}")
        import traceback
        traceback.print_exc()