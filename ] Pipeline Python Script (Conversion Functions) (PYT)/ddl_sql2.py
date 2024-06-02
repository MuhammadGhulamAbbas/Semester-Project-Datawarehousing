# import mysql.connector
# from mysql.connector import Error

# # MySQL connection parameters
# mysql_host = 'localhost'
# mysql_database = 'airportdb'
# mysql_user = 'root'
# mysql_password = 'root'
# # port=3036

# def main_ddl():
#     schemas = []
#     connection = None
#     cursor = None

#     try:
#         # Connect to MySQL database
#         connection = mysql.connector.connect(
#             host=mysql_host,
#             database=mysql_database,
#             user=mysql_user,
#             password=mysql_password
#         )

#         if connection.is_connected():
#             print("Connected to MySQL database")

#             cursor = connection.cursor(dictionary=True)

#             # Get list of tables
#             original_query = """
#             SELECT TABLE_SCHEMA, TABLE_NAME
#             FROM INFORMATION_SCHEMA.TABLES
#             WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = %s
#             """
#             cursor.execute(original_query, (mysql_database,))
#             query_result = cursor.fetchall()

#             def get_table_schema(table_name):
#                 full_table_name = f"{table_name}"
#                 print(f"Processing table: {full_table_name}")

#                 cursor.execute(f"""
#                     SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
#                     FROM INFORMATION_SCHEMA.COLUMNS
#                     WHERE TABLE_NAME = '{table_name}'
#                 """)
#                 columns = cursor.fetchall()

#                 table_schema = f"CREATE TABLE IF NOT EXISTS staging_airportdb.{full_table_name} (\n"
#                 column_definitions = []
#                 for col in columns:
#                     column_def = f"{col['COLUMN_NAME']} {col['DATA_TYPE']}"
#                     if col['CHARACTER_MAXIMUM_LENGTH']:
#                         column_def += f"({col['CHARACTER_MAXIMUM_LENGTH']})"
#                     if col['IS_NULLABLE'] == "NO":
#                         column_def += " NOT NULL"
#                     column_definitions.append(column_def)

#                 table_schema += ",\n  ".join(column_definitions)
#                 table_schema += "\n);"

#                 # Convert to BigQuery compatible data types
#                 table_schema = table_schema.replace("varchar", "STRING")
#                 table_schema = table_schema.replace("char", "STRING")
#                 table_schema = table_schema.replace("int", "INT64")
#                 table_schema = table_schema.replace("smallint", "INT64")
#                 table_schema = table_schema.replace("decimal", "NUMERIC")
#                 table_schema = table_schema.replace("datetime", "DATETIME")
#                 table_schema = table_schema.replace("text", "STRING")

#                 return table_schema

#             # Process each table and print the BigQuery-compatible schema
#             for table in query_result:
#                 schema = get_table_schema(table['TABLE_NAME'])
#                 print(schema)
#                 schemas.append(schema)

#     except Error as e:
#         print(f"Error: {e}")

#     finally:
#         if cursor is not None:
#             cursor.close()
#         if connection is not None and connection.is_connected():
#             connection.close()
#             print("MySQL connection is closed")

#     # print(schemas)
#     return schemas

# # Execute the function to retrieve and print the schemas
# # main_ddl()


import mysql.connector
from mysql.connector import Error
import logging

# MySQL connection parameters
mysql_host = 'localhost'
port = 3307
mysql_database = 'airportdb'
mysql_user = 'root'
mysql_password = 'root'

 

def main_ddl():
    schemas = []
    alter_statements = []
    connection = None
    cursor = None

    try:
        # Connect to MySQL database
        connection = mysql.connector.connect(
            host=mysql_host,
            port=port,
            database=mysql_database,
            user=mysql_user,
            password=mysql_password
        )

        if connection.is_connected():
            # print("Connected to MySQL database")
            logging.info("Connected to MySQL database")

            cursor = connection.cursor(dictionary=True)

            # Get list of tables
            original_query = """
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = %s
            """
            cursor.execute(original_query, (mysql_database,))
            query_result = cursor.fetchall()

            def get_table_schema_and_alter(table_name):
                full_table_name = f"{table_name}"
                logging.info(f"Processing table: {full_table_name}")
                # print(f"Processing table: {full_table_name}")

                # Get columns
                cursor.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{table_name}'
                """)
                columns = cursor.fetchall()

                # Get primary keys
                cursor.execute(f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE TABLE_NAME = '{table_name}' AND CONSTRAINT_NAME = 'PRIMARY'
                """)
                primary_keys = cursor.fetchall()
                primary_key_columns = [col['COLUMN_NAME'] for col in primary_keys]

                table_schema = f"CREATE TABLE IF NOT EXISTS staging_airportdb.{full_table_name} (\n"
                column_definitions = []
                for col in columns:
                    column_def = f"{col['COLUMN_NAME']} {col['DATA_TYPE']}"
                    if col['CHARACTER_MAXIMUM_LENGTH']:
                        column_def += f"({col['CHARACTER_MAXIMUM_LENGTH']})"
                    if col['IS_NULLABLE'] == "NO":
                        column_def += " NOT NULL"
                    column_definitions.append(column_def)

                table_schema += ",\n  ".join(column_definitions)
                table_schema += "\n);"

                # Convert to BigQuery compatible data types
                table_schema = table_schema.replace("varchar", "STRING")
                table_schema = table_schema.replace("char", "STRING")
                table_schema = table_schema.replace("int", "INT64")
                table_schema = table_schema.replace("smallint", "INT64")
                table_schema = table_schema.replace("decimal", "NUMERIC")
                table_schema = table_schema.replace("datetime", "DATETIME")
                table_schema = table_schema.replace("text", "STRING")
                table_schema = table_schema.replace("time", "TIME")
                table_schema = table_schema.replace("timestamp", "TIMESTAMP")

                if primary_key_columns:
                    alter_statement = f"ALTER TABLE staging_airportdb.{full_table_name} ADD PRIMARY KEY ({', '.join(primary_key_columns)}) NOT ENFORCED;"
                else:
                    alter_statement = ""

                return table_schema, alter_statement

            # Process each table and print the BigQuery-compatible schema
            for table in query_result:
                schema, alter_stmt = get_table_schema_and_alter(table['TABLE_NAME'])
                logging.info(schema)
                # print(schema)
                schemas.append(schema)
                if alter_stmt:
                    # print(alter_stmt)
                    logging.info(alter_stmt)
                    alter_statements.append(alter_stmt)

    except Error as e:
        logging.error(f"Error: {e}")
        # print(f"Error: {e}")

    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()
            logging.info("MySQL connection is closed")
            # print("MySQL connection is closed")

    return schemas, alter_statements
 
