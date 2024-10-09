import mysql.connector
import pandas as pd
import os
from datetime import datetime

def export_to_csv(cursor, table_name, output_directory):
    """Export a table to a CSV file using pandas."""
    cursor.execute(f"SELECT * FROM `{table_name}`")  # Use backticks to enclose table names to avoid syntax issues
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    csv_file_path = os.path.join(output_directory, f"{table_name}.csv")
    df.to_csv(csv_file_path, index=False)
    print(f"Data from table '{table_name}' exported to '{csv_file_path}'")

def connect_and_export(server, port, username, password, database, output_directory="peering_database_exports"):
    """Connect to the MySQL database and export each table to a separate CSV file."""
    connection = None  # Initialize connection variable to None
    try:
        # Establish a connection to the MySQL database with the correct host and port
        connection = mysql.connector.connect(
            host=server,
            port=port,
            user=username,
            password=password,
            database=database,
            auth_plugin='mysql_native_password'
        )

        if connection.is_connected():
            print(f"Successfully connected to the database '{database}' on {server}:{port}")
            cursor = connection.cursor()
            
            # Create output directory if it doesn't exist
            os.makedirs(output_directory, exist_ok=True)
            
            # Get a list of all tables in the database
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()

            # Loop through each table and export it to a CSV file
            for (table_name,) in tables:
                # Decode the table name if it's returned as a bytearray
                if isinstance(table_name, bytearray):
                    table_name = table_name.decode('utf-8')
                
                print(f"Exporting data from table '{table_name}'")
                export_to_csv(cursor, table_name, output_directory)

            print(f"All tables have been successfully exported to '{output_directory}'")

    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
    
    finally:
        # Check if connection was successfully established before closing
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# Replace these variables with your MySQL server details
server = '3.71.195.34'
port = 3306
username = 'linx_reporter'
password = 'ReportMe==2023'
database = 'linx_insight'

# Call the function to connect and export data
connect_and_export(server, port, username, password, database)
