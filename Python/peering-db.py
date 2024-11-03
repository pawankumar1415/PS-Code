import pandas as pd
import os
from datetime import datetime
from sqlalchemy import create_engine

def export_to_csv(df, output_directory, file_name):
    """Export a DataFrame to a CSV file using pandas."""
    csv_file_path = os.path.join(output_directory, f"{file_name}.csv")
    df.to_csv(csv_file_path, index=False)
    print(f"Data exported to '{csv_file_path}'")

def connect_and_export(server, port, username, password, database, output_directory="peering_database_exports"):
    """Connect to the MySQL database using SQLAlchemy and export each table to a separate CSV file."""
    try:
        # Create a SQLAlchemy engine
        db_connection_url = f"mysql+mysqlconnector://{username}:{password}@{server}:{port}/{database}"
        engine = create_engine(db_connection_url)

        print(f"Successfully connected to the database '{database}' on {server}:{port}")

        # Create output directory if it doesn't exist
        os.makedirs(output_directory, exist_ok=True)

        # Step 1: Export all individual tables to separate CSV files
        tables = pd.read_sql("SHOW TABLES", con=engine)

        for table_name in tables['Tables_in_' + database]:
            print(f"Exporting data from table '{table_name}'")
            # Load table data into a DataFrame and export it
            table_df = pd.read_sql(f"SELECT * FROM `{table_name}`", con=engine)
            export_to_csv(table_df, output_directory, table_name)

        print("All individual tables have been successfully exported.")

        # Step 2: Generate a consolidated file for specific tables
        print("Generating the consolidated file...")
        
        # Load data from the three specified tables
        connection_peer_df = pd.read_sql("SELECT * FROM connection_peer", con=engine)
        ixp_exchange_df = pd.read_sql("SELECT * FROM ixp_exchange", con=engine)
        net_networks_df = pd.read_sql("SELECT * FROM net_network", con=engine)

        # Step 3: Merge net_networks and Connection_peer based on `peering_net_id`
        merged_df = pd.merge(connection_peer_df, net_networks_df, how='left', left_on='net_id', right_on='peering_net_id', suffixes=('_connection_peer', '_net_networks'))

        # Step 4: Merge with ixp_exchange based on `peering_ixp_id`
        consolidated_df = pd.merge(merged_df, ixp_exchange_df, how='left', left_on='ix_id', right_on='peering_ixp_id', suffixes=('', '_ixp_exchange'))

        # Step 5: Remove duplicate columns if necessary
        consolidated_df = consolidated_df.loc[:, ~consolidated_df.columns.duplicated()]

        # Step 6: Drop any duplicate records after merging based on all columns
        consolidated_df.drop_duplicates(inplace=True)

        # Step 7: Reorder columns based on the sequence: net_networks, ixp_exchange, Connection_peer
        net_network_cols = [col for col in consolidated_df.columns if col.endswith('_net_networks')]
        ixp_exchange_cols = [col for col in consolidated_df.columns if col.endswith('_ixp_exchange')]
        connection_peer_cols = [col for col in consolidated_df.columns if '_net_networks' not in col and '_ixp_exchange' not in col]

        # Set the column order
        column_order = net_network_cols + ixp_exchange_cols + connection_peer_cols
        consolidated_df = consolidated_df[column_order]

        # Step 8: Export the consolidated file
        export_to_csv(consolidated_df, output_directory, "Consolidated_Peering_Data")
        print(f"Consolidated data has been successfully exported to '{output_directory}/Consolidated_Peering_Data.csv'")

    except Exception as e:
        print(f"Error connecting to MySQL: {e}")

# Replace these variables with your MySQL server details
server = '3.71.195.34'
port = 3306
username = 'linx_reporter'
password = 'ReportMe==2023'
database = 'linx_insight'

# Call the function to connect and export data
connect_and_export(server, port, username, password, database)
