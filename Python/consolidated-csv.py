import pandas as pd
import os

# Define the folder paths and files
folders = {
    'IXPDB': 'IXPDB/09_10_24_10_33_47_Consolidated_Data.csv',
    'HE': 'HE/09_10_24_10_32_16_Consolidated_Data.csv',
    'PDB': 'PDB/Consolidated_Peering_Data.csv',
    'RIR': 'RIR/09_10_24_10_40_00_Records.csv'
}

# Mapping for each file source to the destination columns
source_column_mapping = {
    'IXPDB': {
        'organization_id': 'Organization_Id',
        'organization_name': 'Organization_Name',
        'org_website': 'Org_Website',
        'city': 'Org_City',
        'country': 'Org_Country',
        'association': 'Org_Association',
        'provider_id': 'IX_ID',
        'location_count': 'IX_Location_Count',
        'participant_count': 'IX_Paricipant_Count',
        'pdb_id': 'IX_PDB_ID',
        'updated': 'IX_Updated',
        'asn': 'ASN',
        'name': 'ANS_Name',
        'ip_addresses': 'ASN_IP_Addresses'
    },
    'HE': {
        'ParentIndex': 'IX_ID',
        'Exchange Name': 'IX_Name',
        'city': 'IX_City',
        'exchange_cc': 'IX_Country_Code',
        'members': 'IX_Paricipant_Count',
        'website': 'IX_Website',
        'ASN': 'ASN',
        'Name': 'ANS_Name',
        'country_line_cc': 'ASN_Country_Code',
        'name_country': 'ASN_Country_Name',
        'IPv4': 'IPv4',
        'IPv6': 'IPv6',
        'internetExchange': 'IX_InternetExchange',
        'isDataAvailable': 'IX_IsDataAvailable',
        'url': 'IX_URL',
        'adjacencies_v4': 'ASN_Adjacencies_v4',
        'routes_v4': 'ASN_Route_v4',
        'adjacencies_v6': 'ASN_Adjacencies_v6',
        'routes_v6': 'ASN_Route_v6'
    },
    'RIR': {
        'registry': 'IX_Name',
        'opaque': 'ANS_Name',
        'cc': 'ASN_Country_Code',
        'date': 'ASN_Date'
    },
    'PDB': {
        'peering_org_id': 'Organization_Id',
        'name': 'Organization_Name',
        'country': 'Org_Country_Code',
        'city': 'Org_City',
        'region_continent': 'Org_Region_Continent',
        'ix_id': 'IX_ID',
        'name_ixp_exchange': 'IX_Name',
        'net_id': 'Network_ID',
        'asn_connection_peer': 'ASN',
        'country': 'ASN_Country_Code',
        'peering_created_dt': 'ASN_Date',
        'city': 'ASN_City',
        'region_continent': 'ASN_Region_Continent',
        'ipaddr4': 'IPv4',
        'ipaddr6': 'IPv6',
        'aka': 'Org_Aka_Name',
        'name_long': 'Org_Long_Name',
        'info_type': 'Org_Info_Type',
        'info_prefixes4': 'Org_Info_Prefixes4',
        'info_prefixes6': 'Org_Info_Prefixes6',
        'info_traffic': 'Org_Info_traffic',
        'is_rs_peer': 'ANS_Is_Rs_Peer',
        'notes': 'ASN_Notes',
        'speed': 'ASN_Speed',
        'aka_ixp_exchange': 'IX_Aka_Name',
        'name_long_ixp_exchange': 'IX_Long_Name'
    }
}

# Desired columns for the final merged data
destination_columns = [
    'Source', 'Organization_Id', 'Organization_Name', 'Org_Country_Code', 'Org_City', 'Org_Region_Continent', 'IX_ID',
    'IX_Name', 'IX_City', 'IX_Country_Code', 'IX_Updated', 'IX_Paricipant_Count', 'IX_Website', 'Network_ID',
    'Network_Name', 'ASN', 'ANS_Name', 'ASN_Country_Code', 'ASN_Date', 'ASN_City', 'ASN_Country_Name',
    'ASN_Region_Continent', 'IPv4', 'IPv6', 'Org_Website', 'Org_City', 'Org_Country', 'Org_Association',
    'IX_Location_Count', 'IX_Looking_Glass', 'IX_Manrs', 'IX_PDB_ID', 'IX_Addresses', 'IX_Route_Server_ASNS',
    'ASN_Is_IPV6', 'ASN_IP_Addresses', 'IX_InternetExchange', 'IX_IsDataAvailable', 'IX_URL', 'ASN_Adjacencies_v4',
    'ASN_Route_v4', 'ASN_Adjacencies_v6', 'ASN_Route_v6', 'Org_Aka_Name', 'Org_Long_Name', 'Org_Info_Type',
    'Org_Info_Prefixes4', 'Org_Info_Prefixes6', 'Org_Info_traffic', 'ANS_Is_Rs_Peer', 'ASN_Notes', 'ASN_Speed',
    'IX_Aka_Name', 'IX_Long_Name'
]

# Read the files, remove duplicates, and map columns
merged_data = pd.DataFrame(columns=destination_columns)

for prefix, file in folders.items():
    try:
        print(f"Processing file for source: {prefix} - {file}")

        if not os.path.exists(file):
            print(f"File {file} not found. Please check the path.")
            continue

        df = pd.read_csv(file, encoding='ISO-8859-1', low_memory=False)

        # Ensure columns are unique
        if not df.columns.is_unique:
            print(f"Warning: Duplicate columns found in {file}. Resolving duplicates.")
            df.columns = pd.Index([f"{col}_{i}" if df.columns.duplicated()[i] else col for i, col in enumerate(df.columns)])

        # Remove duplicates in the current DataFrame based on key columns
        existing_columns = [col for col in source_column_mapping[prefix].keys() if col in df.columns]
        if existing_columns:
            df = df.drop_duplicates(subset=existing_columns)
        else:
            print(f"Warning: No matching columns found for deduplication in {file}")

        # Map source columns to destination columns
        mapped_columns = {source: dest for source, dest in source_column_mapping[prefix].items() if source in df.columns}
        df = df.rename(columns=mapped_columns)

        # Filter columns to include only the specified destination columns
        df = df[[col for col in destination_columns if col in df.columns]]

        # Ensure columns are unique after renaming
        if not df.columns.is_unique:
            print(f"Warning: Duplicate columns found after renaming in {file}.")
            df.columns = pd.Index([f"{col}_{i}" if df.columns.duplicated()[i] else col for i, col in enumerate(df.columns)])

        # Add missing columns with NaN values
        for col in destination_columns:
            if col not in df.columns:
                df[col] = pd.NA

        # Add Source column at the beginning
        df['Source'] = prefix
        df = df[['Source'] + [col for col in destination_columns if col != 'Source']]

        # Add the cleaned and filtered DataFrame to the merged_data DataFrame
        merged_data = pd.concat([merged_data, df], ignore_index=True)

    except pd.errors.ParserError as e:
        print(f"ParserError while reading {file}: {e}")
    except ValueError as ve:
        print(f"ValueError while processing {file}: {ve}")
    except Exception as e:
        print(f"An error occurred while processing {file}: {e}")

# Final duplicate removal
merged_data = merged_data.drop_duplicates(subset=['Organization_Id', 'ASN', 'IX_ID'])

# Save the merged data
output_file = 'merged_peering_data.csv'
merged_data.to_csv(output_file, index=False)
print(f"Data merged successfully into '{output_file}'.")
