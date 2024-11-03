import pandas as pd
import numpy as np
import os
from datetime import datetime

# Define the folder paths and files
folders = {
    'PDB': 'PDB/Consolidated_Peering_Data.csv',
    'IXPDB': 'IXPDB/17_10_24_15_09_25_Consolidated_Data.csv',
    'HE': 'HE/09_10_24_10_32_16_Consolidated_Data.csv',
    'RIR': 'RIR/09_10_24_10_40_00_Records.csv'
}

counties = "HE/09_10_24_10_28_34_Countries.csv"

# Load the Linx Members ASN data
linx_members_file = 'LinxMembers/Linx-member-11-10-2024.csv'
linx_members_df = pd.read_csv(linx_members_file, encoding='ISO-8859-1')
linx_asns = linx_members_df['ASN'].astype(str).tolist()  # Convert ASN to string for matching

# Mapping for each file source to the destination columns
source_column_mapping = {
    'IXPDB': {
        'organization_id': 'Organization_Id',
        'organization_name': 'Organization_Name',
        'org_website': 'Org_Website',
        'city': 'Org_City',
        'country': 'Org_Country_Code',
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
        'opaque': 'ASN',
        'cc': 'ASN_Country_Code',
        'date': 'ASN_Date',
        'type': 'ASN_Type'  # Including 'type' for filtering asn
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

        # Filter RIR data for type 'asn'
        if prefix == 'RIR':
            df = df[df['type'] == 'asn']

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

# Adding IsPeering column based on Linx ASN
merged_data['ASN'] = merged_data['ASN'].astype(str)  # Ensure ASN is string type for matching
merged_data['IsPeering'] = merged_data['ASN'].apply(lambda x: 'true' if x in linx_asns else 'false')

# Adding IsPublicNetwork column based on presence of 'he.net' in URLs
url_columns = ['Org_Website', 'IX_Website', 'IX_URL']  # Columns where URLs might be present
merged_data['IsPublicNetwork'] = merged_data[url_columns].apply(
    lambda row: any(pd.notna(url) and 'he.net' in url.lower() for url in row), axis=1
)

# Adding ConsolidatedCountryCode column based on provided rules
merged_data['ConsolidatedCountryCode'] = merged_data.apply(
    lambda row: row['ASN_Country_Code'] if pd.notna(row['ASN_Country_Code']) else (
        row['IX_Country_Code'] if pd.notna(row['IX_Country_Code']) else (
            row['Org_Country_Code'] if pd.notna(row['Org_Country_Code']) else pd.NA
        )
    ), axis=1
)

# Check if 'ConsolidatedCountryCode' exists in merged_data
if 'ConsolidatedCountryCode' in merged_data.columns:
    # Load the countries.csv file with UTF-8 encoding
    countries_df = pd.read_csv(counties, encoding='utf-8')

    # Ensure columns are properly named
    countries_df.rename(columns={'cc': 'ConsolidatedCountryCode', 'name': 'country'}, inplace=True)

    # Merge merged_data with countries_df based on the ConsolidatedCountryCode
    merged_data = pd.merge(
        merged_data, 
        countries_df[['ConsolidatedCountryCode', 'country']], 
        on='ConsolidatedCountryCode', 
        how='left'
    )

    # Ensure 'country' column exists before modifying it
    if 'country' in merged_data.columns:
        # Populate 'Antarctica' for country where ConsolidatedCountryCode is 'AQ'
        merged_data.loc[merged_data['ConsolidatedCountryCode'] == 'AQ', 'country'] = 'Antarctica'

        print("Error: 'country' column not found after merging.")
else:
    print("Error: 'ConsolidatedCountryCode' column not found in the data.")


# Final duplicate removal
#merged_data = merged_data.drop_duplicates(subset=['Organization_Id', 'ASN', 'IX_ID'])

# Save the merged data to a new file
output_file = 'Consolidated_Data_All_Source'
dt_str = datetime.now().strftime("%d_%m_%y_%H_%M_%S")
new_file_name = f"{dt_str}_{output_file}.csv"
merged_data['ASN'] = merged_data["ASN"].replace("nan", np.nan)
merged_data.dropna(subset=['ASN'], how='all', inplace=True)
merged_data.to_csv(new_file_name, index=False)

print(f"Data merged successfully into '{new_file_name}'.")

