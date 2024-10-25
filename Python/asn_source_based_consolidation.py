import pandas as pd

# Load the entire dataset
df = pd.read_csv("21_10_24_21_35_08_Consolidated_Data_All_Source.csv")

# Step 1: Define the order of sources for sorting
source_order = ['PDB', 'IXPDB', 'HE', 'RIR']

# Step 2: Split the dataframe into numeric and non-numeric ASN dataframes
df['ASN'] = df['ASN'].astype(str)

# Identify numeric and non-numeric ASNs
numeric_asn_df = df[df['ASN'].str.isnumeric()].copy()
non_numeric_asn_df = df[~df['ASN'].str.isnumeric()].copy()

# Convert the ASN column in numeric dataframe to integer type
numeric_asn_df['ASN'] = numeric_asn_df['ASN'].astype(int)

# Step 3: Sort both dataframes by 'Source' and 'ASN' based on source priority
numeric_asn_df['Source'] = pd.Categorical(numeric_asn_df['Source'], categories=source_order, ordered=True)
numeric_asn_df = numeric_asn_df.sort_values(['ASN', 'Source'])

non_numeric_asn_df['Source'] = pd.Categorical(non_numeric_asn_df['Source'], categories=source_order, ordered=True)
non_numeric_asn_df = non_numeric_asn_df.sort_values(['ASN', 'Source'])

# Step 4: Drop duplicates within each dataframe based on ASN
numeric_asn_df = numeric_asn_df.drop_duplicates(subset='ASN', keep='first')
non_numeric_asn_df = non_numeric_asn_df.drop_duplicates(subset='ASN', keep='first')

# Step 5: Initialize a list to store final rows
final_rows = []

# Set to keep track of added ASNs
added_asns = set()

added_asns_nn = set()

# Step 6: Sequentially append numeric ASN rows to the final rows list
for _, row in numeric_asn_df.iterrows():
    asn = row['ASN']
    if asn not in added_asns:
        final_rows.append(row)
        added_asns.add(asn)

# Step 7: Sequentially append non-numeric ASN rows to the final rows list
for _, row in non_numeric_asn_df.iterrows():
    asn = row['ASN']
    if asn not in added_asns:
        final_rows.append(row)
        added_asns.add(asn)

# Step 8: Create the final dataframe from the list of rows
final_df = pd.DataFrame(final_rows)

# Step 9: Save the filtered DataFrame to a new CSV file
final_df.to_csv("Filtered_ASN_Data.csv", index=False)

print("Filtering completed and saved to 'Filtered_ASN_Data.csv'.")
