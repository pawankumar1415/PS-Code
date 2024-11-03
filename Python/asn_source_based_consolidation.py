import pandas as pd

# Load your dataframe
df = pd.read_csv('27_10_24_22_19_54_Consolidated_Data_All_Source.csv')  # Replace with your file path

# Convert ASN to string type
df['ASN'] = df['ASN'].astype(str)

# Define the priority order for the 'source' column
source_priority = {'PDB': 1, 'IXPDB': 2, 'HE': 3, 'RIR': 4}

# Add a temporary column for sorting by source priority
df['source_priority'] = df['Source'].map(source_priority)

# Populate 'Organization_Name' into 'IX_Name' where source is 'IXPDB'
df.loc[df['Source'] == 'IXPDB', 'IX_Name'] = df['Organization_Name']

# Ensure ID fields have no decimal places by converting them to integers
id_fields = ['Organization_Id', 'IX_ID', 'Network_ID']
for field in id_fields:
    df[field] = df[field].fillna(0).astype(int)

# Drop duplicate rows based on the entire dataframe
df = df.drop_duplicates()

# Sort by 'ASN' in ascending order and 'Source' based on the priority order
df_sorted = df.sort_values(by=['ASN', 'source_priority'])

# Drop duplicates based on the 'ASN' column only, keeping the first occurrence
df_sorted = df_sorted.drop_duplicates(subset='ASN')

# Drop the temporary 'source_priority' column
df_sorted = df_sorted.drop(columns=['source_priority'])

# Save the sorted dataframe to a CSV file
df_sorted.to_csv('Filtered_ASN_Consolidated_Data_file.csv', index=False)

print("Dataframe sorted, duplicates removed (overall and based on ASN), and saved to 'Filtered_ASN_Consolidated_Data_file.csv'")
