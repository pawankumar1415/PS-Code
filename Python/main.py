import pandas as pd

# Load the CSV files
country_line = pd.read_csv(r"..\Exchange Export\27_09_24_03_04_42_Country Lines.csv")
provider_participant = pd.read_csv(r"..\IXPDB\25_09_24_02_29_15_Providers_Participants.csv")
provider = pd.read_csv(r"..\IXPDB\25_09_24_02_29_15_Providers.csv")

# Read the `records` file with `low_memory=False` to avoid mixed type warning
records = pd.read_csv(r"..\RIR\27_09_24_07_55_36_Records.csv", low_memory=False)

# Define an empty DataFrame for the consolidated report
consolidated_report = pd.DataFrame(columns=["Country", "ASN", "ASN Name"])

def record_data_extraction(consolidated_report):
    # Loop through records and append to consolidated_reports if the status is 'assigned'
    rows_to_add = []  # List to collect rows to be added
    for index, row in records.iterrows():
        if row['status'] == 'assigned':
            # Collect rows in a list of dictionaries
            rows_to_add.append({
                "Country": row['cc'],
                "ASN": row['opaque'],
                "ASN Name": row['registry']
            })

    # Use pd.concat instead of append to add the new rows
    print("Record Data Records: -", len(rows_to_add))
    if rows_to_add:
        consolidated_report = pd.concat([consolidated_report, pd.DataFrame(rows_to_add)], ignore_index=True)
    return consolidated_report

def provider_data_extraction(consolidated_report):
    # Step 1: Merge `provider_participant` with `providers` using `ProviderID` and `id`
    merged_data = provider_participant.merge(provider, left_on='ProviderID', right_on='id', how='inner')

    # Step 2: Extract the required columns after verifying their names
    # The `name` column may have been renamed after the merge, so check for 'name_x' or 'name_y'
    rows_to_add = merged_data[['country', 'asn', 'name_x']].rename(
        columns={
            'country': 'Country',
            'asn': 'ASN',
            'name_x': 'ASN Name'  # Adjust the column name based on your specific suffix (e.g., name_x)
        }).to_dict(orient='records')  # Convert the DataFrame to a list of dictionaries for easier appending

    # Step 3: Append the rows to `consolidated_report` using `pd.concat`
    print("Provider Data Records: -", len(rows_to_add))
    if rows_to_add:
        consolidated_report = pd.concat([consolidated_report, pd.DataFrame(rows_to_add)], ignore_index=True)
    return consolidated_report


def country_line_data_extraction(consolidated_report):
    # Collect rows from `country_line` and map them directly
    rows_to_add = []  # List to collect rows to be added
    for index, row in country_line.iterrows():
        rows_to_add.append({
            "Country": row['cc'],  # Map 'cc' to 'Country'
            "ASN": row['asn'],     # Map 'asn' to 'ASN'
            "ASN Name": row['name'] # Map 'name' to 'ASN Name'
        })

    # Use pd.concat to append the new rows
    print("Country Line Data Records: -", len(rows_to_add))
    if rows_to_add:
        consolidated_report = pd.concat([consolidated_report, pd.DataFrame(rows_to_add)], ignore_index=True)
    return consolidated_report

def main():
    # Initialize the DataFrame for consolidated report and pass it to functions
    consolidated_report = pd.DataFrame(columns=["Country", "ASN", "ASN Name"])
    consolidated_report = record_data_extraction(consolidated_report)
    consolidated_report = provider_data_extraction(consolidated_report)
    consolidated_report = country_line_data_extraction(consolidated_report)

    # Save to CSV
    consolidated_report.to_csv("Consolidated_Report.csv", index=False)
    print("Consolidated Report Records: - ", len(consolidated_report))
    print("Consolidated report has been saved successfully.")

    cleaned_consolidated_report = consolidated_report.drop_duplicates()
    cleaned_consolidated_report = cleaned_consolidated_report.sort_values("Country")
    cleaned_consolidated_report.to_csv("Cleaned_Consolidated_Report.csv", index=False)
    print("Cleaned Consolidated Report Records: - ", len(cleaned_consolidated_report))
    print("Cleaned Consolidated report has been saved successfully.")

if __name__ == "__main__":
    main()
