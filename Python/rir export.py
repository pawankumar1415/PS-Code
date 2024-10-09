import os
import requests
from datetime import datetime
import csv

# Create directory to store the exported files
os.makedirs("Exchange Export", exist_ok=True)

# Define URLs
rir_adoption_url = "https://www.nro.net/wp-content/uploads/rpki-uploads/rir-adoption.csv"
economy_adoption_url = "https://www.nro.net/wp-content/uploads/rpki-uploads/economy-adoption.csv"
nro_extended_url = "https://ftp.ripe.net/pub/stats/ripencc/nro-stats/latest/nro-delegated-stats"

# Define path to save files
base_path = r"RIR"

# Ensure that the base path exists
os.makedirs(base_path, exist_ok=True)  # Create the RIR directory if it doesn't exist

# Get current date and time string
dt_str = datetime.now().strftime("%d_%m_%y_%H_%M_%S")

# Define file paths
rir_adoption_path = os.path.join(base_path, f"{dt_str}_rir_adoption.csv")
economy_adoption_path = os.path.join(base_path, f"{dt_str}_economy_adoption.csv")
nro_extended_path = os.path.join(base_path, f"{dt_str}_nro_extended.csv")

# Download the files
def download_file(url, path):
    response = requests.get(url)
    with open(path, 'wb') as file:
        file.write(response.content)
    print(f"Downloaded: {path}")

# Download the files using the defined URLs and paths
download_file(rir_adoption_url, rir_adoption_path)
download_file(economy_adoption_url, economy_adoption_path)
download_file(nro_extended_url, nro_extended_path)

# Function to export records to CSV file
def export_file(data, file_name, base_path):
    dt_str = datetime.now().strftime("%d_%m_%y_%H_%M_%S")
    new_file_name = f"{dt_str}_{file_name}.csv"
    path = os.path.join(base_path, new_file_name)
    with open(path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"Exported: {path}")

# Read the downloaded file
with open(nro_extended_path, 'r', encoding='utf-8') as file:
    content = file.readlines()

# Initialize containers
header_record = []
summary_records = []
records = []

# Process each line in the content
for line_count, line in enumerate(content):
    # Display progress
    percent_complete = (line_count * 100) / len(content)
    print(f"Processing: {round(percent_complete, 0)}% completed")

    # Skip comment lines
    if line.startswith("#"):
        continue

    # Split the line by '|'
    line_fields = line.strip().split('|')

    # Process header record
    if line_count == 0:
        header_record.append({
            'version': line_fields[0],
            'registry': line_fields[1],
            'serial': line_fields[2],
            'records': line_fields[3],
            'startdate': line_fields[4],
            'enddate': line_fields[5],
            'UTCoffset': line_fields[6]
        })
    # Process normal records
    elif len(line_fields) > 6:
        records.append({
            'registry': line_fields[0],
            'cc': line_fields[1],
            'type': line_fields[2],
            'start': line_fields[3],
            'value': line_fields[4],
            'date': line_fields[5],
            'status': line_fields[6],
            'opaque': line_fields[7] if len(line_fields) > 7 else ""
        })
    # Process summary records
    else:
        summary_records.append({
            'registry': line_fields[0],
            'type': line_fields[2],
            'count': line_fields[3]
        })

# Export each type of record to a separate CSV file
export_file(summary_records, "Summary", base_path)
export_file(records, "Records", base_path)
export_file(header_record, "Header", base_path)

print("Process completed successfully!")
