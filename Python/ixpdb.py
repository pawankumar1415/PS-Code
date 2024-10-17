import requests
import pandas as pd
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Define headers and base URL
headers = {"content-type": "application/json"}
base_url = "https://api.ixpdb.net/v1"

# Create directory to store the exported files
os.makedirs("IXPDB Export", exist_ok=True)

# Function to hit HTTP requests in parallel
def hit_http_request(relative_url):
    url = base_url + relative_url
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {relative_url}: {e}")
        return None

# Function to export data to CSV
def export_file(data, filename):
    dt_str = datetime.now().strftime("%d_%m_%y_%H_%M_%S")
    new_file_name = f"{dt_str}_{filename}.csv"
    path = os.path.join("IXPDB Export", new_file_name)
    df = pd.DataFrame(data)
    df.to_csv(path, index=False)

# Function to handle parallel fetching of data
def fetch_parallel_data(urls):
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(hit_http_request, url): url for url in urls}
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)
    return results

# Step 1: Fetch Providers and create a list of unique organization IDs
provider_records = hit_http_request("/provider/list")

# Extract unique organization IDs
org_ids = {record['organization_id'] for record in provider_records}

# Step 2: Fetch Organization details and contacts using parallel requests
org_urls = [f"/organization/{org_id}" for org_id in org_ids]
organisation_data = fetch_parallel_data(org_urls)

# Prepare Organization Records and Contacts
organisation_records = []
organisation_contact = []
for org_res in organisation_data:
    organisation_records.append({
        "organization_id": org_res["id"],
        "organization_name": org_res["name"],
        "website": org_res["website"],
        "city": org_res["city"],
        "country": org_res["country"],
        "association": ", ".join(org_res["association"])
    })
    for contact in org_res["contacts"]:
        organisation_contact.append({
            "organization_id": org_res["id"],
            "phone": contact.get("phone"),
            "name": contact.get("name"),
            "email": contact.get("email"),
            "address": contact.get("address"),
            "city": contact.get("city"),
            "country": contact.get("country")
        })

# Step 3: Fetch Traffic records
traffic_records = hit_http_request("/traffic/list")

# Step 4: Fetch Participant records
participant_response = hit_http_request("/participant/list")
participant_records = [{
    "asn": participant_record["asn"],
    "ip_addresses": ", ".join(participant_record["ip_addresses"]),
    "ipv6": participant_record["ipv6"],
    "manrs": participant_record["manrs"],
    "name": participant_record["name"],
    "provider_count": participant_record["provider_count"]
} for participant_record in participant_response]

# Function to handle parallel fetching of data with retry on failure
def fetch_parallel_data_with_retry(urls, retry_limit=3):
    results = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(hit_http_request, url): url for url in urls}
        for future in as_completed(futures):
            url = futures[future]
            provider_id = url.split("/")[-2]  # Extract Provider ID from URL
            attempt = 0
            while attempt < retry_limit:
                try:
                    result = future.result()
                    if result is not None:
                        results[provider_id] = result
                        break
                except Exception as e:
                    attempt += 1
                    print(f"Error fetching data for ProviderID: {provider_id}, Attempt: {attempt}. Error: {e}")
                    if attempt == retry_limit:
                        print(f"Failed to fetch data for ProviderID: {provider_id} after {retry_limit} attempts")
    return results

# Step 5: Fetch network and participant details for each provider using parallel requests
provider_urls = [f"/provider/{provider['id']}/networks" for provider in provider_records]
network_data = fetch_parallel_data(provider_urls)

provider_networks = []
for provider, networks in zip(provider_records, network_data):
    for network in networks:
        provider_networks.append({
            "ProviderID": provider["id"],
            "network_name": network["name"],
            "addresses": ", ".join(network["addresses"]),
            "route_server_asns": network["route_server_asns"]
        })

# Fetch participant details for each provider using parallel requests
participant_urls = [f"/provider/{provider['id']}/participants" for provider in provider_records]

# Fetch participant data with retry mechanism
participant_data_map = fetch_parallel_data_with_retry(participant_urls)

provider_participants = []

# Iterate through provider records and get their participants from the map
for provider in provider_records:
    provider_id = provider["id"]
    participants = participant_data_map.get(str(provider_id), [])  # Fetch participants by Provider ID
    if participants:  # Check if participants exist
        for participant in participants:
            provider_participants.append({
                "ProviderID": provider_id,
                "asn": participant["asn"],
                "name": participant["name"],
                "ipv6": participant.get("ipv6", "N/A"),  # Handle missing fields
                "ip_addresses": ", ".join(participant.get("ip_addresses", []))  # Handle missing addresses
            })
    else:
        print(f"No participants found for ProviderID: {provider_id}")

# Optional: Print a summary to debug if needed
print(f"Total participants appended: {len(provider_participants)}")

# Step 6: Consolidate provider records for CSV export
provider_records_v2 = [{
    "provider_id": provider["id"],
    "organization_id": provider["organization_id"],
    "provider_name": provider["name"],
    "provider_city": provider["city"],
    "provider_country": provider["country"],
    "location_count": provider["location_count"],
    "looking_glass": ", ".join(provider.get("looking_glass", [])),
    "manrs": provider["manrs"],
    "participant_count": provider["participant_count"],
    "pdb_id": provider["pdb_id"],
    "updated": provider["updated"],
    "website": provider["website"]
} for provider in provider_records]

# Step 7: Create DataFrames for each section and merge into consolidated
org_df = pd.DataFrame(organisation_records)
provider_df = pd.DataFrame(provider_records_v2)
network_df = pd.DataFrame(provider_networks)
participant_df = pd.DataFrame(provider_participants)

# Step 8: Merge Providers with Organizations
provider_org_df = org_df.merge(provider_df, on="organization_id", how="inner")

# Step 9: Merge Providers with Networks
provider_org_network_df = provider_org_df.merge(network_df, left_on="provider_id", right_on="ProviderID", how="left")

# Step 10: Merge with Participants, expanding rows to repeat details
consolidated_data = provider_org_network_df.merge(
    participant_df, left_on="provider_id", right_on="ProviderID", how="left"
)

# Step 11: Export separate and consolidated files
export_file(provider_records_v2, "Providers")
export_file(provider_participants, "Providers_Participants")
export_file(provider_networks, "Providers_Networks")
export_file(organisation_records, "Organizations")
export_file(traffic_records, "Traffic")
export_file(organisation_contact, "Contacts")
export_file(consolidated_data.to_dict(orient="records"), "Consolidated_Data")
