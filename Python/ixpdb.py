import requests
import pandas as pd
import os
from datetime import datetime

headers = {
    "content-type": "application/json"
}
base_url = "https://api.ixpdb.net/v1"

os.makedirs("CSV Export", exist_ok=True)

def hit_http_request(relative_url):
    try:
        url = base_url + relative_url
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {relative_url}: {e}")
        return None

def export_file(data, filename):
    dt_str = datetime.now().strftime("%d_%m_%y_%H_%M_%S")
    new_file_name = f"{dt_str}_{filename}.csv"
    path = os.path.join("CSV Export", new_file_name)
    
    df = pd.DataFrame(data)
    df.to_csv(path, index=False)

provider_records = hit_http_request("/provider/list")
organisation_records = []

org_ids = {record['organization_id'] for record in provider_records}
organisation_contact = []
for org_id in org_ids:
    url = f"/organization/{org_id}"
    org_res = hit_http_request(url)
    if org_res:
        organisation_records.append({
            "name": org_res["name"],
            "id": org_res["id"],
            "website": org_res["website"],
            "city": org_res["city"],
            "country": org_res["country"],
            "association": ", ".join(org_res["association"])
        })
        for contact in org_res["contacts"]:
            organisation_contact.append({
                "organization_id": org_res["id"],
                "phone": contact["phone"],
                "name": contact["name"],
                "email": contact["email"],
                "address": contact["address"],
                "city": contact["city"],
                "country": contact["country"]
            })

traffic_records = hit_http_request("/traffic/list")

participant_records = []
participant_response = hit_http_request("/participant/list")
for participant_record in participant_response:
    participant_records.append({
        "asn": participant_record["asn"],
        "ip_addresses": ", ".join(participant_record["ip_addresses"]),
        "ipv6": participant_record["ipv6"],
        "manrs": participant_record["manrs"],
        "name": participant_record["name"],
        "provider_count": participant_record["provider_count"]
    })

organisation_provider_mapping_records = []
for org_record in organisation_records:
    provider_mapping_records = hit_http_request(f"/organization/{org_record['id']}/providers")
    for map_record in provider_mapping_records:
        organisation_provider_mapping_records.append({
            "ProviderID": map_record["id"],
            "OrganizationID": org_record["id"]
        })

provider_networks = []
provider_participants = []
provider_records_v2 = []
for provider in provider_records:
    networks = hit_http_request(f"/provider/{provider['id']}/networks")
    for network in networks:
        provider_networks.append({
            "ProviderID": provider["id"],
            "name": network["name"],
            "addresses": ", ".join(network["addresses"]),
            "route_server_asns": network["route_server_asns"]
        })
    participants = hit_http_request(f"/provider/{provider['id']}/participants")
    for participant in participants:
        provider_participants.append({
            "ProviderID": provider["id"],
            "asn": participant["asn"],
            "name": participant["name"],
            "ipv6": participant["ipv6"],
            "ip_addresses": ", ".join(participant["ip_addresses"])
        })
    provider_records_v2.append({
        "apis_ixfexport": provider["apis"]["ixfexport"],
        "apis_traffic": provider["apis"]["traffic"],
        "city": provider["city"],
        "country": provider["country"],
        "id": provider["id"],
        "location_count": provider["location_count"],
        "looking_glass": ", ".join(provider["looking_glass"]),
        "manrs": provider["manrs"],
        "name": provider["name"],
        "organization_id": provider["organization_id"],
        "participant_count": provider["participant_count"],
        "pdb_id": provider["pdb_id"],
        "updated": provider["updated"],
        "website": provider["website"]
    })

export_file(provider_records_v2, "Providers")
export_file(provider_participants, "Providers_Participants")
export_file(provider_networks, "Providers_Networks")
export_file(organisation_records, "Organizations")
export_file(organisation_provider_mapping_records, "Organizations_Providers")
export_file(traffic_records, "Traffic")
export_file(organisation_contact, "Contacts")