import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import concurrent.futures
import urllib.parse

import os
# Create directory to store the exported files
base_output_dir = "Exchange Export"
os.makedirs("Exchange Export", exist_ok=True)

def export_file(data, file_name):
    dt_str = datetime.now().strftime("%d_%m_%y_%H_%M_%S")
    new_file_name = f"{dt_str}_{file_name}.csv"
    path = os.path.join("Exchange Export", new_file_name)
    df = pd.DataFrame(data)
    df.to_csv(path, index=False)

def get_trimmed_string(input_string):
    if not input_string or input_string.isspace():
        return ""
    return input_string.strip()

def extract_exchange():
    base_url = "https://bgp.he.net"
    url = f"{base_url}/report/exchanges"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extract exchange data
    exchange_table = soup.find_all('table')[0]
    headers = ["index","internetExchange","members","url","isDataAvailable","cc","city","website"]
    exchange_rows = []
    for count, row in enumerate(exchange_table.find('tbody').find_all('tr'), start=1):
        cells = row.find_all('td')
        internet_exchange = get_trimmed_string(cells[0].get_text())
        internet_exchange_url = base_url + cells[0].find('a')['href'] if cells[0].find('a') else ""
        members = get_trimmed_string(cells[1].get_text())
        is_data_available = get_trimmed_string(cells[2].get_text())
        cc = get_trimmed_string(cells[3].get_text())
        city = get_trimmed_string(cells[4].get_text())
        website = get_trimmed_string(cells[5].get_text())

        exchange_rows.append({
            "index": count,
            "internetExchange": internet_exchange,
            "members": members,
            "url": internet_exchange_url,
            "isDataAvailable": is_data_available,
            "cc": cc,
            "city": city,
            "website": website
        })
    
    df = pd.DataFrame(exchange_rows, columns=headers)
    df.reset_index(inplace=True)
    export_file(df, "Exchange Data")

    # Extract participant data
    participant_table = soup.find_all('table')[1]
    participant_rows = []
    for count, row in enumerate(participant_table.find('tbody').find_all('tr'), start=1):
        cells = row.find_all('td')
        asn = get_trimmed_string(cells[0].get_text())
        asn_url = base_url + cells[0].find('a')['href'] if cells[0].find('a') else ""
        name = get_trimmed_string(cells[1].get_text())
        name_url = base_url + cells[1].find('a')['href'] if cells[1].find('a') else ""
        ixes = get_trimmed_string(cells[2].get_text())

        participant_rows.append({
            "index": count,
            "asn": asn,
            "asn_url": asn_url,
            "name": name,
            "name_url": name_url,
            "ixes": ixes
        })

    export_file(participant_rows, "Participant Data")
    print("Data exported successfully")
   
    return df

# Function to extract the members table from each exchange page in parallel
def fetch_exchange_members(exchange_name, exchange_url, parent_index):
    try:
        response = requests.get(exchange_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        members_table = soup.find('table', {'id': 'members'})
        
        if members_table:
            headers = [header.text.strip() for header in members_table.find_all('th')]
            rows = []
            for row in members_table.find_all('tr')[1:]:
                cols = row.find_all('td')
                row_data = [col.text.strip() for col in cols]
                row_data.append(exchange_name)
                row_data.append(parent_index)
                rows.append(row_data)
            return headers, rows
    except requests.RequestException as e:
        print(f"Failed to process {exchange_name} - URL: {exchange_url} - Error: {e}")
    return None, None

# Parallel processing for extracting all exchange members
def extract_exchange_Lines(df, output_csv="combined_exchange_members.csv", max_workers=10):
    # Use the first column as the exchange name if 'Name' is not present
    name_column = df.columns[2]  # Default to the first column as the exchange name
    combined_rows = []
    combined_headers = []
    # Use ThreadPoolExecutor to process URLs concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_exchange = {
            executor.submit(fetch_exchange_members, row[name_column], row['url'], row['index']): row[name_column]
            for _, row in df.iterrows() if pd.notna(row['url'])
        }

        for future in concurrent.futures.as_completed(future_to_exchange):
            exchange_name = future_to_exchange[future]
            try:
                headers, rows = future.result()
                if rows:
                    if not combined_headers:  # Use the first non-empty table's headers
                        combined_headers = headers + ['Exchange Name', 'ParentIndex']
                    combined_rows.extend(rows)
            except Exception as exc:
                print(f"Error processing {exchange_name}: {exc}")

    # Create a DataFrame for the combined data
    if combined_rows:
        combined_df = pd.DataFrame(combined_rows, columns=combined_headers)
        path = os.path.join(base_output_dir, output_csv)
        combined_df.to_csv(path, index=True)
        print(f"All members' data successfully scraped and saved to '{output_csv}'")
    else:
        print("No data found to save in the combined CSV.")

def extract_country():
    base_url = "https://bgp.he.net"
    url = f"{base_url}/report/world"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extract country data
    country_table = soup.find_all('table')[0]
    country_rows = []
    for count, row in enumerate(country_table.find('tbody').find_all('tr'), start=1):
        cells = row.find_all('td')
        name = get_trimmed_string(cells[0].get_text())
        flag_url = base_url + cells[0].find('img')['src'] if cells[0].find('img') else ""
        cc = get_trimmed_string(cells[1].get_text())
        asn_count = get_trimmed_string(cells[2].get_text())
        url = base_url + cells[3].find('a')['href'] if cells[3].find('a') else ""

        country_rows.append({
            "index": count,
            "name": name,
            "flag_url": flag_url,
            "cc": cc,
            "asn_count": asn_count,
            "url": url
        })

    export_file(country_rows, "Countries")

    # Extract country line data
    country_lines = []
    for country in country_rows:
        response = requests.get(country['url'])
        soup = BeautifulSoup(response.content, 'html.parser')
        country_info_wrapper = soup.find_all('div', class_='tabdata')
        if not country_info_wrapper:
            continue

        country_info_table = country_info_wrapper[0].find('table')
        if not country_info_table:
            continue

        for line_count, tr in enumerate(country_info_table.find('tbody').find_all('tr')):
            cells = tr.find_all('td')
            asn = get_trimmed_string(cells[0].get_text())
            asn_url = base_url + cells[0].find('a')['href'] if cells[0].find('a') else ""
            name = get_trimmed_string(cells[1].get_text())
            adjacencies_v4 = get_trimmed_string(cells[2].get_text())
            routes_v4 = get_trimmed_string(cells[3].get_text())
            adjacencies_v6 = get_trimmed_string(cells[4].get_text())
            routes_v6 = get_trimmed_string(cells[5].get_text())

            country_lines.append({
                "cc": country['cc'],
                "countryIndex": country['index'],
                "index": line_count,
                "asn": asn,
                "asn_url": asn_url,
                "name": name,
                "adjacencies_v4": adjacencies_v4,
                "routes_v4": routes_v4,
                "adjacencies_v6": adjacencies_v6,
                "routes_v6": routes_v6
            })

    export_file(country_lines, "Country Lines")

if __name__ == "__main__":
    extract_country()

    exchange_rows = extract_exchange()
    if exchange_rows is not None:
        extract_exchange_Lines(exchange_rows, max_workers=20)


