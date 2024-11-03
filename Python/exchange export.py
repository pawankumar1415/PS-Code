import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import concurrent.futures
import os

# Create directory to store the exported files
base_output_dir = "Exchange Export"
os.makedirs(base_output_dir, exist_ok=True)

def export_file(df, file_name):
    """Exports a DataFrame to a CSV file with a timestamped filename."""
    dt_str = datetime.now().strftime("%d_%m_%y_%H_%M_%S")
    new_file_name = f"{dt_str}_{file_name}.csv"
    path = os.path.join(base_output_dir, new_file_name)
    df.to_csv(path, index=False)
    print(f"Data successfully exported to '{new_file_name}'")

def get_trimmed_string(input_string):
    """Returns a trimmed string or an empty string if the input is None or whitespace."""
    if not input_string or input_string.isspace():
        return ""
    return input_string.strip()

def extract_exchange():
    """Extracts exchange data from the specified URL."""
    base_url = "https://bgp.he.net"
    url = f"{base_url}/report/exchanges"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extract exchange data
    exchange_table = soup.find_all('table')[0]
    headers = ["index", "internetExchange", "members", "url", "isDataAvailable", "cc", "city", "website"]
    exchange_rows = []
    for count, row in enumerate(exchange_table.find('tbody').find_all('tr'), start=1):
        cells = row.find_all('td')
        exchange_rows.append({
            "index": count,
            "internetExchange": get_trimmed_string(cells[0].get_text()),
            "members": get_trimmed_string(cells[1].get_text()),
            "url": base_url + cells[0].find('a')['href'] if cells[0].find('a') else "",
            "isDataAvailable": get_trimmed_string(cells[2].get_text()),
            "cc": get_trimmed_string(cells[3].get_text()),
            "city": get_trimmed_string(cells[4].get_text()),
            "website": get_trimmed_string(cells[5].get_text())
        })

    exchange_df = pd.DataFrame(exchange_rows, columns=headers)
    export_file(exchange_df, "Exchange_Data")
    return exchange_df

def fetch_exchange_members(exchange_name, exchange_url, parent_index):
    """Extracts members data for a single exchange."""
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
                row_data = {header: col.text.strip() for header, col in zip(headers, cols)}
                row_data['Exchange Name'] = exchange_name
                row_data['ParentIndex'] = parent_index
                rows.append(row_data)
            return headers, rows
    except requests.RequestException as e:
        print(f"Failed to process {exchange_name} - URL: {exchange_url} - Error: {e}")
    return None, None

def extract_exchange_Lines(df, max_workers=10):
    """Extracts member data for all exchanges concurrently using ThreadPoolExecutor."""
    name_column = "internetExchange"
    combined_rows = []

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
                    combined_rows.extend(rows)
            except Exception as exc:
                print(f"Error processing {exchange_name}: {exc}")

    # Create a DataFrame for the combined data
    if combined_rows:
        combined_df = pd.DataFrame(combined_rows)
        export_file(combined_df, "Exchange_Members")
        return combined_df
    else:
        print("No data found to save in Exchange Members.")
        return pd.DataFrame()

def extract_country():
    """Extracts country data from the specified URL."""
    base_url = "https://bgp.he.net"
    url = f"{base_url}/report/world"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extract country data
    country_table = soup.find_all('table')[0]
    country_rows = []
    for count, row in enumerate(country_table.find('tbody').find_all('tr'), start=1):
        cells = row.find_all('td')
        country_rows.append({
            "index": count,
            "name": get_trimmed_string(cells[0].get_text()),
            "flag_url": base_url + cells[0].find('img')['src'] if cells[0].find('img') else "",
            "cc": get_trimmed_string(cells[1].get_text()),
            "asn_count": get_trimmed_string(cells[2].get_text()),
            "url": base_url + cells[3].find('a')['href'] if cells[3].find('a') else ""
        })

    country_df = pd.DataFrame(country_rows)
    export_file(country_df, "Countries")
    return country_df

def extract_country_lines(country_df):
    """Extracts country line data using the country DataFrame."""
    base_url = "https://bgp.he.net"
    country_lines = []

    for _, country in country_df.iterrows():
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
            country_lines.append({
                "cc": country['cc'],
                "countryIndex": country['index'],
                "index": line_count,
                "asn": get_trimmed_string(cells[0].get_text()),
                "asn_url": base_url + cells[0].find('a')['href'] if cells[0].find('a') else "",
                "name": get_trimmed_string(cells[1].get_text()),
                "adjacencies_v4": get_trimmed_string(cells[2].get_text()),
                "routes_v4": get_trimmed_string(cells[3].get_text()),
                "adjacencies_v6": get_trimmed_string(cells[4].get_text()),
                "routes_v6": get_trimmed_string(cells[5].get_text())
            })

    country_lines_df = pd.DataFrame(country_lines)
    export_file(country_lines_df, "Country_Lines")
    return country_lines_df

def consolidate_data(exchange_df, exchange_members_df, country_lines_df, country_df):
    """Consolidates data from multiple sources into a single DataFrame."""
    # Merge exchange members with exchange data
    consolidated_df = exchange_members_df.merge(exchange_df, left_on='ParentIndex', right_on='index', suffixes=('_member', '_exchange'))

    # Merge with country lines using ASN column
    consolidated_df = consolidated_df.merge(country_lines_df, how='left', left_on='ASN', right_on='asn')

    # Rename conflicting `cc` columns for clarity
    consolidated_df.rename(columns={'cc_x': 'exchange_cc', 'cc_y': 'country_line_cc'}, inplace=True)

    # Merge with country data to get the country name using the country line `cc`
    consolidated_df = consolidated_df.merge(country_df[['cc', 'name']], left_on='country_line_cc', right_on='cc', suffixes=('', '_country'))

    # Drop duplicate `cc` column after the merge
    consolidated_df.drop(columns=['cc'], inplace=True)

    # Export the consolidated DataFrame
    export_file(consolidated_df, "Consolidated_Data")
    return consolidated_df


if __name__ == "__main__":
    country_df = extract_country()
    country_lines_df = extract_country_lines(country_df)
    exchange_df = extract_exchange()
    if exchange_df is not None:
        exchange_members_df = extract_exchange_Lines(exchange_df, max_workers=20)
        if not exchange_members_df.empty:
            consolidated_df = consolidate_data(exchange_df, exchange_members_df, country_lines_df, country_df)
