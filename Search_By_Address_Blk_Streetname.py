from src.SVY21_to_WGS84 import SVY21_to_WGS84
import asyncio
#todo 
#HDB, generate postal code to lat lng
#HDB, generate district

#MRT, convert xy to lat lng (D)

#Hawker, NA

#Bus Stop, convert xy to lat lng (D)
from dotenv import load_dotenv, dotenv_values 
import asyncio
import aiohttp
import pandas as pd
import os
import time

# Read the CSV file
csv_file = 'assets/HDB/HDBResale(2024).csv'  # replace with your CSV file path
df = pd.read_csv(csv_file)

# Define the maximum requests per second and the pause interval
MAX_REQUESTS_PER_SECOND = 240
PAUSE_INTERVAL = 10  # seconds

# Initialize a counter for the number of requests made
request_counter = 0

# Define the district mapping
district_mapping = {
    '01': 1, '02': 1, '03': 1, '04': 1, '05': 1, '06': 1,
    '07': 2, '08': 2,
    '14': 3, '15': 3, '16': 3,
    '09': 4, '10': 4,
    '11': 5, '12': 5, '13': 5,
    '17': 6,
    '18': 7, '19': 7,
    '20': 8, '21': 8,
    '22': 9, '23': 9,
    '24': 10, '25': 10, '26': 10, '27': 10,
    '28': 11, '29': 11, '30': 11,
    '31': 12, '32': 12, '33': 12,
    '34': 13, '35': 13, '36': 13, '37': 13,
    '38': 14, '39': 14, '40': 14, '41': 14,
    '42': 15, '43': 15, '44': 15, '45': 15,
    '46': 16, '47': 16, '48': 16,
    '49': 17, '50': 17, '81': 17,
    '51': 18, '52': 18,
    '53': 19, '54': 19, '55': 19, '82': 19,
    '56': 20, '57': 20,
    '58': 21, '59': 21,
    '60': 22, '61': 22, '62': 22, '63': 22, '64': 22,
    '65': 23, '66': 23, '67': 23, '68': 23,
    '69': 24, '70': 24, '71': 24,
    '72': 25, '73': 25,
    '77': 26, '78': 26,
    '75': 27, '76': 27,
    '79': 28, '80': 28,
}

# Create a function to get the district from the postal code
def get_district(postal_code):
    prefix = postal_code[:2]
    return district_mapping.get(prefix, None)

# Create a function to make asynchronous API calls
async def fetch(session, block, street_name):
    global request_counter
    search_val = f"{block} {street_name}"
    url = f"https://www.onemap.gov.sg/api/common/elastic/search?searchVal={search_val}&returnGeom=Y&getAddrDetails=Y&pageNum=1"
    headers = {"Authorization": os.getenv("ONE_MAP_API_KEY")}
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                result = await response.json()
                if 'results' in result and len(result['results']) > 0:
                    data = result['results'][0]
                    print(f"Request {request_counter}: {data}")
                    request_counter += 1
                    return {
                        "Postal": data.get("POSTAL", ""),
                        "Latitude": data.get("LATITUDE", ""),
                        "Longitude": data.get("LONGITUDE", ""),
                        "District": get_district(data.get("POSTAL", ""))
                    }
                else:
                    print(f"Request {request_counter}: No results for {search_val}")
                    request_counter += 1
                    return None
            else:
                print(f"Request {request_counter}: HTTP error {response.status} for {search_val}")
                request_counter += 1
                return None
    except Exception as e:
        print(f"Request {request_counter}: Exception for {search_val}: {e}")
        request_counter += 1
        return None

# Create a function to process a batch of requests
async def process_batch(session, batch):
    tasks = [fetch(session, row['block'], row['street_name']) for index, row in batch.iterrows()]
    return await asyncio.gather(*tasks)

# Main function to orchestrate the process
async def main():
    results = []
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(df), MAX_REQUESTS_PER_SECOND):
            batch = df.iloc[i:i + MAX_REQUESTS_PER_SECOND]
            responses = await process_batch(session, batch)
            results.extend(responses)
            await asyncio.sleep(PAUSE_INTERVAL)  # pause to respect rate limit

    # Process the results
    for i, response in enumerate(results):
        if response:
            df.at[i, 'Postal'] = response['Postal']
            df.at[i, 'Latitude'] = response['Latitude']
            df.at[i, 'Longitude'] = response['Longitude']
            df.at[i, 'District'] = response['District'] if response['District'] is not None else None
        else:
            df.at[i, 'Postal'] = None
            df.at[i, 'Latitude'] = None
            df.at[i, 'Longitude'] = None
            df.at[i, 'District'] = None

    # Ensure 'District' column is of integer type
    df['District'] = df['District'].astype('Int64')

    # Save the results to a new CSV file
    df.to_csv('HDBResale(2024)_converted.csv', index=False)
    print("Conversion completed and saved to 'converted_addresses.csv'")

# Run the script
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

    # # Load the CSV file
    # df = pd.read_csv('assets\Bus Stop\converted_coordinates.csv')

    # # Check for missing data in 'Latitude' and 'Longitude' columns
    # missing_latitude = df['Latitude'].isnull().sum()
    # missing_longitude = df['Longitude'].isnull().sum()

    # print(f"Number of missing 'Latitude' values: {missing_latitude}")
    # print(f"Number of missing 'Longitude' values: {missing_longitude}")

    # # Optionally, print rows with missing data
    # missing_data = df[df['Latitude'].isnull() | df['Longitude'].isnull()]
    # print("Rows with missing 'Latitude' or 'Longitude':")
    # print(missing_data)

    # # Optionally, save rows with missing data to a new CSV file for further inspection
    # missing_data.to_csv('missing_coordinates.csv', index=False)