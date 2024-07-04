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
csv_file = 'assets/Train Station/RapidTransitSystemStation.csv'  # replace with your CSV file path
df = pd.read_csv(csv_file)

# Define the maximum requests per second and the pause interval
MAX_REQUESTS_PER_SECOND = 240
PAUSE_INTERVAL = 10  # seconds

# Initialize a counter for the number of requests made
request_counter = 0


# Create a function to make asynchronous API calls
async def fetch(session, location):
    global request_counter
    search_val = f"{location}"
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
                        "Latitude": data.get("LATITUDE", ""),
                        "Longitude": data.get("LONGITUDE", "")
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
    tasks = [fetch(session, row['STN_NAM_DE']) for index, row in batch.iterrows()]
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
            df.at[i, 'Latitude'] = response['Latitude']
            df.at[i, 'Longitude'] = response['Longitude']
        else:
            df.at[i, 'Latitude'] = None
            df.at[i, 'Longitude'] = None


    # Save the results to a new CSV file
    df.to_csv('converted_addresses.csv', index=False)
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