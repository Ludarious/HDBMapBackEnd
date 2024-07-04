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
csv_file = 'assets/HDB/hdb first 10.csv'  # replace with your CSV file path
df = pd.read_csv(csv_file)

# Define the maximum requests per second and the pause interval
MAX_REQUESTS_PER_SECOND = 240
PAUSE_INTERVAL = 10  # seconds

# Initialize a counter for the number of requests made
request_counter = 0

# Create a function to make asynchronous API calls
async def fetch(session, x, y):
    global request_counter
    url = f"https://www.onemap.gov.sg/api/common/convert/3414to4326?X={x}&Y={y}"
    headers = {"Authorization": os.getenv("ONE_MAP_API_KEY")}
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                result = await response.json()
                if 'latitude' in result and 'longitude' in result:
                    print(f"Request {request_counter}: Converted (X={x}, Y={y}) to (Latitude={result['latitude']}, Longitude={result['longitude']})")
                    request_counter += 1
                    return result
                else:
                    print(f"Request {request_counter}: Error converting (X={x}, Y={y}): {result}")
                    request_counter += 1
                    return None
            else:
                print(f"Request {request_counter}: HTTP error {response.status} for (X={x}, Y={y})")
                request_counter += 1
                return None
    except Exception as e:
        print(f"Request {request_counter}: Exception for (X={x}, Y={y}): {e}")
        request_counter += 1
        return None

# Create a function to process a batch of requests
async def process_batch(session, batch):
    tasks = [fetch(session, row['X'], row['Y']) for index, row in batch.iterrows()]
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
            df.at[i, 'Latitude'] = response['latitude']
            df.at[i, 'Longitude'] = response['longitude']
        else:
            df.at[i, 'Latitude'] = None
            df.at[i, 'Longitude'] = None

    # Save the results to a new CSV file
    df.to_csv('converted_coordinates.csv', index=False)
    print("Conversion completed and saved to 'converted_coordinates.csv'")


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