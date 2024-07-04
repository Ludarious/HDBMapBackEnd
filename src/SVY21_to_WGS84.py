import requests
# importing os module for environment variables
import os
# importing necessary functions from dotenv library
from dotenv import load_dotenv, dotenv_values 
import pandas as pd
import json
import asyncio
import aiohttp
import time 

class SVY21_to_WGS84():


    # Read the CSV file
    csv_file = './assets/Bus Stop/BusStop.csv'  # replace with your CSV file path
    df = pd.read_csv(csv_file)

    # Define the maximum requests per second and the pause interval
    MAX_REQUESTS_PER_SECOND = 240
    PAUSE_INTERVAL = 10  # seconds

    # Create a function to make asynchronous API calls
    async def fetch(session, x, y):
        url = f"https://www.onemap.gov.sg/api/common/convert/3414to4326?X={x}&Y={y}"
        headers = {"Authorization": os.getenv("ONE_MAP_API_KEY")}
        async with session.get(url, headers=headers) as response:
            return await response.json()

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
                time.sleep(PAUSE_INTERVAL)  # pause to respect rate limit

        # Process the results
        for i, response in enumerate(results):
            df.at[i, 'Latitude'] = response['latitude']
            df.at[i, 'Longitude'] = response['longitude']

        # Save the results to a new CSV file
        df.to_csv('converted_coordinates.csv', index=False)

    # Run the script
    if __name__ == "__main__":
        asyncio.run(main())

    




