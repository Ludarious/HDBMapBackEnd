import pandas as pd

# Load the CSV file into a DataFrame
df = pd.read_csv('assets/Train Station/MRT_Converted.csv')

# Create an object array (dictionary) mapping IDs to values
object_array = {
    'JURONG EAST MRT STATION': 2,
    'BOUNA VISTA MRT STATION': 2,
    'OUTRAM PARK MRT STATION': 3,
    'RAFFLES PLACE MRT STATION': 2,
    'CITY HALL MRT STATION': 2,
    'BUGIS MRT STATION': 2,
    'PAYA LEBAR MRT STATION': 2,
    'TAMPINES MRT STATION': 2,
    'EXPO MRT STATION': 2,
    'CHOA CHU KANG MRT STATION': 2,
    'WOODLANDS MRT STATION': 2,
    'BISHAN MRT STATION': 2,
    'NEWTON MRT STATION': 2,
    'ORCHARD MRT STATION': 2,
    'DHOBY GHAUT MRT STATION': 3,
    'MARINA BAY MRT STATION': 3,
    'HARBOURFRONT MRT STATION': 2,
    'CHINATOWN MRT STATION': 2,
    'LITTLE INDIA MRT STATION': 2,
    'SERANGOON MRT STATION': 2,
    'SENGKANG MRT STATION': 2,
    'PUNGGOL MRT STATION': 2,
    'BOTANIC GARDENS MRT STATION': 2,
    'CALDECOTT MRT STATION': 2,
    'MACPHERSON MRT STATION': 2,
    'PUNGGOL MRT STATION': 2,
    'PROMENADE MRT STATION': 2,
    'BAYFRONT MRT STATION': 2,
    'STEVENS MRT STATION': 2,
    'BUKIT PANJANG MRT STATION': 2
}

# Map the values from the object array to a new column in the DataFrame
df['NO.OF_STN'] = df['STN_NAM_DE'].map(object_array)
# Fill the blank rows with the value 1
df['NO.OF_STN'].fillna(1,inplace=True)
# Convert the new column to dtype int
df['NO.OF_STN'] = df['NO.OF_STN'].astype(int)
# Save the updated DataFrame back to a CSV file
df.to_csv('updated_file.csv', index=False)