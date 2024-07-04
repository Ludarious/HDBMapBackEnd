import pandas as pd

# Step 1: Read the CSV file
df = pd.read_csv('assets/HDB/HDBResale_converted.csv')

df_cleaned = df.dropna()

missing = df_cleaned.isnull().sum()

# Display the result
print("Missing values in each column:")
print(missing)
df.info()