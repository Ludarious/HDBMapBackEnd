import pandas as pd

# Load the CSV file into a DataFrame
df = pd.read_csv('assets/HDB/OriginalHDBResale.csv')

# Display the first few rows of the DataFrame (optional)
print(df.head())

# Keep rows where the specified column contains either "2017" or "2018"
# Replace 'your_column_name' with the actual column name
df_filtered = df[df['month'].astype(str).str.contains("2024")]

# Save the modified DataFrame back to a CSV file
df_filtered.to_csv('assets/HDB/HDBResale(2024).csv', index=False)
