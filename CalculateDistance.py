# import pandas as pd
# import numpy as np
# from scipy.spatial import cKDTree #python -m pip install scipy

# # Load CSV files
# df_A = pd.read_csv('assets/HDB/HDBResale_converted.csv')
# df_B = pd.read_csv('assets/Train Station/MRT_Converted.csv')
# df_A = df_A.dropna() 
# # Convert degrees to radians
# df_A[['Latitude', 'Longitude']] = np.radians(df_A[['Latitude', 'Longitude']])
# df_B[['Latitude', 'Longitude']] = np.radians(df_B[['Latitude', 'Longitude']])

# # Earth's radius in km
# R = 6371

# # Convert lat/lon to 3D Cartesian coordinates
# def latlon_to_cartesian(lat, lon):
#     x = R * np.cos(lat) * np.cos(lon)
#     y = R * np.cos(lat) * np.sin(lon)
#     z = R * np.sin(lat)
#     return x, y, z

# # Apply the conversion
# df_A['x'], df_A['y'], df_A['z'] = latlon_to_cartesian(df_A['Latitude'], df_A['Longitude'])
# df_B['x'], df_B['y'], df_B['z'] = latlon_to_cartesian(df_B['Latitude'], df_B['Longitude'])

# # Build KD-Tree for B points
# tree = cKDTree(df_B[['x', 'y', 'z']])

# # Iterate through each point in A
# for i, row_A in df_A.iterrows():
#     print (i)
#     # Query the tree for points within 3km radius
#     distances, indices = tree.query([row_A[['x', 'y', 'z']]], k=len(df_B))
    
#     # Filter out invalid distances (those beyond the largest valid index)
#     valid_indices = indices[distances < np.inf]
#     valid_distances = distances[distances < np.inf]
    
#     # Find the closest point
#     if len(valid_distances) > 0:
#         min_index = np.argmin(valid_distances)
#         closest_point_index = valid_indices[min_index]
#         closest_point = df_B.iloc[closest_point_index]

#         # Update df_A with closest point information
#         df_A.at[i, 'closest_mrt_stn'] = closest_point['STN_NAM_DE']
#         df_A.at[i, 'mrt_stn_distance_km'] = valid_distances[min_index]

# df_A = df_A.drop(columns=['x', 'y','z'])

# # Save the results to a CSV file
# df_A.to_csv('closest_pinpoints.csv', index=False)

# print("The closest points have been calculated and saved to 'closest_pinpoints.csv'")

import pandas as pd
import numpy as np
from scipy.spatial import cKDTree

# Load CSV files
df_A = pd.read_csv('assets/HDB/HDBResale_converted.csv')
df_B = pd.read_csv('assets/Train Station/MRT_Converted.csv')

# Drop rows with NaN values in df_A
df_A = df_A.dropna()

# Convert degrees to radians
df_A[['Latitude', 'Longitude']] = np.radians(df_A[['Latitude', 'Longitude']])
df_B[['Latitude', 'Longitude']] = np.radians(df_B[['Latitude', 'Longitude']])

# Earth's radius in km
R = 6371

# Convert lat/lon to 3D Cartesian coordinates
def latlon_to_cartesian(lat, lon):
    x = R * np.cos(lat) * np.cos(lon)
    y = R * np.cos(lat) * np.sin(lon)
    z = R * np.sin(lat)
    return x, y, z

# Apply the conversion to Cartesian coordinates
df_A['x'], df_A['y'], df_A['z'] = latlon_to_cartesian(df_A['Latitude'], df_A['Longitude'])
df_B['x'], df_B['y'], df_B['z'] = latlon_to_cartesian(df_B['Latitude'], df_B['Longitude'])

# Build KD-Tree for B points
tree = cKDTree(df_B[['x', 'y', 'z']])

# Initialize columns in df_A for closest point information
df_A['closest_mrt_stn'] = None
df_A['mrt_stn_distance_km'] = np.nan

# Iterate through each point in A
for i, row_A in df_A.iterrows():
    print(i)
    # Query the tree for the nearest point
    distance, index = tree.query([row_A[['x', 'y', 'z']]], k=1)
    
    # Check if a valid point is found
    if distance[0] < np.inf:
        closest_point = df_B.iloc[index[0]]
        
        # Update df_A with closest point information
        df_A.at[i, 'closest_mrt_stn'] = closest_point['STN_NAM_DE']
        df_A.at[i, 'mrt_stn_distance_km'] = distance[0]

# Convert latitude and longitude back to degrees
df_A[['Latitude', 'Longitude']] = np.degrees(df_A[['Latitude', 'Longitude']])

# Drop Cartesian coordinates columns
df_A = df_A.drop(columns=['x', 'y', 'z'])

# Save the results to a CSV file
df_A.to_csv('closest_pinpoints.csv', index=False)

print("The closest points have been calculated and saved to 'closest_pinpoints.csv'")