# importing pandas 
import pandas as pd 
  
# merging two csv files 
df = pd.concat( 
    map(pd.read_csv, ['HDBResale(2017)_converted.csv', 'HDBResale(2018)_converted.csv','HDBResale(2019)_converted.csv','HDBResale(2020)_converted.csv','HDBResale(2021)_converted.csv','HDBResale(2022)_converted.csv','HDBResale(2023)_converted.csv','HDBResale(2024)_converted.csv']), ignore_index=True) 
print(df) 
df.to_csv('HDBResale_converted.csv', index=False)