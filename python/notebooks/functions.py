import pandas as pd
import os
import sys

os.chdir(sys.path[2])
df = pd.read_csv(r'data/process/res_with_geo_loc.csv')
df[['city', 'state', 'country']] = df['stolen_bikes_place'].str.split(',', expand=True)
df.to_csv(r'data/process/res_stolen_loc_splitted.csv')
