# -*- coding: utf-8 -*-
"""
Created on Thu Mar 17 20:53:02 2022

@author: fuzzy
"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from tools import *

NEW_STATION_COLS = ['ID', 'Name', 'City', 'Latitude', 'Longitude', 'DP Capacity', 'Landmark', 'Online Date']
STATION_COLUMNS = dict()
for i in range(len(STATION_COLUMNS)):
    STATION_COLUMNS[STATION_COLUMNS[i]] = list()

STATION_COLUMNS['ID'] = 'id'
STATION_COLUMNS['Name'] = 'name'
STATION_COLUMNS['City'] = 'city'
STATION_COLUMNS['Latitude'] = 'latitude'
STATION_COLUMNS['Longitude'] = 'longitude'
STATION_COLUMNS['DP Capacity'] = 'dpcapacity'
STATION_COLUMNS['Landmark'] = 'landmark'
STATION_COLUMNS['Online Date'] = ['online date', 'online_date', 'dateCreated']

def station_columns():
    station_cols = list()
    for p in STATION_FILES:
        with p.open() as f:
            new_list = [s.strip().strip('"') for s in next(f).strip().split(',')]
            while len(new_list) < MAX_COLS:
                new_list.append('')
            station_cols.append(new_list)
    return station_cols

def combine_station_files():
    df_sum = pd.DataFrame()
    for f in STATION_FILES:
    #     if not f.name == 'Divvy_Stations_2017_Q3Q4.csv':
        print(f'Processing {f}')
        df = pd.read_csv(f)
        new_cols = list()
        for s in df.columns:
            try:
                k = reverse_lookup(STATION_COLUMNS, s)
                assert(k)
            except AssertionError as e:
                print(f'Bad lookup value: {s}')
                print(f'File: {f.name}')
            new_cols.append(k)
        if "Unnamed: 7" in df.columns:
            print("Dropping column...")
    #         df = df.drop("Unnamed: 7", axis=1)
        try:
            df.columns = new_cols
            df.reindex(columns=NEW_STATION_COLS)
            print(df.head())
        except ValueError as e:
            print(f'Value Error: {f}')
            print_exc()
        if not len(df_sum):
            df_sum = df
        else:
            df_sum = pd.concat([df_sum, df])

    #     print(df_sum.describe())
        print(f'{len(df_sum)=}\n')
    print(df_sum)
    df = df_sum.drop(columns=[None], axis=1)
    df.sort_values("ID", inplace=True)
    df["Name"] = df["Name"].replace(' (\*)', '')
#    df = df.drop(columns=[pd.NA], axis=1)
    return df

def condense_stations(df:pd.DataFrame):
    new_df = pd.DataFrame()
    print("Running")
    IDs = df['ID'].unique()
    print(f'{len(IDs)=}')
    print()
    for i in IDs:
        print(f'{i=}')
        df2 = df[df['ID']==i].sort_values("Online Date", ascending=False)
    #     df2.reindex(index=range(len(df2)))
        print(df2)
        print()
        
        L = list()
        for s in df2["Online Date"]:
            try:
                L.append(parse(s).date())
            except TypeError:
                pass
        L = set(L)
        print(f'Online Dates: {L}')
        print(f'Most recent date: {max(L)}')
        most_recent_date = max(L)
        
        names = list()
        for s, n in zip(df2["Online Date"], df2["Name"]):
            try:
    #             print(type(s))
    #             print(f'{s=}\n{n=}')
                if type(s) is str and parse(s).date() == most_recent_date:
                    names.append(n)
            except:
                pass
        print(f'{names=}')
        unique_names = list(set(names))
        unique_names.sort()
        print(f'{unique_names=}')
        d = dict()
        C = ["Name","Popularity"]
        for c in C:
            d[c] = list()
        for n in unique_names:
            d["Name"].append(n)
            d["Popularity"].append(len(df2[df2["Name"] == n]))
        print(d)
        popular_names = pd.DataFrame(d).sort_values("Popularity", ascending=False)
        print(popular_names)
        name = popular_names["Name"][0]
        print(f'{name=}')
        
        C = df2['City'].dropna()
        city = None
        print(f'{C=}')
        if C.any():
            for c in C:
                city = c
                break
    #     print(f'{type(city)=}')
    
        lats = df2["Latitude"]
        diff = max(lats) - min(lats) if len(df2) > 1 else 0
        if diff > 0.01:
            print("WARNING: Difference in latitude > 0.01")
            print(f'{len(df2)=}')
            print(f'{max(lats)=}')
            print(f'{min(lats)=}')
            print(f'{diff=}')
            print(i)
            print()
        lons = df2["Longitude"]
        diff = max(lons) - min(lons) if len(df2) > 1 else 0
        if diff > 0.01:
            print("WARNING: Difference in longitude > 0.01")
            print(f'{len(df2)=}')
            print(f'{max(lons)=}')
            print(f'{min(lons)=}')
            print(f'{diff=}')
            print(i)
            print()
        if (lats < 40).any() or (lats > 43).any() or (lons < -89).any() or (lons > -85).any():
            print("WARNING: Unreasonable latitude and/or longitude!")
            print(i)
            print()
            
        latitude = mean(lats)
        longitude = mean(lons)
        print(f'{city=}')
        print(f'{latitude=}')
        print(f'{longitude=}')
        
        C = df2['DP Capacity'].dropna()
        dpcapacity = None
    #     print(f'{C=}')
        if C.any():
            for c in C:
                dpcapacity = c
                break
                
        C = df2['Landmark'].dropna()
        landmark = None
    #     print(f'{C=}')
        if C.any():
            for c in C:
                landmark = c
                break
                
        d = {"ID": [i],
             "Name": [name],
             "Latitude": [latitude],
             "Longitude": [longitude],
             "City": [city],
             "DP Capacity": [dpcapacity],
             "Landmark": [landmark],
             "Online Date": [most_recent_date]}
                
        row = pd.DataFrame(d, index=[i]) # dtype=[("ID", int), ("Name", str), ("Latitude", float), ("Longitude", float), ("City", str),
                                           #     ("DP Capacity", int), ("Landmark", float), ("Online Date", str)])
                
        new_df = pd.concat([new_df, row])
        
        print()
    
    #     if i > 5:
    #         break
            
    return new_df

if __name__ == '__main__':
    print("Running `stations`")
