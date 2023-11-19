# fill the area id column
import os 
import pandas as pd
import numpy as np

def fill_area_code(df):

    df['AreaID'].dropna(inplace=True)
    cn_id = df['AreaID'][0]
    print(cn_id)
    df['AreaID'] = cn_id
    
    return df
        

def read_and_concatenate(folder_path):
    # Lists to store DataFrames
    gen_dataframes = []
    load_dataframes = []

    # Iterate over all files in the folder
    list_files = [file for file in os.listdir(folder_path) if file!='test.csv']
    for file in list_files:
        if file.endswith('.csv'):
            # print('---------------------------------')
            # print(file)
            file_path = os.path.join(folder_path, file)
            

            # Read 'gen' files
            if file.startswith('gen'):
                df = pd.read_csv(file_path)
                if file == 'gen_SP_B10.csv':
                    df['AreaID'] = '10YES-REE------0'
                df = fill_area_code(df)
                df['quantity'].fillna(0, inplace=True)
                gen_dataframes.append(df)


            # Read 'load' files
            elif file.startswith('load'):
                df = pd.read_csv(file_path)
                df = fill_area_code(df)
                df['Load'].fillna(0, inplace=True)
                load_dataframes.append(df)

    print('=====================================')
    print('Read all files in the raw_data folder')   
    print('=====================================')

    # Concatenate DataFrames vertically
    gen_concatenated = pd.concat(gen_dataframes, axis=0, ignore_index=True)
    load_concatenated = pd.concat(load_dataframes, axis=0, ignore_index=True)

    # Combine 'gen' and 'load' DataFrames
    combined_dataframe = pd.concat([gen_concatenated, load_concatenated], axis=0, ignore_index=True)

    print('=====================================')
    print('Combined all data frames')   
    print('=====================================')

    return combined_dataframe


def further_processing(df):
    # change date format
    print('=====================================')
    print('Further processing')   
    print('=====================================')
    df['StartTime'] = pd.to_datetime(df['StartTime'].str.replace('\+00:00Z', '', regex=True)).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['EndTime'] = pd.to_datetime(df['EndTime'].str.replace('\+00:00Z', '', regex=True)).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['StartTime'] = pd.to_datetime(df['StartTime'])
    df['EndTime'] = pd.to_datetime(df['EndTime'])

    df['AreaID'] = df['AreaID'].replace({
        '10YHU-MAVIR----U': 'HU',
        '10YIT-GRTN-----B': 'IT',
        '10YPL-AREA-----S': 'PO',
        '10YES-REE------0': 'SP',
        '10Y1001A1001A92E': 'UK',
        '10Y1001A1001A83F': 'DE',
        '10Y1001A1001A65H': 'DK',
        '10YSE-1--------K': 'SE',
        '10YNL----------L': 'NE'
    })

    df.fillna(0, inplace=True)
    df['gen/load'] = 'load'
    df['Load'] = df['Load'].fillna(0)
    df.loc[df['Load']==0,'gen/load']='gen'
    df['power'] = df['quantity'] + df['Load']
    
    # Extract date and hour
    df['Date'] = df['StartTime'].dt.date
    df['Hour'] = df['StartTime'].dt.hour

    # aggregate data per country, load/gen, date and hour
    aggregated_data = df.groupby(['AreaID', 'gen/load', 'Date', 'Hour'])['power'].sum().reset_index()
    aggregated_data['concatenated'] = aggregated_data['AreaID']  + aggregated_data['gen/load']

    pivot = aggregated_data.pivot_table(
    index=['Date', 'Hour'],
    columns=['concatenated'],
    values='power',
    aggfunc='sum'
    )

    # Replace zeros with NaN
    pivot.replace(0, np.nan, inplace=True)

    # Drop rows where all elements are NaN
    pivot.dropna(how='all', inplace=True)

    #pivot=pivot.reset_index()
    pivot['Date'] = pd.to_datetime(pivot['Date'])
    pivot = pivot[pivot['Date'].dt.year == 2022]

    pivot.to_csv('../data/final_data.csv', index=False)
    print('=====================================')
    print('Pivot df saved to final_data')   
    print('=====================================')
    
    return pivot


def main():
    folder_path = '../data/raw_data/'
    data = read_and_concatenate(folder_path)
    pivot = further_processing(data)


if __name__ == "__main__":
    main()