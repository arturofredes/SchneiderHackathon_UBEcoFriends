import argparse
import pandas as pd
from utils_2 import *

def load_data(file_path):
    # TODO: Load data from CSV file
    df = pd.read_csv(file_path)
    return df


def clean_data(data,file_type):
    if file_type == 'gen':
        energy_val = 'quantity'
    else:
        energy_val = 'Load'
        
    # Convert the timestamp columns to datetime format
    data['StartTime'] = pd.to_datetime(data['StartTime'].str.replace('Z',''), format='%Y-%m-%dT%H:%M%z')
    data['EndTime'] = pd.to_datetime(data['EndTime'].str.replace('Z',''), format='%Y-%m-%dT%H:%M%z')

    # Ensure the data is sorted by time
    data = data.sort_values(by='StartTime')

    # Impute missing values by taking the mean of the preceding and following values
    data[energy_val].fillna((data[energy_val].shift() + data[energy_val].shift(-1)) / 2, inplace=True)

    # Create a new column 'hourly_time' to represent the hourly level
    data['hourly_time'] = data['StartTime'].dt.floor('H')

    unique_units = data['UnitName'].unique()
    #print(unique_units)
    if len(unique_units) == 1:
        if file_type == 'gen':
            # Resample the data to an hourly level, preserving all columns
            data_resampled = data.set_index('StartTime').resample('1H').agg({
                'EndTime': 'last',
                'AreaID': 'first',
                'UnitName': 'first',
                'PsrType': 'first',
                energy_val: 'sum',
                'hourly_time': 'last'
            }).reset_index()            
            
        else:
            # Resample the data to an hourly level, preserving all columns
            data_resampled = data.set_index('StartTime').resample('1H').agg({
                'EndTime': 'last',
                'AreaID': 'first',
                'UnitName': 'first',
                energy_val: 'sum',
                'hourly_time': 'last'
            }).reset_index()
        
       # data_resampled = data_resampled.groupby('StartTime')[energy_val].sum().reset_index()
        return data_resampled
    else:
        print("Wrong")
        return data

def preprocess_data(df):
    # TODO: Generate new features, transform existing features, resampling, etc.

    return df_processed

def save_data(df, output_file):
    df.to_csv(output_file, index=False)
    pass

def parse_arguments():
    parser = argparse.ArgumentParser(description='Data processing script for Energy Forecasting Hackathon')
    parser.add_argument(
        '--input_file',
        type=str,
        default='data/raw_data.csv',
        help='Path to the raw data file to process'
    )
    parser.add_argument(
        '--output_file', 
        type=str, 
        default='data/processed_data.csv', 
        help='Path to save the processed data'
    )
    return parser.parse_args()

def main():
    data_folder='../data/raw_data/'
    countries = ['HU','IT','PO','SP','UK','DE','DK','SE','NE']

    for country in countries:
        print('======================================================')
        print(country)
        print('======================================================')
        print('_____cleaning data_____')
        gen_files=get_generation_files(country, data_folder = data_folder)
        print('Generation files to be cleaned: ',gen_files)
        for file in gen_files:
            df=load_data(data_folder+file)
            df=clean_data(df,'gen')
            save_data(df, '../data/clean_data/'+file.replace('.csv','')+'_clean.csv')
        print('cleaned generation files')

        load_file=get_load_file(country, data_folder = data_folder)
        print('load file to be cleaned: ',load_file)
        df=load_data(data_folder+load_file)
        df=clean_data(df,'Load')

        save_data(df, '../data/clean_data/'+load_file.replace('.csv','')+'_clean.csv')


        print('_____aggregating data_____')
        load_file = get_load_file(country,'../data/clean_data/')
        load_df = pd.read_csv('../data/clean_data/'+load_file)

        df_country_combined = pd.DataFrame(columns = [
            'StartTime','EndTime', 'AreaID', 'UnitName'
        ])

        # add each generation source
        generation_files=get_generation_files(country, data_folder = '../data/clean_data/')
        for i, file in enumerate(generation_files):
            df = pd.read_csv('../data/clean_data/'+file)
            source_type = df['PsrType'][0]
            # print(source_type)
            df.rename(columns={'quantity': 'quantity_'+ source_type}, inplace=True)
            df.drop(columns=['PsrType'], inplace=True)
            if i == 0:
                df_country_combined = df

            else:
                df_country_combined = pd.concat([df_country_combined, df['quantity_'+ source_type]], axis=1)

        # Aggregate the quantity columns into a new column 'total_quantity'
        df_country_combined['total_green_energy'] = df_country_combined.filter(like='quantity_').sum(axis=1)

        # add load column from the load file
        df_country_combined = pd.concat([df_country_combined, load_df['Load']], axis=1)

        name_country_file = country + '_data' + '.csv'

        df_country_combined.to_csv(os.path.join('..','data','final_data', name_country_file), index=False)

        combined_df = pd.DataFrame()
        folder = os.path.join('data', 'final_data')
        files = os.listdir(folder)
        files = [file for file in files if file!='data_def.csv']

        for i, file in enumerate(files):
            df_country = pd.read_csv(os.path.join(folder, file))
            country = file[:2]
            # print(country)
            if i == 0:
                combined_df = df_country[['StartTime', 'EndTime',  'total_green_energy', 'Load']]
                combined_df.rename(columns={
                    'total_green_energy': 'green_energy_'+ country,
                    'Load': country + '_Load'
                },inplace=True)
            else:
                combined_df = pd.concat([combined_df, df_country[['total_green_energy', 'Load']]], axis=1)
                combined_df.rename(columns={
                    'total_green_energy': 'green_energy_'+ country,
                    'Load': country + '_Load'
                },inplace=True)

        combined_df.to_csv(os.path.join(folder, 'data_def.csv'), index=False)

if __name__ == "__main__":
    args = parse_arguments()
    main()