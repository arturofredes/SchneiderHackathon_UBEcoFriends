import argparse
import pandas as pd
from adriana import *
<<<<<<< HEAD
=======
from extra_preprocessing import *
>>>>>>> 7f21385563bae5cce1b99a30b117c111687fbfa9

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

    if len(unique_units) == 1:
        # Resample the data to an hourly level, preserving all columns
        data_resampled = data.set_index('StartTime').resample('1H').agg({
            'EndTime': 'last',
            'AreaID': 'first',
            'UnitName': 'first',
            energy_val: 'sum',
            'hourly_time': 'last'
        }).reset_index()
        
        data_resampled = data_resampled.groupby('StartTime')[energy_val].sum().reset_index()
        return data_resampled
    else:
        print("Wrong")
        return data

def preprocess_data(df):
    # TODO: Generate new features, transform existing features, resampling, etc.

    return df_processed

def save_data(df, output_file):
    df.to_csv(output_file)
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

def main(input_file, output_file):
    data_folder='../data/raw_data'
    countries=get_list_countries(data_folder=data_folder)
    for i, country in countries:
        gen_files=get_generation_files(country, data_folder = data_folder)
        for file in gen_files:
            df=load_data(file)
            df=clean_data(df,'gen')
            save_data(df, '../data/clean_data/'+file+'clean')

        load_file=get_load_file(country, data_folder = 'data')
        df=load_data(load_file)
        df=clean_data(df,'Load')
        save_data(df, '../data/clean_data/'+load_file+'clean')

if __name__ == "__main__":
    args = parse_arguments()
    main(args.input_file, args.output_file)