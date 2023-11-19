import argparse
import pandas as pd
from utils_2 import *

def load_data(file_path):
    # TODO: Load data from CSV file
    df = pd.read_csv(file_path)
    return df


def clean_df(data, file_type):
    if file_type == 'gen':
        energy_val = 'quantity'
    else:
        energy_val = 'Load'
        
    # Convert the timestamp columns to datetime format
    data['StartTime'] = pd.to_datetime(data['StartTime'].str.replace('\+00:00Z', '', regex=True)).dt.strftime('%Y-%m-%d %H:%M:%S')
    data['EndTime'] = pd.to_datetime(data['EndTime'].str.replace('\+00:00Z', '', regex=True)).dt.strftime('%Y-%m-%d %H:%M:%S')
    data['StartTime'] = pd.to_datetime(data['StartTime'])
    data['EndTime'] = pd.to_datetime(data['EndTime'])
    
    # Ensure the data is sorted by time
    data = data.sort_values(by='StartTime')
    
    #cutoff_date = pd.Timestamp('2023-01-01')
    #data = data[data['EndTime'] <= cutoff_date]

    imputed_column = data[energy_val].copy()
    missing_indices = imputed_column.index[imputed_column.isna()]

    for i in missing_indices:
        previous_value = imputed_column.iloc[i - 1] if i > 0 else None
        next_value = imputed_column.iloc[i + 1] if i < len(imputed_column) - 1 else None

        if pd.notna(previous_value) and pd.notna(next_value):
            mean_value = (previous_value + next_value) / 2
            imputed_column.iloc[i] = mean_value

    data[energy_val] = imputed_column
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
    
def cleaning_pipeline():
    # raw_folder = '../data/raw_data/' 
    raw_folder = 'data/raw_data/' 
    countries = ['HU','IT','PO','SP','UK','DE','DK','SE','NE']

    for country in countries:
        print('======================================================')
        print(country)
        print('======================================================')
        print('_____cleaning data_____')

        # generation files
        gen_files_paths = get_generation_files(country, data_folder = raw_folder)
        print('Generation files to be cleaned: ', gen_files_paths)
        for file_path in gen_files_paths:
            gen_df = load_data(file_path)
            gen_df = clean_df(gen_df, 'gen')
            save_data(gen_df, (file_path.replace('raw_data', 'clean_data')).replace('.csv', '')+'_clean.csv')
        print('cleaned generation files')

        # load file
        load_file_path = get_load_file(country, data_folder = raw_folder)
        print('load file to be cleaned: ', load_file_path)
        load_df = load_data(load_file_path)
        load_df = clean_df(load_df, 'Load')

        save_data(load_df, (load_file_path.replace('raw_data', 'clean_data')).replace('.csv', '')+'_clean.csv')

    print('Cleaned data for all countries')


def agregate_data_within_country():

    start_time_array, end_time_array = get_hours_in_year()
    countries = ['HU','IT','PO','SP','UK','DE','DK','SE','NE']

    print('_____aggregating data_____')
    # clean_folder = '../data/clean_data/'
    clean_folder = 'data/clean_data/'

    for country in countries:
        print('======================================================')
        print(country)
        df_country_combined = pd.DataFrame(columns = ['StartTime','EndTime'])

        # add each generation source
        generation_files_path = get_generation_files(country, data_folder = clean_folder)
        print('Data to be merged: ', generation_files_path)
        for i, file_path in enumerate(generation_files_path):
            clean_gen_df = pd.read_csv(file_path)
            source_type = clean_gen_df['PsrType'][0]
            # print(source_type)
            new_quant_column = 'quantity_'+ source_type
            clean_gen_df.rename(columns={'quantity': new_quant_column}, inplace=True)
            clean_gen_df.drop(columns=['PsrType','UnitName', 'hourly_time', 'AreaID', 'EndTime'], inplace=True)
            
            
            df_country_combined['StartTime'] = start_time_array
            df_country_combined['EndTime'] = end_time_array
            df_country_combined = df_country_combined.merge(clean_gen_df[['StartTime', new_quant_column]], how='left', on='StartTime')


        # Aggregate the quantity columns into a new column 'total_quantity'
        df_country_combined['total_green_energy'] = df_country_combined.filter(like='quantity_').sum(axis=1)

        # add load column from the load file
        load_file_path = get_load_file(country, clean_folder)
        clean_load_df = pd.read_csv(load_file_path)
        df_country_combined = df_country_combined.merge(clean_load_df[['StartTime', 'Load']], how='left', on='StartTime')
        name_country_file = country + '_data' + '.csv'

        # final_folder = os.path.join('..','data','final_data')
        final_folder = os.path.join('data','final_data')
        df_country_combined.to_csv(os.path.join(final_folder, name_country_file), index=False)


def agregate_data_among_countries():
    final_folder = os.path.join('data','final_data')
    print('_____Combining datasets of all countries_____')
    # final_df = pd.DataFrame()
    final_files = os.listdir(final_folder)
    final_files = [file for file in final_files if file!='data_merged.csv']

    for i, file_name in enumerate(final_files):
        df_country = pd.read_csv(os.path.join(final_folder, file_name))
        country = file_name[:2]
        # print(country)
        if i == 0:
            final_df = df_country[['StartTime', 'EndTime',  'total_green_energy', 'Load']].copy()
            final_df.rename(columns={
                'total_green_energy': 'green_energy_'+ country,
                'Load': country + '_Load'
            },inplace=True)
        else:
            #combined_df = pd.concat([combined_df, df_country[['total_green_energy', 'Load']]], axis=1)
            final_df = final_df.merge(df_country[['StartTime', 'total_green_energy' , 'Load']], how='left', on='StartTime')
            final_df.rename(columns={
                'total_green_energy': 'green_energy_'+ country,
                'Load': country + '_Load'
            },inplace=True)

    path = os.path.join(final_folder, 'data_merged.csv')
    final_df.to_csv(path, index=False)
    print(f'final dataset saved to {path}')

    return final_df

def extract_features():
    folder_path = 'final_folder'
    filename = 'data_merged.csv'
    file_path = os.path.join(folder_path, filename)
    data = pd.read_csv(file_path)

    print('_____CExtracting features_____')

    # Extract hour of the day
    data['hour_of_day'] = data['StartTime'].dt.hour

    # Define European dates for seasons
    spring_start = pd.to_datetime('2023-03-21')
    summer_start = pd.to_datetime('2023-06-21')
    autumn_start = pd.to_datetime('2023-09-22')
    winter_start = pd.to_datetime('2023-12-21')

    # Create a new column 'season' based on the defined seasons
    data['season'] = pd.cut(
        data['StartTime'],
        bins=[pd.Timestamp.min, spring_start, summer_start, autumn_start, winter_start, pd.Timestamp.max],
        labels=['winter', 'spring', 'summer', 'autumn', 'winter'],
        right=False
    )

    # One-hot encode the 'season' column
    data = pd.get_dummies(data, columns=['season'], drop_first=True)

    # Extract day of the week and create 'weekend' column
    data['day_of_week'] = data['StartTime'].dt.dayofweek
    data['is_weekend'] = pd.get_dummies(data['day_of_week'].isin([5, 6]).astype(int), drop_first=True)

    output_path = os.path.join(folder_path, 'data_extracted.csv')
    data.to_csv(output_path, index=False)

    return data

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

    cleaning_pipeline()
    agregate_data_within_country()
    agregate_data_among_countries()
    extract_features()

if __name__ == "__main__":
    args = parse_arguments()
    main()