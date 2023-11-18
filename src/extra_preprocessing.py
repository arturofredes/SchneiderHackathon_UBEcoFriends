import pandas as pd

def process_energy_data(data,file_type):
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

    if len(extract_unit_names(data)[0]) == 1:
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

def extract_unit_names(data):
    # Extract unique unit names
    unique_units = data['UnitName'].unique()

    # Calculate the time interval between each consecutive data point
    time_intervals = (data['StartTime'].dt.floor('H')).diff().unique()

    return unique_units, time_intervals