import pandas as pd
import os



def get_generation_files(country, data_folder):
    files = os.listdir(data_folder)
    gen_file_list = [file for file in files if file.startswith(f'gen_{country}')]
    return gen_file_list


def get_load_file(country, data_folder):
    files = os.listdir(data_folder)
    for file in files:
        if file.startswith(f'load_{country}'):
            return file


import pandas as pd
def get_hours_in_year():
    # Especifica el rango de fechas desde el primer día de 2022 hasta el último
    start_date = "2022-01-01 00:00:00"
    end_date = "2022-12-31 23:00:00"

    # Crea un rango de fechas cada hora
    start_time_range = pd.date_range(start=start_date, end=end_date, freq='H')

    # Convierte el rango de fechas en un array de strings en el formato deseado
    start_time_array = start_time_range.strftime('%Y-%m-%d %H:%M:%S%z').to_numpy()

    # Imprime el array

    # Especifica el rango de fechas desde el primer día de 2022 hasta el último
    start_date = "2022-01-01 01:00:00"
    end_date = "2023-01-01 00:00:00"

    # Crea un rango de fechas cada hora
    end_time_range = pd.date_range(start=start_date, end=end_date, freq='H')

    # Convierte el rango de fechas en un array de strings en el formato deseado
    end_time_array = end_time_range.strftime('%Y-%m-%d %H:%M:%S%z').to_numpy()

    # Imprime el array
    
    return start_time_array, end_time_array
