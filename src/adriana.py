from data_processing import *
import pandas as pd
import os


def get_list_countries(data_folder='data'):

    files = os.listdir(data_folder)
    countries = []
    for file_name in files:
        if file_name.startswith('load'):
            country = file_name.split('_')[1][:-4]
            countries.append(country)
    countries = list(set(countries))

    return countries


def get_generation_files(country, data_folder = 'data'):
    files = os.listdir(data_folder)
    gen_file_list = [file for file in files if file.startswith(f'gen_{country}')]
    return gen_file_list


def get_load_file(country, data_folder = 'data'):
    files = os.listdir(data_folder)
    for file in files:
        if file.startswith(f'load_{country}'):
            return file



