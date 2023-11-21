import pandas as pd
import argparse
import numpy as np
import json
from model_training import *

def load_data(file_path):
    # TODO: Load processed data from CSV file

    df = pd.read_csv(file_path)
    df.interpolate(method='linear', limit_direction='both', inplace=True)
    
    return df

def load_model(model_path):
    # TODO: Load the trained model
    loaded_model = load_model(model_path)
    return loaded_model

def make_predictions(df, model):
    # TODO: Use the model to make predictions on the test data


    predictions = model.predict(sequences_test)
    predicted_labels = np.argmax(predictions, axis=1)
    result_dict = {"target": {}}
    for i, label in enumerate(predicted_labels):
        result_dict["target"][str(i + 1)] = int(label)

    return result_dict


def save_predictions(result_dict, json_file_path):
    # TODO: Save predictions to a JSON file

    # Save the dictionary as JSON
    with open(json_file_path, 'w') as json_file:
        json.dump(result_dict, json_file)

    print(f"Result dictionary saved to {json_file_path}")

    pass

def parse_arguments():
    parser = argparse.ArgumentParser(description='Prediction script for Energy Forecasting Hackathon')
    parser.add_argument(
        '--input_file', 
        type=str, 
        default='data/test_data.csv', 
        help='Path to the test data file to make predictions'
    )
    parser.add_argument(
        '--model_file', 
        type=str, 
        default='models/model.pkl',
        help='Path to the trained model file'
    )
    parser.add_argument(
        '--output_file', 
        type=str, 
        default='predictions/predictions.json', 
        help='Path to save the predictions'
    )
    return parser.parse_args()

def main(file_path, model_file, output_file):
    df = load_data(file_path)
    model = load_model(model_file)
    predictions = make_predictions(df, model)
    save_predictions(predictions, output_file)

if __name__ == "__main__":
    args = parse_arguments()
    main(args.input_file, args.model_file, args.output_file)
