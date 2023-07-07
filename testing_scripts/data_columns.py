import os
import http.client
import json
import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from itertools import chain, starmap


# Load the environment variables from the .env file
from dotenv import load_dotenv
load_dotenv()

# SportRadar API keys:
PLAYER_MAPPINGS_APIKEY = os.getenv("PLAYER_MAPPINGS_APIKEY")
PLAYER_PROFILE_APIKEY = os.getenv("PLAYER_PROFILE_APIKEY")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")


# --------Global Varriables--------
# Step 1: Create an empty lsit to store the raw player id data called from the sport radar API. 
raw_player_id_data = []
# Step 2: Create an empty list to store the external_id values
id_keys_list = []
# Step 3(alternate): Create an empty dictionary to store the player profiles. 
player_profiles_dict = {}
#
path = "C:/Users/brend/Documents/GitHub/DA-MONEY-PUCK/"



def parse_id_keys(raw_player_id_data='testing_scripts/sample_player_mappings(ID).json'):
    
    # Load JSON data from file using json.load()
    with open(raw_player_id_data) as f:
        external_id_data = json.load(f)

    # Extract the external_id values from the mappings list
    id_keys_list = [mapping['external_id'] for mapping in external_id_data['mappings']]

    # Return the list of external_id values
    return id_keys_list


from flatten_json import flatten
def build_player_profiles_df(id_keys_list, path):
    player_profiles_list = []
    for key in id_keys_list:
        try:
            with open(f"{path}/json_files/response{key}.json") as infile:
                json_data = json.load(infile)
                player_profiles_list.append(json_data)
        except FileNotFoundError:
            # print(f"File response{key}.json not found")
            pass
        except json.JSONDecodeError:
            # print(f"Invalid JSON data in file response{key}.json")
            pass

    df = pd.json_normalize(player_profiles_list)

    dic_flattened = []
    try:
        dic_flattened = [flatten(dict(d)) for d in df.to_dict(orient='records')]
    except Exception as e:
        print(f"An error occurred: {e}")
        pass
    
    df = pd.DataFrame(dic_flattened)

    return df
    # player_profiles_df = pd.json_normalize(flat_list)

    # write dataframe to a pickle file
    # player_profiles_df.to_pickle('player_profiles_df.pkl')

    # return player_profiles_df


def main():
    # scrape_player_id(PLAYER_MAPPINGS_APIKEY)
    id_keys_list = parse_id_keys()
    player_profiles_df = build_player_profiles_df(id_keys_list, path)
    # print(player_profiles_df)
    #read pickle file with pandas
    # profile_df = pd.read_pickle('C:/Users/brend/Documents/GitHub/DA-MONEY-PUCK/player_profiles_df.pkl')
    # print(profile_df)
    #is json normalize capable of exploding nested json. 3-5 levels deep. 
    # print(player_profiles_df.shape)
    # print(player_profiles_df.info())  
    # print(player_profiles_df.head(15))  
    # Define the conditions
    condition1 = 'seasons_8_year'
    condition2 = ''

    # Get the column names satisfying the conditions
    filtered_columns = [col_name for col_name in player_profiles_df.columns if condition1 in col_name and condition2 in col_name]

    # Print the filtered column names
    print(filtered_columns)
    print(player_profiles_df.loc[:, ['full_name', 'seasons_1_year']])

if __name__ == "__main__":
    main()