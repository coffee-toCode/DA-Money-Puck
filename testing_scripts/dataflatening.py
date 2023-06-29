import os
import json
import pandas as pd
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


"""
--------Step 2--------: 
Parse the raw_player_id_data file to extract the needed ID keys. Before we access the API for the player profiles' we need to parse the 
player IDs which were previously retrieved from a different sportradar API. We actually need the "external_ID" which is the API's way of 
identifying the individual players. We will then call each id code and store the respone json file for later use.
"""


def parse_id_keys(raw_player_id_data='testing_scripts/sample_player_mappings(ID).json'):
    
    # Load JSON data from file using json.load()
    with open(raw_player_id_data) as f:
        external_id_data = json.load(f)
        # print(f)

    # Extract the external_id values from the mappings list
    id_keys_list = [mapping['external_id'] for mapping in external_id_data['mappings']]

    # Return the list of external_id values
    return id_keys_list


"""
--------Step 3(alternate)--------:
This step is optional and requires having made the API calls and saved the players profiles' to json files. 
The goal here is to build a player profile dictionary from the saved player profiles. 
"""
def build_player_profiles_dict(id_keys_list):
        
    for key in id_keys_list:
        try:
            with open(f"C:/Users/brend/Documents/GitHub/DA-Sports-Scheduling-App/json_files/response{key}.json") as infile:
                json_data = json.load(infile)
                player_profiles_dict[key] = json_data
        except FileNotFoundError:
            # print(f"File response{key}.json not found")
            pass
        except json.JSONDecodeError:
            # print(f"Invalid JSON data in file response{key}.json")
            pass
    return player_profiles_dict


"""
--------Step 4--------:
This step creates a dataframe and then saves it into a pkl file.  
"""

def flatten_json_iterative_solution(player_profiles_dict):
    """Flatten a nested json file"""

    def unpack(parent_key, parent_value):
        """Unpack one level of nesting in json file"""
        # Unpack one level only!!!
        
        if isinstance(parent_value, dict):
            for key, value in parent_value.items():
                temp1 = parent_key + '_' + key
                yield temp1, value
        elif isinstance(parent_value, list):
            i = 0 
            for value in parent_value:
                temp2 = parent_key + '_'+str(i) 
                i += 1
                yield temp2, value
        else:
            yield parent_key, parent_value    

            
    # Keep iterating until the termination condition is satisfied
    while True:
        # Keep unpacking the json file until all values are atomic elements (not player_profiles_dict or list)
        player_profiles_dict = dict(chain.from_iterable(starmap(unpack, player_profiles_dict.items())))
        # Terminate condition: not any value in the json file is player_profiles_dict or list
        if not any(isinstance(value, dict) for value in player_profiles_dict.values()) and \
           not any(isinstance(value, list) for value in player_profiles_dict.values()):
            break
    return player_profiles_dict



def main():
    # scrape_player_id(PLAYER_MAPPINGS_APIKEY)
    id_keys_list = parse_id_keys()
    player_profiles_dict = build_player_profiles_dict(id_keys_list)
    player_profiles_dict = flatten_json_iterative_solution(player_profiles_dict)
    print(player_profiles_dict)
    # create_data_frame(player_profiles_dict)
    # database_connection(PW="testing123", HOST='localhost', PORT='5432', profile_df=pd.read_pickle('my_dataframe.pkl'))

if __name__ == "__main__":
    main()