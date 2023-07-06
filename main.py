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


# --------Step 1--------: scrape the SportRadar API for the player ID keys.
def scrape_player_id(PLAYER_MAPPINGS_APIKEY) -> str:
    """
    Scrapes player ID keys for use in the 'pull_player_profile' script.
    Returns data in JSON format.
    """
    global raw_player_id_data

    conn = http.client.HTTPSConnection("api.sportradar.us")
    conn.request("GET", "http://api.sportradar.us/icehockey/trial/v2/en/players/mappings.json?api_key=" + PLAYER_MAPPINGS_APIKEY)
    res = conn.getresponse()
    raw_player_id_data = res.read()

    return raw_player_id_data.decode("utf-8")


"""
--------Step 2--------: 
Parse the raw_player_id_data file to extract the needed ID keys. Before we access the API for the player profiles' we need to parse the 
player IDs which were previously retrieved from a different sportradar API. We actually need the "external_ID" which is the API's way of 
identifying the individual players. We will then call each id code and store the respone json file for later use.
"""
# def parse_id_keys(raw_player_id_data):
    
#     # Parse the JSON data from the file using json.load()
#     external_id_data = json.loads(raw_player_id_data)
    
#     # Iterate over the mappings list and extract the external_id values
#     for mapping in external_id_data['mappings']:
#         id_keys_list.append(mapping['external_id'])

#     # Return the list of external_id values
#     return id_keys_list


def parse_id_keys(raw_player_id_data='testing_scripts/sample_player_mappings(ID).json'):
    
    # Load JSON data from file using json.load()
    with open(raw_player_id_data) as f:
        external_id_data = json.load(f)

    # Extract the external_id values from the mappings list
    id_keys_list = [mapping['external_id'] for mapping in external_id_data['mappings']]

    # Return the list of external_id values
    return id_keys_list



"""
--------Step 3(main)--------:
The next step is required due to the limitaitons of the sportradar API in that we are limited to 1 call/second and 1000 calls/month. 
Therefore in order to only make a single set of API calls we will run this function and save the raw, returned data as json files for later use and practice.
We need to cycle through the external ids and insert them into the below URL in order to pull each player's profile.
There is no way to pull all of the player's profiles from the API with a single call. 
"""
def api_scrape_player_profiles(id_keys_list, PLAYER_PROFILE_APIKEY):
    
    base_url_start = "http://api.sportradar.us/nhl/trial/v7/en/players/"
    base_url_end = "/profile.json?api_key=" + PLAYER_PROFILE_APIKEY

    conn = http.client.HTTPSConnection("api.sportradar.us")


    for key in id_keys_list:
        url = base_url_start + str(key) + base_url_end
        conn.request("GET", url)
        response = conn.getresponse()
        data = response.read().decode("utf-8")
        try:
            json_data = json.loads(data)
            with open(f"response{key}.json", "w") as outfile:
                json.dump(json_data, outfile)
            print(json_data)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON data: {e}")
        conn.close()
        time.sleep(1.1) # Wait for 1.1 seconds before making the next call


"""
--------Step 3(alternate)--------:
This step is optional and requires having made the API calls and saved the players profiles' to json files. 
The goal here is to build a player profile dictionary from the saved player profiles. 
"""
def build_player_profiles_lst(id_keys_list, path):

    Profiles_lst = []
    for key in id_keys_list:
        try:
            with open(f"{path}/json_files/response{key}.json") as infile:
                json_data = json.load(infile)
                Profiles_lst.append(json_data)
        except FileNotFoundError:
            # print(f"File response{key}.json not found")
            pass
        except json.JSONDecodeError:
            # print(f"Invalid JSON data in file response{key}.json")
            pass
            
    df = pd.json_normalize(Profiles_lst)



"""
--------Step 4--------:
This step creates a dataframe and then saves it into a pkl file.  
"""

# def flatten_json_iterative_solution(dictionary):
#     """Flatten any dictionary of dictionaries nested json file"""

#     def unpack(parent_key, parent_value):
#         """Unpack one level of nesting in json file"""
#         # Unpack one level only!!!
        
#         if isinstance(parent_value, dict):
#             for key, value in parent_value.items():
#                 temp1 = parent_key + '_' + key
#                 yield temp1, value
#         elif isinstance(parent_value, list):
#             i = 0 
#             for value in parent_value:
#                 temp2 = parent_key + '_'+str(i) 
#                 i += 1
#                 yield temp2, value
#         else:
#             yield parent_key, parent_value    

            
#     # Keep iterating until the termination condition is satisfied
#     while True:
#         # Keep unpacking the json file until all values are atomic elements (not player_profiles_dict or list)
#         dictionary = dict(chain.from_iterable(starmap(unpack, dictionary.items())))
#         # Terminate condition: not any value in the json file is player_profiles_dict or list
#         if not any(isinstance(value, dict) for value in dictionary.values()) and \
#            not any(isinstance(value, list) for value in dictionary.values()):
#            break

#     return dictionary

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


from sqlalchemy import exc
from sqlalchemy.orm import Session
#load dataframe into postgresql
def database_connection(PW="testing123", HOST='localhost', PORT='5432', df_path = os.path.join(path, "/my_dataframe.pkl")):
    
    #read pickle file with pandas
    profile_df = pd.read_pickle(df_path)
    
    #establishing the connection
    conn = psycopg2.connect(
        database="postgres",
        user='postgres',
        password=PW,
        host=HOST,
        port=PORT
    )
    
    # create SQLAlchemy engine to simplify writing of dataframe to postgres
    engine = create_engine('postgresql+psycopg2://postgres:'+PW+'@'+HOST+':'+PORT+'/postgres')

    # create Session object bound to this engine
    session = Session(engine)
    
    print("Connection established")
    time.sleep(1)

    # load dataframe into database - replace <table_name> with your desired table name
    
    
    # for value in profile_df:
    #     print(profile_df[value])
    try:
    # Attempt to commit the changes to the database
        profile_df[1:50].to_sql('Player_Profiles', engine, if_exists='replace')
        session.commit()
    except exc.IntegrityError:
        # Handle the unique constraint violation error here if it occurs
        pass
        print("Error: Unique Constraint Violation")

    # close the session and connection
    session.close()
    engine.dispose()
    #Closing the connection
    conn.close()


def main():
    # scrape_player_id(PLAYER_MAPPINGS_APIKEY)
    id_keys_list = parse_id_keys()
    player_profiles_df = build_player_profiles_df(id_keys_list)
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