import os
import http.client
import json
import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


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
def parse_id_keys(raw_player_id_data):
    
    # Parse the JSON data from the file using json.load()
    external_id_data = json.loads(raw_player_id_data)
    
    # Iterate over the mappings list and extract the external_id values
    for mapping in external_id_data['mappings']:
        id_keys_list.append(mapping['external_id'])

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
def create_data_frame(player_profiles_dict):
    df = pd.DataFrame(player_profiles_dict)

    # write dataframe to a pickle file
    df.to_pickle('my_dataframe.pkl')
    
    # read dataframe from the same pickle file
    profile_df = pd.read_pickle('my_dataframe.pkl')
    return profile_df

def creare_player_profile_json(player_profiles_dict):
    
    with open("player_profiles.json", "w") as outfile:
        json.dump(player_profiles_dict, outfile)
    
    return player_profiles_dict


#load dataframe into postgresql
def database_connection(PW="testing123", HOST='localhost', PORT='5432', profile_df=pd.read_pickle('my_dataframe.pkl')):
    
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
   
    # load dataframe into database - replace <table_name> with your desired table name
    profile_df.to_sql('Player Profiles', engine, if_exists='replace', index=False)

    print("Connection established")

    #Closing the connection
    conn.close()

database_connection(PW="testing123", HOST='localhost', PORT='5432', profile_df=pd.read_pickle('my_dataframe.pkl'))


# def main():
#     scrape_player_id(PLAYER_MAPPINGS_APIKEY)
#     parse_id_keys(raw_player_id_data)
#     build_player_profiles_dict(id_keys_list)
#     create_data_frame(player_profiles_dict)
# if __name__ == "__main__":
#     #main()