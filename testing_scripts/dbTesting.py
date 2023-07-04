import os
import http.client
import json
import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from itertools import chain, starmap
from psycopg2 import OperationalError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


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


from sqlalchemy import exc
from sqlalchemy.orm import Session
#load dataframe into postgresql

def parse_id_keys(raw_player_id_data='testing_scripts/sample_player_mappings(ID).json'):
    
    # Load JSON data from file using json.load()
    with open(raw_player_id_data) as f:
        external_id_data = json.load(f)

    # Extract the external_id values from the mappings list
    id_keys_list = [mapping['external_id'] for mapping in external_id_data['mappings']]

    # Return the list of external_id values
    return id_keys_list

def build_player_profiles_df(id_keys_list):
    player_profiles_list = []
    for key in id_keys_list:
        try:
            with open(f"C:/Users/brend/Documents/GitHub/DA-Sports-Scheduling-App/json_files/response{key}.json") as infile:
                json_data = json.load(infile)
                player_profiles_list.append(json_data)
        except FileNotFoundError:
            # print(f"File response{key}.json not found")
            pass
        except json.JSONDecodeError:
            # print(f"Invalid JSON data in file response{key}.json")
            pass

    player_profiles_df = pd.json_normalize(player_profiles_list)

    # write dataframe to a pickle file
    player_profiles_df.to_pickle('player_profiles_df.pkl')

    return player_profiles_df

def create_database(PW="testing123", HOST='localhost', PORT='5432'):
    conn = None
    try:
        conn = psycopg2.connect(
            database="postgres",
            user='postgres',
            password=PW,
            host=HOST,
            port=PORT
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE Player_Profiles")
        print("Database created successfully!")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while creating PostgreSQL database: {error}")
    finally:
        if conn:
            conn.close()
import numpy as np
import psycopg2
import pandas as pd

def create_table_from_dataframe(player_profiles_df, PW="testing123", HOST='localhost', PORT='5432'):
    conn = None
    try:
        conn = psycopg2.connect(
            database="postgres",
            user='postgres',
            password=PW,
            host=HOST,
            port=PORT
        )
        cursor = conn.cursor()

        # Get column names and types from DataFrame
        columns = []
        for column in player_profiles_df.columns:
            if player_profiles_df[column].dtype == 'int64':
                columns.append(f"{column.replace('.', '_')} INTEGER")
            elif np.issubdtype(player_profiles_df[column].dtype, np.floating):
                columns.append(f"{column.replace('.', '_')} FLOAT")
            elif player_profiles_df[column].dtype == 'object':
                columns.append(f"{column.replace('.', '_')} TEXT")
            # Add more conditions for other data types if needed

        # Create the table with auto-generated column names
        cursor.execute(f"CREATE TABLE raw_profiles ({', '.join(columns)})")
        print("Table created successfully!")

        # Insert the data into the table
        for _, row in player_profiles_df.iterrows():
            values = []
            for value in row.values:
                if isinstance(value, np.ndarray):
                    if len(value) > 1:
                        # Convert the array to a string representation using np.array2string
                        values.append(np.array2string(value))
                    else:
                        values.append(str(value[0])) # Use the first element of the array
                elif isinstance(value, str):
                    values.append(f"'{value}'")
                elif pd.isnull(value):
                    values.append('NULL')
                else:
                    values.append(str(value))
            cursor.execute(f"INSERT INTO raw_profiles VALUES ({', '.join(values)})")

        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while creating PostgreSQL table: {error}")
    finally:
        if conn:
            cursor.close()
            conn.close()


def main():
    id_keys_list = parse_id_keys()
    player_profiles_df = build_player_profiles_df(id_keys_list)
    # create_database()
    create_table_from_dataframe(player_profiles_df)

if __name__ == "__main__":
    main()