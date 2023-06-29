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


from sqlalchemy import exc
from sqlalchemy.orm import Session
#load dataframe into postgresql
def database_connection(PW="testing123", HOST='localhost', PORT='5432', path='C:/Users/brend/Documents/GitHub/DA-Sports-Scheduling-App/my_dataframe.pkl'):
    
    #read pickle file with pandas
    profile_df = pd.read_pickle(path)
    
    #establishing the connection
    conn = psycopg2.connect(
        database="postgres",
        user='postgres',
        password=PW,
        host=HOST,
        port=PORT
    )
    print("Connection established")
    time.sleep(0)
 
    # create a cursor object to execute SQL commands
    cur = conn.cursor()
    print("cursor created")
    
    #Retrieving single row
    sql = '''SELECT * from Player_Profiles'''

    #Executing the query
    cur.execute(sql)
    #Fetching 1st row from the table
    result = cur.fetchall()
    print(result)


    # close cursor and connection objects
    cur.close()
    print("cursor closed")
    conn.close()
    print("connection closed")
from psycopg2 import OperationalError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

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
            cursor.close()
            conn.close()

def create_table_from_dataframe(df, PW="testing123", HOST='localhost', PORT='5432'):
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
        for column in df.columns:
            if df[column].dtype == 'int64':
                columns.append(f"{column} INTEGER")
            elif df[column].dtype == 'float64':
                columns.append(f"{column} FLOAT")
            elif df[column].dtype == 'object':
                columns.append(f"{column} TEXT")
            # Add more conditions for other data types if needed
        
        # Create the table with auto-generated column names
        cursor.execute(f"CREATE TABLE your_table_name ({', '.join(columns)})")
        print("Table created successfully!")
        
        # Insert the data into the table
        for _, row in df.iterrows():
            values = [f"'{value}'" if isinstance(value, str) else str(value) for value in row.values]
            cursor.execute(f"INSERT INTO your_table_name VALUES ({', '.join(values)})")
        
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while creating PostgreSQL table: {error}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def main():
    # database_connection()
    # scrape_player_id(PLAYER_MAPPINGS_APIKEY)
    # id_keys_list = parse_id_keys()
    # player_profiles_dict = build_player_profiles_dict(id_keys_list)
    # player_profiles_dict = flatten_json_iterative_solution(player_profiles_dict)
    # create_data_frame(player_profiles_dict)
    path = 'C:/Users/brend/Documents/GitHub/DA-Sports-Scheduling-App/my_dataframe.pkl'
    profile_df = pd.read_pickle(path)
    profile_json = profile_df.to_json
    profile_df.info(verbose=False, memory_usage='deep')
    print(profile_df)
    

if __name__ == "__main__":
    main()