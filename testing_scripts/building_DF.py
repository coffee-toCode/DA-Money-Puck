import json
import pandas as pd

# --------Global Varriables--------
# Create an empty list to store the external_id values
id_keys_list = []
# Create an empty dictionary to store the player profiles. 
# player_profiles_dict = {}

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


def main():
    # scrape_player_id(PLAYER_MAPPINGS_APIKEY)
    id_keys_list = parse_id_keys()
    player_profiles_df = build_player_profiles_df(id_keys_list)
    
    print(player_profiles_df)
    print(player_profiles_df.shape)
    print(player_profiles_df.info())  
    print(player_profiles_df.head(50))   
    # create_data_frame(player_profiles_dict)
    # database_connection()

if __name__ == "__main__":
    main()