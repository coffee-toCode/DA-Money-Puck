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

def build_player_profiles_dict(id_keys_list):

    for key in id_keys_list:
        try:
            with open(f"C:/Users/brend/Documents/GitHub/DA-Sports-Scheduling-App/json_files/response{key}.json") as infile:
                """
                1. call json file data
                """
                json_data = json.load(infile)
                # flatten that data
                
                json.normalize
                # coaless into a list of dictionaries(https://sparkbyexamples.com/pandas/pandas-convert-list-of-dictionaries-to-dataframe/?expand_article=1)
                player_profiles_dict[key] = json_data
        except FileNotFoundError:
            # print(f"File response{key}.json not found")
            pass
        except json.JSONDecodeError:
            # print(f"Invalid JSON data in file response{key}.json")
            pass
    return player_profiles_dict


def create_data_frame(player_profiles_dict):
    df = pd.DataFrame(data = player_profiles_dict, index=[0])

    # write dataframe to a pickle file
    df.to_pickle('my_dataframe.pkl')


def main():
    # scrape_player_id(PLAYER_MAPPINGS_APIKEY)
    id_keys_list = parse_id_keys()
    player_profiles_dict = build_player_profiles_dict(id_keys_list)
    # player_profiles_dict = flatten_json_iterative_solution(player_profiles_dict)
    
    lst = []
    for i in range(5):
        try:
            with open(f"C:/Users/brend/Documents/GitHub/DA-Sports-Scheduling-App/json_files/response{id_keys_list[i]}.json") as infile:
                json_data = json.load(infile)
                lst.append(json_data)
        except FileNotFoundError:
            # print(f"File response{key}.json not found")
            pass
        except json.JSONDecodeError:
            # print(f"Invalid JSON data in file response{key}.json")
            pass
            
    df = pd.json_normalize(lst)

    print(df)
    print(df.shape)
    print(df.info())    
    # create_data_frame(player_profiles_dict)
    # database_connection()

if __name__ == "__main__":
    main()