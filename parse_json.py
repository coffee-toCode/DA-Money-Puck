import json

with open(r'C:\Users\brend\Documents\GitHub\DA-Sports-Scheduling-App\response1f0c067f-55e0-4055-9e51-c682e6f75591.json', 'r') as player_profile:
    player_data = json.load(player_profile)


    # extract the relevant information
    id_value = player_data['id']
    full_name = player_data['full_name']
    first_name = player_data['first_name']
    last_name = player_data['last_name']
    year = player_data['seasons'][0]['year']
    type_value = player_data['seasons'][0]['type']




    parsed_total_stats = player_data['seasons'][0]['teams'][0]['statistics']['total']
    total_goals = parsed_total_stats['goals']
    total_assists = parsed_total_stats['assists']
    total_shots = parsed_total_stats['shots']
    print(f'Goals: {total_goals}, Assists: {total_assists}, Shots: {total_shots}')

# close the file after reading
player_profile.close()