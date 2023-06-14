"""
This script is used to scrape the player profiles'. 

This script is developed from the sportradar API doucmentation and prints data in JSON format. XML is also available via the sportradar API.

Before we access the API for the player profiles' we need to parse the player IDs which were previously retrieved from a different sportradar API. 
We actually need the "external_ID" which is the API's way of identifying the individual players. We will then call each id code and store the 
respone json file for later use.
"""

import json

# Open the file for reading
with open("C:/Users/brend/Documents/GitHub/DA-Sports-Scheduling-App/Data_Gathering/sample_player_mappings(ID).json", "r") as f:
    # Read each line in the file
    for line in f:
        # Load the JSON data from the line
        external_id_data = json.loads(line.strip())
        # Remove the 'id' key from the mappings dictionary
        for mapping in external_id_data['mappings']:
            del mapping['id']
        # Print the updated JSON data to the console
        print(json.dumps(external_id_data))







"""
The next step is required due to the limitaitons of the sportradar API in that we are limited to 1 call/second and 1000 calls/month. 
We need to cycle through the external ids and insert them into the below URL in order to pull each player's profile.
There is no way to pull all of the player's profiles at once. 
"""

import http.client

conn = http.client.HTTPSConnection("api.sportradar.us")

conn.request("GET", "http://api.sportradar.us/nhl/trial/v7/en/players/433de553-0f24-11e2-8525-18a905767e44/profile.json?api_key=zxrh73wswmh4zqbckym97pwr")

res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))

