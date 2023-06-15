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
        # print(json.dumps(external_id_data))
        print("external ID success")

count = 0

for item in external_id_data["mappings"]:
    if str(item["external_id"]):
        count+= 1

print(count)
