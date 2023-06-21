import json

with open(r'C:\Users\brend\Documents\GitHub\DA-Sports-Scheduling-App\json_files\response1f0c067f-55e0-4055-9e51-c682e6f75591.json', 'r') as player_profile:
    player_data = json.load(player_profile)

    # # extract the relevant information
    # id_value = player_data['id']
    # full_name = player_data['full_name']
    # first_name = player_data['first_name']
    # last_name = player_data['last_name']
    # year = player_data['seasons'][0]['year']
    # type_value = player_data['seasons'][0]['type']

    parsed_total_stats = player_data['seasons'][0]['teams'][0]['statistics']['total']
    total_goals = parsed_total_stats['goals']
    total_assists = parsed_total_stats['assists']
    total_shots = parsed_total_stats['shots']
    print(f'Goals: {total_goals}, Assists: {total_assists}, Shots: {total_shots}')

# close the file after reading
player_profile.close()




"""
Genie

To create a PostgreSQL table programmatically from a JSON file, you can use the following steps:

Read the contents of the JSON file using your preferred programming language's JSON library.
Extract the columns for your table from the JSON object and store them in an array or list.
Construct a SQL CREATE TABLE statement dynamically using the extracted column names and their data types.
Execute the SQL statement against your database using the appropriate database driver for your programming language.
Here is an example implementation using Python and the psycopg2 library:


This code assumes that each object in your JSON file has the same structure with the same keys. You may need to modify the code to handle different JSON schemas.

# """
# import json
# import psycopg2

# # Read in JSON file
# with open('path/to/file.json', 'r') as f:
#     json_data = json.load(f)

# # Extract columns and their types
# columns = []
# for key in json_data[0].keys():
#     columns.append((key, 'TEXT'))

# # Construct CREATE TABLE statement
# table_name = 'my_table'
# create_table_statement = f'CREATE TABLE {table_name} ('
# for i, (col_name, data_type) in enumerate(columns):
#     create_table_statement += f'{col_name} {data_type}'
#     if i != len(columns) - 1:
#         create_table_statement += ', '
# create_table_statement += ');'

# # Connect to database and execute SQL statement
# conn = psycopg2.connect(user="username", password="password", host="localhost", port="5432", database="database_name")
# cur = conn.cursor()
# cur.execute(create_table_statement)
# conn.commit()
# cur.close()
# conn.close()
