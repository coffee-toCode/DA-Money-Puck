import pandas as pd
import json

# Load the JSON data from a file in the same directory
with open('samplePP.json', 'r') as f:
    data = json.load(f)

# Use pandas.json_normalize() to create a Pandas DataFrame
df = pd.json_normalize(data)

# Display the resulting DataFrame
print(df.head(10))

# Write the DataFrame to a CSV file
df.to_csv('testoutput.csv', index=False)