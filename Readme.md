## Intro

Unintentionally inspired by Money Ball. No this project does not star Jona Hill or Brad Pitt. 

All API's referenced in Git history have been deactivated. 

## Local setup

Clone the repository to your favorite IDE, and in the repo's root directory, run

```
python -m venv myenv

(mac,linux)
   source .venv/bin/activate

(windows)
   .\myenv\Scripts\activate


pip install -r requirements.txt
```

Then you should be good to start


<!-- ## run the app

(windows)
   python .\main.py -->

## updating requirements file: 
   pip freeze > requirements.txt


## Goals
   1. pull data from API
   2. store and clean data
   3. diplay data in varrious dashboards

## temp notes
To retrieve the XML Schema Definition (.XSD) for the Player Profile use the following URL.
https://feed.elasticstats.com/schema/hockey/profile-v7.0.xsd


sample XML data and structure:
https://developer.sportradar.com/files/nhl_v7_player_profile_example.xml



## To-do
[x] Refactor the json files into a pandas dataframe (could also try using polars)
[ ] Save dataframe into the postgresDB
[x] Modularize code into separate functions or classes
