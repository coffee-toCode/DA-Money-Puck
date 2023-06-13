#This script is used to scrape the playerIDs' for use in the 'pull_player_profile' script. 

#This script is developed from the sportradar API doucmentation and prints data in xml format. JSON is also available via the sportradar API.






import http.client

conn = http.client.HTTPSConnection("api.sportradar.us")

conn.request("GET", "http://api.sportradar.us/icehockey/trial/v2/en/players/mappings.xml?api_key=a8jvysc5kas2e2y86rwdscr9")

res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))