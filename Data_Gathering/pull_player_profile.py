#This script is used to scrape the player profiles' for use in the '' script. 

#This script is developed from the sportradar API doucmentation and prints data in xml format. JSON is also available via the sportradar API.






import http.client

conn = http.client.HTTPSConnection("api.sportradar.us")

conn.request("GET", "http://api.sportradar.us/nhl/trial/v7/en/players/433de553-0f24-11e2-8525-18a905767e44/profile.xml?api_key=zxrh73wswmh4zqbckym97pwr")

res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))