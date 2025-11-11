import requests
import urllib
import json
          
dataset_id = "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"
column_filters = ""
row_filters = {}
offset = 60
sort = None
filters = {}
for key, value in row_filters.items():
    filters[key] = {
        "type": "ILIKE",
        "value": str(value) if value is not None else ""
    }

row_filters_encoded = urllib.parse.quote(json.dumps(filters))

url = "https://data.gov.sg/api/action/datastore_search?resource_id=" + dataset_id
if column_filters:
  url = url + "&fields=" + column_filters
if filters:
  url = url + "&filters=" + row_filters_encoded
if offset:
  url = url + "&offset=" + str(offset)
if sort:
  url = url + "&sort=" + sort
  
url += "&limit=10"
        
response = requests.get(url)
print(response.json())
      