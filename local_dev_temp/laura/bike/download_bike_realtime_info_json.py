
# import pandas as pd
import json
import requests

url = "https://tcgbusfs.blob.core.windows.net/dotapp/youbike/v2/youbike_immediate.json"  # noqa
Response = requests.get(url)

dictjson = Response.json()

with open('bike_realtime_info2.json', 'w') as file:
    json.dump(dictjson, file, ensure_ascii=False)
