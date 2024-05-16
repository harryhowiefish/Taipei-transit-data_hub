import pandas as pd

url = "https://tcgbusfs.blob.core.windows.net/dotapp/youbike/v2/youbike_immediate.json"  # noqa
df = pd.read_json(url)
df.to_csv("bike_usage_realtime.csv")
