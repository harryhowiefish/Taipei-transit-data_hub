from io import BytesIO
import zipfile
import requests
import pandas as pd


url = "https://tcgbusfs.blob.core.windows.net/dotapp/youbike_second_ticket_opendata/YouBikeHis.csv"
df = pd.read_csv(url)


for index, row in df.iterrows():
    try:
        fileinfo = row['fileinfo']
        fileURL = row['fileURL']
        filename = fileinfo.replace("年", "_").replace(
            "月", "_bike_usage_history.csv")

        response = requests.get(fileURL)
        zip_content = BytesIO(response.content)

        with zipfile.ZipFile(zip_content, 'r') as zip_ref:
            data = zip_ref.namelist()
            for file in data:
                if '.csv' in file:
                    file_in_zip = file
            with zip_ref.open(file_in_zip) as csv_file:
                transform_df = pd.read_csv(
                    csv_file, header=None, encoding_errors='replace')

        first_cell = transform_df.iloc[0, 0]
        is_rent_time = first_cell == 'rent_time'
        is_rent_time
        if is_rent_time:
            transform_df.columns = transform_df.iloc[0]
            transform_df = transform_df.drop(0)

        transform_df.to_csv(filename)

        print(filename, fileURL)

    except Exception as e:
        print(f"An error occurred for row {index}: {
              e}. Skipping to the next row...")
        continue
