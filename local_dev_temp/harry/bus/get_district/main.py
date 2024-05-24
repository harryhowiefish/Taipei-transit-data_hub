from collections import Counter
import functions_framework
import json
import requests
from dotenv import load_dotenv
import os
load_dotenv()

API_KEY = os.environ['google_map_api_key']


def extract_data(data):
    locations = data['results']
    area_level_2_options = []
    area_level_3_options = []
    if not locations:
        return None, None
    for loc in locations:
        components = loc['address_components']
        for component in components:
            if 'administrative_area_level_2' in component.get('types', ''):
                area_level_2_options.append(component['long_name'])
            elif 'administrative_area_level_3' in component.get('types', ''):
                area_level_3_options.append(component['long_name'])
    return Counter(area_level_2_options).most_common(n=1)[0][0], Counter(area_level_3_options).most_common(n=1)[0][0]


@functions_framework.http
def get_district(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    # request_args = request.args

    lat = request_json['lat']
    lng = request_json['lng']
    reverse_geocoding_url = f'https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lng}&key={API_KEY}&language=zh-TW'  # noqa
    try:
        response = requests.get(reverse_geocoding_url)
        data = response.json()

    except Exception as e:
        return f'Excpetion {e} raised during geocoding API call...'

    level_2, level_3 = extract_data(data)
    result = {'district': level_2, 'county': level_3}
    return json.dumps(result, ensure_ascii=False)
