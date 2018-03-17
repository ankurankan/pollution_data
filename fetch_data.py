import time
from ast import literal_eval

import requests
import pandas as pd
from tqdm import tqdm

from config import API_KEY, LIMIT, FILENAME

def fetch_data():
    while True:
        r = requests.get('https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69?format=json&api-key=' + API_KEY + '&limit=' + str(LIMIT))
        data_dict = literal_eval(r.text)
        print("Fetched data for time: ", str(data_dict['records'][0]['last_update']))

        df_pollution = pd.DataFrame(data_dict['records'])
        with open(FILENAME, 'a') as f:
            df_pollution.to_csv(f, header=False)

        for i in tqdm(range(60)):
            time.sleep(60)

def initialize_headers(headers):
    df = pd.DataFrame(columns=headers)
    df.to_csv(FILENAME, columns=headers)

if __name__ == '__main__':
    headers = ['city', 'country', 'id', 'last_update', 'pollutant_avg', 'pollutant_id', 
               'pollutant_max', 'pollutant_min', 'pollutant_unit', 'state', 'station']
    _ = initialize_headers(headers)
    fetch_data()
