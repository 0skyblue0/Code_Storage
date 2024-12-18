import requests
import pandas as pd
import json

with open('API_Vworld.key', 'r',encoding='utf8') as f:
    key = f.read()

def vworld_request(url:str,params:dict):
    'PNU 기반'
    main_dataframe = pd.DataFrame()
    params.update({'key' : key, 'numOfRows': 1000, 'format':'json'})
    pagenum = 0
    data_counter = 0
    while True:
        pagenum += 1
        params.update({'pageNo': pagenum})
        response = requests.get(url, params=params)
        res_data = json.loads(response.content.decode())
        try:
            if 'response' in res_data.keys():
                return 'pnu is not valid'
            else:
                res_data = res_data[list(res_data.keys())[0]]
                data = res_data['field']
        except Exception as E:
            raise E
        max_counter = int(res_data['totalCount'])
        data_counter += len(data)

        temp_dataframe = pd.DataFrame(data)
        main_dataframe = pd.concat([main_dataframe, temp_dataframe])

        if data_counter >= max_counter:
            break
    return main_dataframe