from datetime import date
from requests.exceptions import HTTPError
import json
import os
import requests
from hdfs import InsecureClient # library docs https://hdfscli.readthedocs.io/en/latest/index.html

config = {
    "url": "https://robot-dreams-de-api.herokuapp.com",
    "endpoint": "/out_of_stock",
    "username": "rd_dreams",
    "password": "djT6LasE",
    "directory": "./data/hw_l7_from_api/",
    "hdfs_directory": "/bronze/hw_l7_from_api"
}

def get_data(process_date=None):
    token = get_jwt_token(
                url=f'{config.get("url")}/auth',
                username=config.get('username'),
                password=config.get('password')
    )

    if not process_date:
        process_date = str(date.today())
    try:
        url = config.get('url') + config.get('endpoint')
        headers = {'Content-Type': 'application/json', 'Authorization': 'JWT ' + token}
        data = json.dumps({"date": process_date})
        response = requests.get(url, headers=headers, data=data, timeout=10)
        response.raise_for_status()
        local_folder_path = os.path.join(config['directory'], process_date)
        os.makedirs(local_folder_path, exist_ok=True)
        with open(os.path.join(config['directory'], process_date, process_date + '_data.json'), 'w') as json_file:
            data = response.json()
            json.dump(data, json_file)
        print(f'OK for process_date: {process_date}')
        client = InsecureClient(f'http://127.0.0.1:50070/', user='user')
        client.makedirs(config['hdfs_directory'])
        client.upload(config['hdfs_directory'], local_folder_path, overwrite=True)
        print('Successfully copied to HDFS - ', config['hdfs_directory'])
    except Exception as ex:
        print(f'Print _ Error for process_date: {process_date} {response.json()}- Details: {ex}')
        raise ConnectionError(f'Error for process_date: {process_date} {response.json()}- Details: {ex}')
        

def get_jwt_token(url: str, username: str, password: str) -> str:
    try:
        headers = {'Content-Type': 'application/json'}
        data = json.dumps({"username": username, "password": password})
        r = requests.post(url, headers=headers, data=data)
        r.raise_for_status()
        return r.json()['access_token']
    except Exception as ex:
        raise TypeError(ex)


if __name__ == '__main__':
    date_list = ['2021-06-24', '2021-06-19', '2021-06-20', '2021-06-21', '2022-01-23']
    get_data('2021-06-24')
    #for dt in date_list:
    #    get_data(process_date=dt)