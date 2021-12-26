from config import Config
from datetime import date
from requests.exceptions import HTTPError
import json
import os
import requests


def get_data(config: dict, token: str, process_date=None):
    if not process_date:
        process_date = str(date.today())
    try:
        url = config.get('url') + config.get('endpoint')
        headers = {'Content-Type': 'application/json', 'Authorization': 'JWT ' + token}
        data = json.dumps({"date": process_date})
        response = requests.get(url, headers=headers, data=data)
        response.raise_for_status()
        os.makedirs(os.path.join(config['directory'], process_date), exist_ok=True)
        with open(os.path.join(config['directory'], process_date, process_date + '_data.json'), 'w') as json_file:
            data = response.json()
            json.dump(data, json_file)
        print(f'OK for process_date: {process_date}')
    except Exception as ex:
        print(f'Error for process_date: {process_date} {response.json()}- Details{ex}')

def get_jwt_token(url: str, username: str, password: str) -> str:
    try:
        headers = {'Content-Type': 'application/json'}
        data = json.dumps({"username": username, "password": password})
        r = requests.post(url, headers=headers, data=data)
        r.raise_for_status()
        return r.json()['access_token']
    except Exception as ex:
        raise TypeError(ex)

def main():
    config = Config(os.path.join('.', 'config.yaml')).get_config('rd_api')

    token = get_jwt_token(
                url=f'{config.get("url")}/auth',
                username=config.get('username'),
                password=config.get('password'),
            )
    #print(token)
    date_list = ['2021-06-24', '2021-06-19', '2021-06-20', '2021-06-21', '2021-12-18']

    for dt in date_list:
        get_data(config=config, token=token, process_date=dt)


if __name__ == '__main__':
    main()