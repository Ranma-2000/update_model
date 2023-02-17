import datetime
from time import mktime
import pandas as pd
from Google import Create_Service
import json
import sqlite3
from googleapiclient.http import MediaIoBaseDownload
from dateutil.relativedelta import relativedelta
import io
import os

convert2sec = {
    's': 1,
    'm': 60,
    'h': 60*60,
    'd': 24*60*60,
    'w': 7*24*60*60,
}

while True:
    # Config --------------------------------------------------------------------
    config = json.load(open('volume/check_update_config.json'))
    model_update_config = json.load(open('volume/config.json'))

    current_date = datetime.datetime.now()

    update_timestamp = config['task']['update']['update_timestamp']
    update_timestamp = datetime.datetime.strptime(update_timestamp, '%Y-%m-%dT %H:%M:%S.%fZ').timetuple()
    update_timestamp = datetime.datetime.fromtimestamp(mktime(update_timestamp))

    con = sqlite3.connect(f"volume/20221216.db")
    cur = con.cursor()
    cur.execute('SELECT * FROM model_version_control WHERE   id = (SELECT MAX(id)  FROM model_version_control)')
    sqlite_output = cur.fetchone()
    if current_date >= update_timestamp:
        check_update_status = True
    else:
        check_update_status = False
        # print('Not due date.')

    # Google API
    CLIENT_SECRET_FILE = config['parameters']['google_api']['client_secret_file']
    API_NAME = config['parameters']['google_api']['api_name']
    API_VERSION = config['parameters']['google_api']['api_version']
    SCOPES = [config['parameters']['google_api']['scopes']]
    if check_update_status:
        service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

        folder_id = config['parameters']['google_api']['folder_id']['model_version_launch']
        query = f"parents = '{folder_id}'"

        response = service.files().list(q=query).execute()
        files = response.get('files')
        if len(files) > 0:
            pd.set_option('display.max_columns', 100)
            pd.set_option('display.width', 200)
            files = pd.DataFrame(files)
            # print(files)
            # print(model_update_config['parameters']['model_name'])
            # print(files.loc[files['name'] != 'info.json']['name'].values[0])
            if (sqlite_output[3] == 'used') & (model_update_config['parameters']['model']['name'] != files.loc[files['name'] != 'info.json']['name'].values[0]):
                json_file_id = files.loc[files['name'] == 'info.json']['id']
                request = service.files().get_media(fileId=json_file_id.values[0])
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fd=fh, request=request)
                download_status = False

                while not download_status:
                    download_progress, download_status = downloader.next_chunk()
                fh.seek(0)

                cloud_storage_info = json.load(fh)

                version_created_date = cloud_storage_info['created date']
                version_created_date = datetime.datetime.strptime(version_created_date, '%Y-%m-%dT %H:%M:%S.%fZ').timetuple()
                version_created_date = datetime.datetime.fromtimestamp(mktime(version_created_date))
                model_file_id = files.loc[files['name'] != 'info.json']['id']
                model_file_name = files.loc[files['name'] != 'info.json']['name']
                request = service.files().get_media(fileId=model_file_id.values[0])
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fd=fh, request=request)
                download_status = False

                while not download_status:
                    download_progress, download_status = downloader.next_chunk()
                fh.seek(0)
                with open(os.path.join(model_update_config['parameters']['model']['path'], model_file_name.values[0]), 'wb') as f:
                    # print(model_file_name.values[0])
                    f.write(fh.read())
                    f.close()
                    print('Downloaded up-to-date model')

                    with open('check_update_config.json', 'w') as js:
                        config['task']['update']['update_timestamp'] = datetime.datetime.strftime(
                            (current_date + relativedelta(minutes=+2)),
                            '%Y-%m-%dT %H:%M:%S.%fZ')
                        json.dump(
                            config,
                            js,
                            indent=4
                        )
                records = [
                    (
                        datetime.datetime.strftime(current_date, '%Y-%m-%dT %H:%M:%S.%fZ'),
                        model_file_name.values[0],
                        'downloaded'
                    )
                ]
                cur.executemany('INSERT INTO model_version_control (created_timestamp, model_name, status) VALUES(?,?,?);', records)
                con.commit()
