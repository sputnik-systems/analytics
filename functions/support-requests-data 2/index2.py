import requests
import time
import json
import os
import datetime
import boto3
import pytz


FOLDER_ID = os.getenv("FOLDER_ID") # id каталога из которого береться запрос
FOLDER = os.getenv("FOLDER") #имя папки в которую будет выкладываться
ACCESS_KEY = os.getenv("ACCESS_KEY") #aws_access_key_id для S3
SECRET_KEY = os.getenv("SECRET_KEY") #aws_secret_access_key в s3
BUCKET_NAME = os.getenv("BUCKET_NAME") #имя бакета
TIME_ZONE = os.getenv("TIME_ZONE", "Europe/Moscow") #настройка функции
TEMP_FILENAME = "/tmp/temp_file"
URL_ALERT = os.getenv("URL_ALERT")

url = 'https://docs.google.com/spreadsheets/d/1tuVDy6kU6KeyoGNIpoGRhRuuN5Mc165mijqUA0j83Og/gviz/tq?tqx=out:csv&sheet=form1'

response = requests.get(url)
data = response.content
with open(TEMP_FILENAME, 'wb') as file:
    file.write(data)
    
def get_s3_instance(): # функция создает соединение
    session = boto3.session.Session()
    return session.client(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )

def upload_dump_to_s3(): # функция выгружает данные в s3
    get_s3_instance().upload_file(
        Filename=TEMP_FILENAME,
        Bucket=BUCKET_NAME,
        Key=f"{FOLDER}/support_requests_data_2.csv"
    )

def remove_temp_files(): #функция удаляет временный файл
    os.remove(TEMP_FILENAME)

get_s3_instance()
upload_dump_to_s3()
remove_temp_files()

