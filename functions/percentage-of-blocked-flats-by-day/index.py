import requests
import time
import json
import os
import datetime
import boto3
import pytz
from dateutil.relativedelta import relativedelta

# Обязательная инициирующую функция, в которой подгружается токен и которая используется для вызова других функций
def handler(event, context):
    headers={'Authorization':context.token['access_token'],'Accept':'application/json'}

    #'b1gb310irjlk6b99e14g' - аналитика
    #'b1gc7vi2ckqausoc5dr7' - спутник

    FOLDER_ID = os.getenv("FOLDER_ID") # id каталога из которого береться запрос
    FOLDER = os.getenv("FOLDER") #имя папки в которую будет выкладываться
    ACCESS_KEY = os.getenv("ACCESS_KEY") #aws_access_key_id для S3
    SECRET_KEY = os.getenv("SECRET_KEY") #aws_secret_access_key в s3
    BUCKET_NAME = os.getenv("BUCKET_NAME") #имя бакета
    TIME_ZONE = os.getenv("TIME_ZONE", "Europe/Moscow") #настройка функции
    TEMP_FILENAME = "/tmp/temp_file"
    URL_ALERT = os.getenv("URL_ALERT") # Ссылка для алерта для отслеживания.
   
    def get_now_datetime_str(): # получаем актуальное время
        time_zone = os.getenv("TIME_ZONE", "Europe/Moscow") # меняем таймзону на московскую
        now = datetime.datetime.now(pytz.timezone(time_zone))    
        yesterday = now - datetime.timedelta(days=1) #нужна вчерашняя дата так как данные за прошлый день
        last_month_data = now - relativedelta(month=1)
        return {'key': yesterday.strftime('year=%Y/month=%m/%d.csv'),
                'key_month': yesterday.strftime('year=%Y/month=%m.csv'),
                'now':now.strftime('%Y-%m-%d %H:%M:%S'),
                'yesterday_data':yesterday.strftime('%Y-%m-%d'),
                'yesterday':yesterday.strftime('%Y-%m-%d %H:%M:%S'), 
                'year':yesterday.strftime('%Y'),
                'month':yesterday.strftime('%m'),
                'day':yesterday.strftime('%d'),
                'last_month_data':last_month_data.strftime('%Y-%m-%d')
                }

    def create_query(): #функция создает новый запрос и возвращает id для запроса результата
        body = {
            "name":query_name, 
            "TYPE":"ANALYTICS", 
            "text":query_text, 
            "description":query_description
        }
        response = requests.post(
            f'https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries?project={FOLDER_ID}',
            headers=headers,
            json=body
        )
        if response.status_code == 200:
            return response.json()["id"]
        return f' Code: {response},  text: {response.text}'

    def get_request(offset): # фунция возвращает ответ запроса. Максимум 1000 строк.
        offset = offset
        get_query_results_url = f'https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/{request_id}/results/0?project={FOLDER_ID}&offset={str(offset)}&limit=1000'
        response = requests.get(
            get_query_results_url,
            headers = headers
        )
        return response

    def if_cell_is_list(cell): # функция участвует в преобразовании данных при создании файла
        if isinstance(cell, list):
            if len(cell) == 0:
                return ''
            else: 
                return cell[0]
        else:
            return cell
    
    def write_temp_file():
        offset = 0
        response = get_request(offset) #запрашиваем данные запроса
        columns = [rows['name'] for rows in response.json()['columns']] #выделяем названия столбцов
        special_str = ""
        for j in columns:
            special_str = f"{special_str}{str(j)},"
        temp_file = open(TEMP_FILENAME, 'w')
        temp_file.write(special_str[:-1]+'\n')

    def write_temp_file():
        offset = 0
        response = get_request(offset) #запрашиваем данные запроса
        columns = [rows['name'] for rows in response.json()['columns']] #выделяем названия столбцов
        special_str = ""
        for j in columns:
            special_str = f"{special_str}{str(j)},"
        temp_file = open(TEMP_FILENAME, 'w', encoding='utf-8')
        temp_file.write(special_str[:-1]+'\n')
        while response.status_code == 200 and len(response.json()['rows']) != 0:  #Цикл делает запросы по 10000, пока не кончатся данные
            response = get_request(offset)
            response_rows = response.json()['rows']
            rows = [[if_cell_is_list(cell) for cell in row] for row in response_rows]  #Преобразуются строки
            # Открывает созданный файл и добавляет в него строки
            for row in rows:
                special_str = ''
                for word in row:
                    if isinstance(word, str):
                        special_str += "'{0}',".format(word.replace("'", ""))
                    else:
                        special_str += "{0},".format(word)
                temp_file.write(special_str[:-1]+'\n') 
            offset +=1000 # увеличивает смещение

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
            Key=key
        )

    def remove_temp_files(): #функция удаляет временный файл
        os.remove(TEMP_FILENAME)


    key = f"{FOLDER}/{get_now_datetime_str()['key']}"
    yesterday_data = get_now_datetime_str()['yesterday_data']
    now = get_now_datetime_str()['now']

    query_text = open('query.txt','r').read().format(yesterday_data)
    query_name = f'{FOLDER} {now}' #имя, которое появляется в запросах
    query_description = f'Ежеднеаный запрос - {yesterday_data}'#описание, которое появляется в запросах

    request_id = create_query()
    while str(get_request(0)) == '<Response [400]>':
        time.sleep(10)  # пауза для создания запроса

    write_temp_file()
    get_s3_instance()
    upload_dump_to_s3()
    remove_temp_files()
    
    requests.get(URL_ALERT)

    return {
        'statusCode': 200,
        'body': 'файл выгружен',
        
    }
