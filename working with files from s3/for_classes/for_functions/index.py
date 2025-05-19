import requests
import time
import requests
import time
import os
import datetime
import boto3
import pytz
from dateutil.relativedelta import relativedelta
import pandas as pd

FOLDER_ID = os.getenv("FOLDER_ID") # id каталога из которого береться запрос
FOLDER = os.getenv("FOLDER") #имя папки в которую будет выкладываться
ACCESS_KEY = os.getenv("ACCESS_KEY") #aws_access_key_id для S3
SECRET_KEY = os.getenv("SECRET_KEY") #aws_secret_access_key в s3
BUCKET_NAME = os.getenv("BUCKET_NAME") #имя бакета
TIME_ZONE = os.getenv("TIME_ZONE", "Europe/Moscow") #настройка функции
TEMP_FILENAME = "/tmp/temp_file"
# URL_ALERT = os.getenv("URL_ALERT") # Ссылка для алерта для отслеживания. 
type_of_empty_values = os.getenv("type_of_empty_values")

if type_of_empty_values == 'None':
    type_of_empty_values = None
if type_of_empty_values == '':
    type_of_empty_values = ''

class Dowloading_from_query:
    def __init__ (self):
        self.headers = None
        self.FOLDER_ID = None
        self.query_name = None
        self.query_text = None
        self.TEMP_FILENAME = None
        self.query_description = None
    
    def get_query_parameters(self,parameters):
        self.headers = parameters['headers']
        self.FOLDER_ID = parameters['FOLDER_ID']
        self.query_name = parameters['query_name']
        self.query_text = parameters['query_text']
        self.TEMP_FILENAME = parameters['TEMP_FILENAME']
        self.query_description = parameters['query_description']
        
    def create_query(self): #функция создает новый запрос и возвращает id для запроса результата
        body = {
            "name":self.query_name, 
            "TYPE":"ANALYTICS", 
            "text":self.query_text, 
            "description":self.query_description
        }
        response = requests.post(
            f'https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries?project={self.FOLDER_ID}',
            headers=self.headers,
            json=body
        )
        if response.status_code == 200:
            return response.json()["id"]
        return f' Code: {response},  text: {response.text}'
    
    def get_request(self,offset): # фунция возвращает ответ запроса. Максимум 1000 строк.
        offset = offset
        get_query_results_url = f'https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/{self.request_id}/results/0?project={self.FOLDER_ID}&offset={str(offset)}&limit=1000'
        response = requests.get(
            get_query_results_url,
            headers = self.headers
        )
        return response
    
    def if_cell_is_list(self,cell, type_of_empty_values = None): # функция участвует в преобразовании данных при создании файла
        if isinstance(cell, list):
            if len(cell) == 0:
                return type_of_empty_values
            else: 
                return cell[0]
        else:
            return cell
    
    def write_temp_file(self, type_of_empty_values = None):
        self.request_id = self.create_query()
        while str(self.get_request(0)) == '<Response [400]>':
            time.sleep(10)

        offset = 0
        response = self.get_request(offset) #запрашиваем данные запроса
        print(response)
        columns = [rows['name'] for rows in response.json()['columns']] #выделяем названия столбцов
        special_str = ""
        for j in columns:
            special_str = f"{special_str}{str(j)},"
        temp_file = open(self.TEMP_FILENAME, 'w', encoding='utf-8')
        temp_file.write(special_str[:-1]+'\n')

        while response.status_code == 200 and len(response.json()['rows']) != 0:  #Цикл делает запросы по 10000, пока не кончатся данные
            response = self.get_request(offset)
            response_rows = response.json()['rows']
            rows = [[self.if_cell_is_list(cell,type_of_empty_values) for cell in row] for row in response_rows]  #Преобразуются строки
            # Открывает созданный файл и добавляет в него строки
            for row in rows:
                special_str = ''
                for word in row:
                    if isinstance(word, type(None)):
                        special_str += ","
                    if isinstance(word, str):
                        special_str += "'{0}',".format(word.replace("'", ""))
                    else:
                        special_str += "{0},".format(word)
                temp_file.write(special_str[:-1]+'\n')  
            offset +=1000 # увеличивает смещение


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

def get_s3_instance(): # функция создает соединение
        session = boto3.session.Session()
        return session.client(
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net'
        )

def upload_dump_to_s3(key): # функция выгружает данные в s3
    get_s3_instance().upload_file(
        Filename=TEMP_FILENAME,
        Bucket=BUCKET_NAME,
        Key=key
    )

def download_query_text_from_s3(): # функция выгружает данные в s3
    get_s3_instance().download_file(
        Bucket= BUCKET_NAME,
        Key=f'query_texts/{FOLDER}.txt',
        Filename='/tmp/query.txt'
    )

def download_for_query(): # функция выгружает данные в s3
    get_s3_instance().download_file(
        Bucket= BUCKET_NAME,
        Key=f'for_functions/Dowloading_from_query.py',
        Filename='/tmp/Dowloading_from_query.py'
    )
    
def remove_temp_files(): #функция удаляет временный файл
    os.remove(TEMP_FILENAME)

def parameters_def(FOLDER,i):

    date = f"{dates_pd.loc[i,'date'].strftime('%Y-%m-%d')}"
    now = get_now_datetime_str()['now']
    query_text = open('/tmp/query.txt','r').read().format(date)
    query_name = f'{FOLDER} {now}'
    query_description = f'Ежедневный запрос- {date}'
    key = f"{FOLDER}/{dates_pd.loc[i,'date_key']}"

    return {
        'key':key,
        'date':date,
        'now':now,
        'headers':headers,
        'FOLDER_ID':FOLDER_ID,
        'query_text':query_text,
        'query_name':query_name,
        'query_description':query_description,
        'TEMP_FILENAME': TEMP_FILENAME
        }

def run(i,FOLDER):
    download_query_text_from_s3()
    parameters = parameters_def(FOLDER,i=i)
    dowloading_from_query = Dowloading_from_query()
    dowloading_from_query.get_query_parameters(parameters)
    dowloading_from_query.write_temp_file(type_of_empty_values)
    key = f"{FOLDER}/{dates_pd.loc[i,'date_key']}"
    upload_dump_to_s3(key)
    remove_temp_files()

yesterday_data = get_now_datetime_str()['yesterday_data']
start_date = datetime.datetime.strptime(yesterday_data,'%Y-%m-%d').date()
end_date = datetime.datetime.strptime(yesterday_data,'%Y-%m-%d').date()
dates_pd = pd.DataFrame({
        'date': pd.date_range(start=start_date, end=end_date),
        'date_key': pd.date_range(start=start_date, end=end_date).strftime('year=%Y/month=%m/%d.csv')
        })
dates_pd = dates_pd.iloc[::-1].reset_index(drop=True)

headers = None
def handler(event, context):
    global headers
    headers={'Authorization':context.token['access_token'],'Accept':'application/json'}

    for i in range(0,dates_pd.shape[0]):
        run(i,FOLDER)

    return {
        'statusCode': 200,
        'body': 'Hello World!',
    }