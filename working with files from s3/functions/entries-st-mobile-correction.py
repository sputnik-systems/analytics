import requests
import time
import json
import os
import datetime
import boto3
import pytz
import pandas as pd

def handler(event, context):

    FOLDER_ID = os.getenv("FOLDER_ID") # id каталога из которого береться запрос
    FOLDER = os.getenv("FOLDER") #имя папки в которую будет выкладываться
    ACCESS_KEY = os.getenv("ACCESS_KEY") #aws_access_key_id для S3
    SECRET_KEY = os.getenv("SECRET_KEY") #aws_secret_access_key в s3
    BUCKET_NAME = os.getenv("BUCKET_NAME") #имя бакета
    TIME_ZONE = os.getenv("TIME_ZONE", "Europe/Moscow") #настройка функции
    TEMP_FILENAME = "/tmp/temp_file"

    def get_now_datetime_str(): # получаем актуальное время
        time_zone = os.getenv("TIME_ZONE", "Europe/Moscow") # меняем таймзону на московскую
        now = datetime.datetime.now(pytz.timezone(time_zone))    
        yesterday = now - datetime.timedelta(days=1) #нужна вчерашняя дата так как данные за прошлый день
        return {'key':yesterday.strftime('year=%Y/month=%m/%d.csv'),

                'now':now.strftime('%Y-%m-%d %H:%M:%S'),
                'yesterday_data':yesterday.strftime('%Y-%m-%d'),
                'yesterday':yesterday.strftime('%Y-%m-%d %H:%M:%S'), 
                'year':yesterday.strftime('%Y'),
                'month':yesterday.strftime('%m'),
                'day':yesterday.strftime('%d')}


    def get_s3_instance():
        session = boto3.session.Session()
        return session.client(
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net'
    )

    s3 = get_s3_instance()
    key = get_now_datetime_str()['key']

    s3.download_file(Bucket=BUCKET_NAME,Key = f'entries/{key}',Filename='/tmp/entries')
    s3.download_file(Bucket=BUCKET_NAME,Key = f'entries_st_mobile/{key}',Filename='/tmp/entries_st_mobile')

    entries_df = pd.read_csv('/tmp/entries')
    entries_df['monetization'] = entries_df['subsc_restrict'].apply(lambda x: "True" if x == 'freemonetization' else "False")
    entries_df['r_address_uuid'] = entries_df['address_uuid']
    
    entries_st_mobile_df = pd.read_csv('/tmp/entries_st_mobile')
    entries_st_mobile_df['l_address_uuid'] = entries_st_mobile_df['address_uuid']

    merget_df = entries_st_mobile_df.merge(
        entries_df[['r_address_uuid','address','monetization','partner_uuid']],
        left_on='l_address_uuid', 
        right_on='r_address_uuid',
        how='outer',
        suffixes=('_left', '_right'))
        
    merget_df_unique_from_entries_st_mobile = merget_df[merget_df['r_address_uuid'] \
                                            .isnull()][['report_date','address_uuid','partner_uuid_left','monetization_left']]
    merget_df_unique_from_entries_st_mobile = merget_df_unique_from_entries_st_mobile \
                                            .rename(columns={'partner_uuid_left':'partner_uuid','partner_uuid_left':'partner_uuid'})
        
    entries_df_for_concatenation = entries_df[['report_date','address_uuid','partner_uuid','monetization']]
    entries_df_for_concatenation = entries_df_for_concatenation.drop_duplicates()
    df_concated = pd.concat([entries_df_for_concatenation, merget_df_unique_from_entries_st_mobile]).reset_index().drop('index', axis=1)
    
    df_concated.to_csv(f'/tmp/new_entries_st_mobile',sep=',', index=False)

    s3_file_path = f'entries_st_mobile/{key}'
    pc_file_path = f'/tmp/new_entries_st_mobile'
    s3.upload_file(pc_file_path, BUCKET_NAME, s3_file_path)

    return {
        'statusCode': 200,
        'body': 'Hello World!',
    }