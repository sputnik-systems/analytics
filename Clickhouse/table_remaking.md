---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.17.1
  kernelspec:
    display_name: myenv
    language: python
    name: python3
---

___
# Tags: #tools
___


```python
import clickhouse_connect
import datetime
import os
import pytz
import pandas as pd
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import sys
sys.path.append('/home/boris/Documents/Work/analytics/Clickhouse')
from clickhouse_client import ClickHouse_client
ch = ClickHouse_client()
pd.set_option('display.max_rows', 1000)
```

```python
query_text = """ 
SELECT
    *
FROM db1.citizens_st_mobile_parquet
WHERE report_date = '2025-06-01'
limit 10
"""

ch.query_run(query_text)
```

# citizens_st_mobile_parquet remaking

```python
start_date = datetime.datetime.strptime('2023-07-10','%Y-%m-%d').date()
end_date = datetime.datetime.strptime('2025-05-29','%Y-%m-%d').date()

dates_pd = pd.DataFrame({
        'date': pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d'),
        'date_key': pd.date_range(start=start_date, end=end_date).strftime('%/year=%Y/month=%m/%d.csv')
        })
dates_pd = dates_pd.iloc[::-1].reset_index(drop=True)
dates_pd
```

```python
start_date = datetime.datetime.strptime('2025-05-30','%Y-%m-%d').date()
end_date = datetime.datetime.strptime('2025-07-10','%Y-%m-%d').date()

dates_pd = pd.DataFrame({
        'date': pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d'),
        'date_key': pd.date_range(start=start_date, end=end_date).strftime('%/year=%-Y/month=%m/%d%')
        })
dates_pd = dates_pd.iloc[::-1].reset_index(drop=True)
for day_index in dates_pd.index:
    date = dates_pd.loc[day_index,['date']].values[0]
    date_key = dates_pd.loc[day_index,['date_key']].values[0]
    query_text = f"""
        INSERT INTO db1.citizens_st_mobile_ch
        SETTINGS s3_truncate_on_insert = 1
        SELECT
            report_date,
            citizen_id,
            trial_available,
            state,
            activated_at,
            flat_uuid,
            address_uuid
        FROM db1.citizens_st_mobile
        WHERE report_date = '{date}'
            AND _path LIKE '{date_key}'
    """
    ch.query_run(query_text)
    print(date)
```

```python
start_date = datetime.datetime.strptime('2023-07-10','%Y-%m-%d').date()
end_date = datetime.datetime.strptime('2025-05-29','%Y-%m-%d').date()

dates_pd = pd.DataFrame({
        'date': pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d'),
        'date_key': pd.date_range(start=start_date, end=end_date).strftime('%/year=%-Y/month=%m/%d%')
        })
dates_pd = dates_pd.iloc[::-1].reset_index(drop=True)
for day_index in dates_pd.index:
    date = dates_pd.loc[day_index,['date']].values[0]
    date_key = dates_pd.loc[day_index,['date_key']].values[0]
    query_text = f"""
        INSERT INTO db1.citizens_st_mobile_ch
        SETTINGS s3_truncate_on_insert = 1
        SELECT
            citizens_st_mobile.report_date AS report_date,
            citizens_st_mobile.citizen_id AS citizen_id,
            citizens_st_mobile.trial_available AS trial_available,
            citizens_st_mobile.state AS state,
            citizens_st_mobile_ch_2025_05_30.activated_at AS activated_at,
            citizens_st_mobile_ch_2025_05_30.flat_uuid AS flat_uuid,
            citizens_st_mobile_ch_2025_05_30.address_uuid AS address_uuid
        FROM db1.citizens_st_mobile AS citizens_st_mobile
        LEFT JOIN
            (SELECT
                citizen_id,
                flat_uuid,
                address_uuid,
                activated_at
            FROM db1.citizens_st_mobile
            WHERE report_date = '2025-05-30'
             AND _path LIKE '{date_key}') AS citizens_st_mobile_ch_2025_05_30
            ON citizens_st_mobile_ch_2025_05_30.citizen_id = citizens_st_mobile.citizen_id
        WHERE report_date = '{date}'
            AND _path LIKE '{date_key}'
    """
    ch.query_run(query_text)
    print(date)
# ch.query_run(query_text)

```

## Обновление данных после 2025-05-30

```python
start_date = datetime.datetime.strptime('2025-05-30','%Y-%m-%d').date()
end_date = datetime.datetime.strptime('2025-06-30','%Y-%m-%d').date()

dates_pd = pd.DataFrame({
        'date': pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d'),
        'date_key': pd.date_range(start=start_date, end=end_date).strftime('year=%Y/month=%m/%d.parquet')
        })
dates_pd = dates_pd.iloc[::-1].reset_index(drop=True)
for day_index in dates_pd.index:
    date = dates_pd.loc[day_index,['date']].values[0]
    date_key = dates_pd.loc[day_index,['date_key']].values[0]
    query_text = f"""
        INSERT INTO FUNCTION s3(
        'https://storage.yandexcloud.net/dwh-asgard/citizens_st_mobile_parquet/{date_key}',
        'parquet'
        )
        SETTINGS s3_truncate_on_insert = 1
        SELECT
            report_date,
            citizen_id,
            trial_available,
            state,
            activated_at,
            flat_uuid,
            address_uuid
        FROM db1.citizens_st_mobile_ch
        WHERE report_date = '{date}'
        
    """
    ch.query_run(query_text)
    print(date)
# ch.query_run(query_text)

```

```python
start_date = datetime.datetime.strptime('2025-07-04','%Y-%m-%d').date()
end_date = datetime.datetime.strptime('2025-07-04','%Y-%m-%d').date()

dates_pd = pd.DataFrame({
        'date': pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d'),
        'date_key': pd.date_range(start=start_date, end=end_date).strftime('year=%Y/month=%m/%d.parquet')
        })
dates_pd = dates_pd.iloc[::-1].reset_index(drop=True)
for day_index in dates_pd.index:
    date = dates_pd.loc[day_index,['date']].values[0]
    date_key = dates_pd.loc[day_index,['date_key']].values[0]
    query_text = f"""
        INSERT INTO FUNCTION s3(
        'https://storage.yandexcloud.net/dwh-asgard/citizens_st_mobile_parquet/{date_key}',
        'parquet'
        )
        SETTINGS s3_truncate_on_insert = 1
        SELECT
            report_date,
            citizen_id,
            trial_available,
            state,
            activated_at,
            flat_uuid,
            address_uuid
        FROM db1.citizens_st_mobile_ch
        WHERE report_date = '{date}'
        
    """
    ch.query_run(query_text)
    print(date)
# ch.query_run(query_text)
```

```python
query_text = """
SYSTEM REFRESH VIEW db1.citizens_st_mobile_parquet_mv
"""

ch.query_run(query_text)
```

citizens_st_mobile_parquet_mv

```python
query_text = """
SELECT
    *
FROM db1.citizens_st_mobile_parquet
WHERE flat_uuid != ''
ORDER BY report_date
limit 10
"""

ch.query_run(query_text)
```

```python
query_text = """
SELECT
    *
FROM db1.citizens_st_mobile_parquet
WHERE flat_uuid != ''
ORDER BY report_date
limit 10
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.citizens_st_mobile_remake_parquet_ch
    (
    `report_date` Date,
    `citizen_id` Int32,
    `trial_available` Int32,
    `state` String,
    `flat_uuid` String,
    `address_uuid` String
    )
    ENGINE = MergeTree()
    ORDER BY report_date
    """

ch.query_run(query_text)
```

```python
query_text = """
CREATE MATERIALIZED VIEW db1.citizens_st_mobile_remake_parquet_mv
REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.citizens_st_mobile_remake_parquet_ch AS
INSERT INTO FUNCTION s3(
    'https://storage.yandexcloud.net/dwh-asgard/citizens_st_mobile_remake_parquet_ch/{date_key}',
    'parquet'
    )
    SETTINGS s3_truncate_on_insert = 1
    SELECT
        report_date,
        citizen_id,
        trial_available,
        state,
        flat_uuid,
        address_uuid
    FROM db1.citizens_st_mobile_ch AS citizens_st_mobile
    WHERE report_date = '{date}'
        
    """

ch.query_run(query_text)
```

# add date for billing_orders_devices_st_partner

```python
import pandas as pd
```

```python
df = pd.read_parquet('/home/boris/Downloads/citizens_st_mobile_parquet.parquet')
```

```python
query_text = """--sql 
    CREATE TABLE db1.billing_orders_devices_dir_partner
    (   
        `billing_account_id` Int64,
        `cost` Float64,
        `count` Int64,
        `created_at` DateTime,
        `device_type` String,
        `device_uuid` String,
        `partner_uuid` String,
        `report_date` Date,
        `service` String,
        `state` String,
        `total` Float64
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/billing_orders_devices_dir_partner/*.csv','CSVWithNames')
    PARTITION BY billing_account_id
    """

ch.query_run(query_text)
```

```python
import pandas as pd
df = pd.read_parquet('/home/boris/Downloads/29.parquet')

df[df['flat_uuid'].isna() == True]
```

```python
query_text = """
    DROP TABLE db1.billing_orders_devices_dir_partner
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        *
    FROM db1.billing_orders_devices_st_partner
    WHERE report_date = '2025-05-22'
    limit 10
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        *
    FROM db1.billing_orders_devices_st_partner
    WHERE report_date = '2025-05-21'
    limit 10
    """

ch.query_run(query_text)
```

```python
start_date = datetime.datetime.strptime('2025-01-24','%Y-%m-%d').date()
end_date = datetime.datetime.strptime('2025-05-21','%Y-%m-%d').date()

dates_pd = pd.DataFrame({
        'date': pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d'),
        'date_key': pd.date_range(start=start_date, end=end_date).strftime('year=%-Y/month=%-m/%-d.csv')
        })
dates_pd = dates_pd.iloc[::-1].reset_index(drop=True)
for day_index in dates_pd.index:
    date = dates_pd.loc[day_index,['date']].values[0]
    date_key = dates_pd.loc[day_index,['date_key']].values[0]
    query_text = f"""
        INSERT INTO FUNCTION s3(
        'https://storage.yandexcloud.net/dwh-asgard/billing_orders_devices_st_partner/{date_key}',
        'CSVWithNames'
        )
        SETTINGS s3_truncate_on_insert = 1
            SELECT
            `billing_account_id`,
                `cost`,
                `count`,
                `created_at`,
                `device_type`,
                `device_uuid`,
                `partner_uuid`,
                toDate(created_at) AS `report_date`,
                `service`,
                `state`,
                `total`

            FROM db1.billing_orders_devices_dir_partner
        WHERE toDate(created_at) = '{date}'
        
    """
    ch.query_run(query_text)
    print(date)
# ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
   `billing_account_id`,
    `cost`,
    `count`,
    `created_at`,
    `device_type`,
    `device_uuid`,
    `partner_uuid`,
    toDate(created_at) AS `report_date`,
    `service`,
    `state`,
    `total`

FROM db1.billing_orders_devices_dir_partner
limit 10
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.billing_orders_devices_st_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.billing_orders_devices_st_partner_ch AS
    SELECT
        *
    FROM db1.billing_orders_devices_st_partner
    """

ch.query_run(query_text)
```

___

```python
query_text = """--sql
WITH full_table AS(
SELECT
    installation_point_st_partner.partner_uuid AS partner_uuid,
    installation_point_st_partner.installation_point_id  AS installation_point_id,
    sessions_st_mobile.citizen_id AS citizen_id,
    installation_point_st_partner.monetization AS monetization,
    subscriptions_st_mobile.state AS subscriptions_state,
    entries_st_mobile.ble_available AS ble_available,
    citizens_dir_mobile.activated_at AS activated_at,
    city
FROM db1.`sessions_st_mobile_ch` AS sessions_st_mobile
LEFT JOIN db1.subscriptions_st_mobile_ch AS subscriptions_st_mobile 
    ON subscriptions_st_mobile.citizen_id = sessions_st_mobile.citizen_id
    AND subscriptions_st_mobile.report_date = sessions_st_mobile.report_date
LEFT JOIN  db1.`citizens_st_mobile_ch` AS citizens_st_mobile 
    ON citizens_st_mobile.citizen_id = sessions_st_mobile.citizen_id
    AND citizens_st_mobile.report_date = sessions_st_mobile.report_date
LEFT JOIN db1.`citizens_dir_mobile_ch` AS citizens_dir_mobile ON citizens_dir_mobile.citizen_id = sessions_st_mobile.citizen_id
LEFT JOIN db1.`flats_dir_partner_ch` AS flats_dir_partner ON flats_dir_partner.flat_uuid = citizens_dir_mobile.flat_uuid
LEFT JOIN db1.`entries_installation_points_dir_partner_ch` AS entries_installation_points_dir_partner ON flats_dir_partner.address_uuid = entries_installation_points_dir_partner.address_uuid
LEFT JOIN db1.`installation_point_st_partner_ch` AS installation_point_st_partner 
            ON installation_point_st_partner.installation_point_id = entries_installation_points_dir_partner.installation_point_id
            AND installation_point_st_partner.report_date = citizens_st_mobile.report_date
LEFT JOIN db1.`entries_st_mobile_ch` AS entries_st_mobile
            ON `entries_st_mobile`.`report_date` = sessions_st_mobile.`report_date` 
            AND`entries_st_mobile`.`address_uuid` = flats_dir_partner.`address_uuid`
WHERE DateTime::MakeDate(DateTime::ParseIso8601(`last_use`)) 
    BETWEEN '2025-06-01' and '2025-06-30'
    AND citizens_st_mobile.state = 'activated'
)
--
SELECT
    '2025-06-30' AS report_date,
    partner_uuid,
    city,
    count(DISTINCT citizen_id) AS total_active_users,
    count(DISTINCT if(DateTime::MakeDate(DateTime::ParseIso8601(activated_at)) 
                    BETWEEN '2025-06-01' and '2025-06-30',citizen_id,null)) new_active_users,
    count(DISTINCT if(monetization = 1,citizen_id,null)) as total_active_users_monetization,
    count(DISTINCT if(monetization = 1 and subscriptions_state = 'activated',citizen_id,null)) as total_active_user_subscribed_monetization,
    count(DISTINCT if(ble_available = 'true',citizen_id)) AS total_active_users_ble_available,
    count(DISTINCT if(ble_available = 'true' and monetization = 1,citizen_id)) AS total_active_users_ble_available_monetization,
    count(DISTINCT if(ble_available = 'true' and monetization = 1 and subscriptions_state = 'activated',citizen_id)) AS total_active_users_ble_available_subscribed_monetization
FROM full_table
GROUP BY
    partner_uuid,
    city
LIMIT 10
    """

ch.query_run(query_text)
```

```python

```
