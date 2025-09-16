---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.17.2
  kernelspec:
    display_name: myenv
    language: python
    name: python3
---

## Start

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

___
### Tags: #Source #Partner

### Links: 
___


["id",                                                                                           
 "issue_id", - это на будущее,  когда будет группировка жалоб по ишьб                                                                                    
 "src_type", какая сущность пожаловалсь (Citizen/Camera/Intercom)                                                                             
 "src_uuid", - uuid этой сущности                                                                                 
 "src_address_uuid", - адрес этой сущности                                                                      
 "src_address_type",     адрес этой сущности                                                                        
 "device_uuid", - uuid устройтсва на которое жалуется сущность                                                                              
 "device_type",                                                                
 "err_code", код ошибки intercom_offline / video / no_intercom                                                                           
 "err_src", откуда пришло MOB (мобилка) / WEB (веб интерфейсы для партнеров) / API (бэк )/ DEV (устройство)                                                                              
 "err_func",  sync (там еще будет)                                                                                   
 "aasm_state",  - статус жалобы pending / processed / failed                                                                             
 "process_error", - ошибка если статус failed                                                                           
 "actor_type", - какой живой человек с сердцем пожаловался или авто система (будет либо Citizen, Partner, auto)
 "actor_identifier", - uuid / id актера
 "created_at",
 "updated_at"]

```python
# creating a table from s3

query_text = """--sql 
CREATE TABLE db1.complaints_st_partner
(
    `report_date` Date,
    `issue_id` String,
    `src_type` String,
    `src_uuid` String,
    `src_address_uuid` String,
    `src_address_type` String,
    `device_uuid` String,
    `device_type` String,
    `err_code` String,
    `err_src` String,
    `err_func` String,
    `aasm_state` String,
    `process_error` String,
    `actor_type` String,
    `actor_identifier` String,
    `created_at` DateTime64(3),
    `updated_at` DateTime64(3)
)
ENGINE = S3(
    'https://storage.yandexcloud.net/dwh-asgard/complaints_st_partner/year=*/month=*/*.csv',
    'CSVWithNames'
)
PARTITION BY report_date
SETTINGS date_time_input_format = 'best_effort'
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.complaints_st_partner_ch
(
    `report_date` Date,
    `issue_id` String,
    `src_type` String,
    `src_uuid` String,
    `src_address_uuid` String,
    `src_address_type` String,
    `device_uuid` String,
    `device_type` String,
    `err_code`	String,
    `err_src` String,	
    `err_func`	String,
    `aasm_state` String,	
    `process_error`	String,
    `actor_type` String,
    `actor_identifier` String,
    `created_at` DateTime64,
    `updated_at` DateTime64,
)
    ENGINE = MergeTree()
    ORDER BY src_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.complaints_st_partner_mv
    REFRESH EVERY 1 DAY OFFSET 4 HOUR RANDOMIZE FOR 1 HOUR TO db1.complaints_st_partner_ch AS
    SELECT
        *
    FROM db1.complaints_st_partner
ORDER BY report_date DESC
"""
ch.query_run(query_text)
```

___
## Tools
___
### query


```python
query_text = """--sql
    SELECT
        *
    FROM db1.complaints_st_partner_ch
    ORDER BY report_date DESC
    limit 100
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        *
    FROM db1.complaints_st_partner
    WHERE report_date = '2025-08-16'
    limit 100
    """

ch.query_run(query_text)

```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.complaints_st_partner_ch DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.complaints_st_partner
    """

ch.query_run(query_text)
```

### drop ch

```python
query_text = """--sql
    DROP TABLE db1.complaints_st_partner_ch
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.complaints_st_partner_mv
"""

ch.query_run(query_text)
```
