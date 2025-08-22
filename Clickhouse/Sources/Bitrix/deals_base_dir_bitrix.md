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
### Tags: #Source #Bitrix

### Links: 
___

```python
(
    ID UInt64,
    TITLE String,
    TYPE_ID String,
    STAGE_ID String,
    PROBABILITY String,
    CURRENCY_ID String,
    OPPORTUNITY Float64,
    IS_MANUAL_OPPORTUNITY String,
    TAX_VALUE Float64,
    LEAD_ID String,
    COMPANY_ID UInt64,
    CONTACT_ID String,
    QUOTE_ID UInt64,
    BEGINDATE Date,
    CLOSEDATE Date,
    ASSIGNED_BY_ID UInt64,
    CREATED_BY_ID UInt64,
    MODIFY_BY_ID UInt64,
    DATE_CREATE DateTime,
    DATE_MODIFY DateTime,
    OPENED UInt8,
    CLOSED UInt8,
    ADDITIONAL_INFO String,
    LOCATION_ID String,
    CATEGORY_ID UInt8,
    STAGE_SEMANTIC_ID String,
    IS_NEW UInt8,
    IS_RECURRING UInt8,
    IS_RETURN_CUSTOMER UInt8,
    IS_REPEATED_APPROACH UInt8,
    SOURCE_ID String,
    SOURCE_DESCRIPTION String,
    ORIGINATOR_ID String,
    ORIGIN_ID String,
    MOVED_BY_ID UInt64,
    MOVED_TIME DateTime,
    LAST_ACTIVITY_TIME DateTime,
    UTM_SOURCE String,
    UTM_MEDIUM String,
    UTM_CAMPAIGN String,
    UTM_CONTENT String,
    UTM_TERM String,
    PARENT_ID_186 UInt64,
    LAST_COMMUNICATION_TIME DateTime,
    LAST_ACTIVITY_BY UInt64
)
```

```python
# creating a table from s3

query_text = """--sql 
CREATE TABLE db1.deals_base_dir_bitrix
(
    ID UInt64,
    TITLE String,
    TYPE_ID String,
    STAGE_ID String,
    PROBABILITY String,
    CURRENCY_ID String,
    OPPORTUNITY Float64,
    IS_MANUAL_OPPORTUNITY String,
    TAX_VALUE Float64,
    LEAD_ID String,
    COMPANY_ID String,
    CONTACT_ID String,
    QUOTE_ID String,
    BEGINDATE String,
    CLOSEDATE String,
    ASSIGNED_BY_ID String,
    CREATED_BY_ID String,
    MODIFY_BY_ID String,
    DATE_CREATE String,
    DATE_MODIFY String,
    OPENED String,
    CLOSED String,
    ADDITIONAL_INFO String,
    LOCATION_ID String,
    CATEGORY_ID UInt8,
    STAGE_SEMANTIC_ID String,
    IS_NEW String,
    IS_RECURRING String,
    IS_RETURN_CUSTOMER String,
    IS_REPEATED_APPROACH String,
    SOURCE_ID String,
    SOURCE_DESCRIPTION String,
    ORIGINATOR_ID String,
    ORIGIN_ID String,
    MOVED_BY_ID String,
    MOVED_TIME String,
    LAST_ACTIVITY_TIME String,
    UTM_SOURCE String,
    UTM_MEDIUM String,
    UTM_CAMPAIGN String,
    UTM_CONTENT String,
    UTM_TERM String,
    PARENT_ID_186 String,
    LAST_COMMUNICATION_TIME String,
    LAST_ACTIVITY_BY String
)
ENGINE = S3('https://storage.yandexcloud.net/aggregated-data/bitrix_deals_base/*.csv','CSVWithNames')
SETTINGS format_csv_delimiter = ';'
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.deals_base_dir_bitrix_ch
(
    ID UInt64,
    TITLE String,
    TYPE_ID String,
    STAGE_ID String,
    PROBABILITY String,
    CURRENCY_ID String,
    OPPORTUNITY Float64,
    IS_MANUAL_OPPORTUNITY String,
    TAX_VALUE Float64,
    LEAD_ID String,
    COMPANY_ID String,
    CONTACT_ID String,
    QUOTE_ID String,
    BEGINDATE String,
    CLOSEDATE String,
    ASSIGNED_BY_ID String,
    CREATED_BY_ID String,
    MODIFY_BY_ID String,
    DATE_CREATE String,
    DATE_MODIFY String,
    OPENED String,
    CLOSED String,
    ADDITIONAL_INFO String,
    LOCATION_ID String,
    CATEGORY_ID UInt8,
    STAGE_SEMANTIC_ID String,
    IS_NEW String,
    IS_RECURRING String,
    IS_RETURN_CUSTOMER String,
    IS_REPEATED_APPROACH String,
    SOURCE_ID String,
    SOURCE_DESCRIPTION String,
    ORIGINATOR_ID String,
    ORIGIN_ID String,
    MOVED_BY_ID String,
    MOVED_TIME String,
    LAST_ACTIVITY_TIME String,
    UTM_SOURCE String,
    UTM_MEDIUM String,
    UTM_CAMPAIGN String,
    UTM_CONTENT String,
    UTM_TERM String,
    PARENT_ID_186 String,
    LAST_COMMUNICATION_TIME String,
    LAST_ACTIVITY_BY String
)
    ENGINE = MergeTree()
    ORDER BY ID
    
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.deals_base_dir_bitrix_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.deals_base_dir_bitrix_ch AS
    SELECT
        *
    FROM db1.deals_base_dir_bitrix
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
    FROM db1.deals_base_dir_bitrix_ch
    limit 100
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.deals_base_dir_bitrix_mv
    """

ch.query_run(query_text)
```


### drop ch

```python
query_text = """--sql
    DROP TABLE db1.deals_base_dir_bitrix_ch
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.cameras_dir_asgard_mv
"""

ch.query_run(query_text)
```
