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
    OWNER_ID UInt64,
    OWNER_TYPE String,
    PRODUCT_ID UInt64,
    PRODUCT_NAME String,
    ORIGINAL_PRODUCT_NAME String,
    PRICE Float64,
    PRICE_EXCLUSIVE Float64,
    PRICE_NETTO Float64,
    PRICE_BRUTTO Float64,
    PRICE_ACCOUNT Float64,
    QUANTITY Float64,
    DISCOUNT_TYPE_ID Int32,
    DISCOUNT_RATE Float64,
    DISCOUNT_SUM Float64,
    TAX_RATE Float64,
    TAX_INCLUDED UInt8,        -- булево (0/1)
    CUSTOMIZED UInt8,          -- булево (0/1)
    MEASURE_CODE String,
    MEASURE_NAME String,
    SORT Int32,
    XML_ID String,
    TYPE String,
    STORE_ID UInt64,
    RESERVE_ID UInt64,
    DATE_RESERVE_END DateTime, -- дата/время окончания резерва
    RESERVE_QUANTITY Float64,
    DEAL_ID UInt64
)
```

```python
# creating a table from s3
query_text = """--sql 
CREATE TABLE db1.productrows_dir_bitrix
(
    ID UInt64,
    OWNER_ID UInt64,
    OWNER_TYPE String,
    PRODUCT_ID UInt64,
    PRODUCT_NAME String,
    ORIGINAL_PRODUCT_NAME String,
    PRICE Float64,
    PRICE_EXCLUSIVE Float64,
    PRICE_NETTO Float64,
    PRICE_BRUTTO Float64,
    PRICE_ACCOUNT Float64,
    QUANTITY Float64,
    DISCOUNT_TYPE_ID Int32,
    DISCOUNT_RATE Float64,
    DISCOUNT_SUM Float64,
    TAX_RATE String,
    TAX_INCLUDED String,        -- булево (0/1)
    CUSTOMIZED String,          -- булево (0/1)
    MEASURE_CODE String,
    MEASURE_NAME String,
    SORT String,
    XML_ID String,
    TYPE String,
    STORE_ID String,
    RESERVE_ID String,
    DATE_RESERVE_END String, -- дата/время окончания резерва
    RESERVE_QUANTITY String,
    DEAL_ID String
)
ENGINE = S3('https://storage.yandexcloud.net/aggregated-data/bitrix_productrows/*.csv','CSVWithNames')
SETTINGS format_csv_delimiter = ';'
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.productrows_dir_bitrix_ch
    (
        ID UInt64,
        OWNER_ID UInt64,
        OWNER_TYPE String,
        PRODUCT_ID UInt64,
        PRODUCT_NAME String,
        ORIGINAL_PRODUCT_NAME String,
        PRICE Float64,
        PRICE_EXCLUSIVE Float64,
        PRICE_NETTO Float64,
        PRICE_BRUTTO Float64,
        PRICE_ACCOUNT Float64,
        QUANTITY Float64,
        DISCOUNT_TYPE_ID Int32,
        DISCOUNT_RATE Float64,
        DISCOUNT_SUM Float64,
        TAX_RATE String,
        TAX_INCLUDED String,        -- булево (0/1)
        CUSTOMIZED String,          -- булево (0/1)
        MEASURE_CODE String,
        MEASURE_NAME String,
        SORT String,
        XML_ID String,
        TYPE String,
        STORE_ID String,
        RESERVE_ID String,
        DATE_RESERVE_END String, -- дата/время окончания резерва
        RESERVE_QUANTITY String,
        DEAL_ID String
)
    ENGINE = MergeTree()
    ORDER BY ID
    
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.productrows_dir_bitrix_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.productrows_dir_bitrix_ch AS
    SELECT
        *
    FROM db1.productrows_dir_bitrix
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
    FROM  db1.productrows_dir_bitrix_mv
    limit 100
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.productrows_dir_bitrix_mv
    """

ch.query_run(query_text)
```


### drop ch

```python
query_text = """--sql
    DROP TABLE db1.productrows_dir_bitrix_ch
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
