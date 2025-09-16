---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.17.3
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

```python
# creating a table from s3

query_text = """--sql 
CREATE TABLE db1.companies_st_partner
(
    `report_date` Date,
    `partner_uuid` String,
    `is_blocked` Int16,
    `pro_subs` Int16,
    `enterprise_subs` Int16,
    `billing_pro` Int16,
    `enterprise_not_paid` Int16,
    `enterprise_test` Int16,
    `balance` Float64,
    `tariff` String,
    `kz_pro` Int16
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/companies_st_partner/year=*/month=*/*.csv', 'CSVWithNames')
PARTITION BY partner_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.companies_st_partner_ch
(
    `report_date` Date,
    `partner_uuid` String,
    `is_blocked` Int16,
    `pro_subs` Int16,
    `enterprise_subs` Int16,
    `billing_pro` Int16,
    `enterprise_not_paid` Int16,
    `enterprise_test` Int16,
    `balance` Float64,
    `tariff` String,
    `kz_pro` Int16,
    `tariff_full` String
)
    ENGINE = MergeTree()
    ORDER BY partner_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.companies_st_partner_mv
    REFRESH EVERY 1 DAY OFFSET 4 HOUR RANDOMIZE FOR 1 HOUR TO db1.companies_st_partner_ch AS
    SELECT
        DISTINCT
        `report_date` ,
        `partner_uuid` ,
        `is_blocked` ,
        `pro_subs` ,
        `enterprise_subs` ,
        `billing_pro` ,
        `enterprise_not_paid` ,
        `enterprise_test` ,
        max(`balance`) OVER (partition by partner_uuid,report_date) AS `balance`,
        `kz_pro`,
        CASE
            WHEN pro_subs = 1 THEN 'pro'
            WHEN kz_pro = 1 THEN 'kz_pro'
            WHEN enterprise_subs = 1 then 'enterprise'
            ELSE 'start'
        END AS `tariff`,
        CASE
            WHEN enterprise_test = 1 then 'Enterprise Тест'
            WHEN enterprise_not_paid = 1 then 'Enterprise без биллинга'
            WHEN enterprise_subs = 1 then 'Enterprise'
            WHEN kz_pro = 1  then 'PRO Казахстан'
            WHEN pro_subs = 1 and billing_pro = 0 then 'PRO без биллинга'
            WHEN pro_subs = 1 and billing_pro = 1 then 'PRO'
            ELSE 'Start'
        END as tariff_full
    FROM db1.companies_st_partner
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
    FROM db1.companies_st_partner_ch
    ORDER BY report_date desc
    limit 100
    """

ch.query_run(query_text)

```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.companies_st_partner_ch DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.companies_st_partner_mv
    """

ch.query_run(query_text)
```


### drop ch

```python
query_text = """--sql
    DROP TABLE db1.companies_st_partner_ch
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.companies_st_partner_mv
"""

ch.query_run(query_text)
```
