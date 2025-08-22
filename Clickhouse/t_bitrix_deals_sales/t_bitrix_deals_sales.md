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
### Tags: #Tables

### Links: 

[[deals_base_dir_bitrix]]

[[category_id_dir_bitrix]]

[[companies_id_dir_bitrix]]

[[companies_dir_partner]]
___

```python

```

```python
query_text = """--sql
WITH t1 AS(
SELECT 
	bdb.ID AS id,
	bdb.TITLE AS title,
	bdb.OPPORTUNITY AS opportunity ,
--	bdb.STAGE_ID AS stage_id ,
--	cidb.name AS stage_name,
--	bdb.CURRENCY_ID AS currency_id, 
	bdb.COMPANY_ID AS company_id,
	cidbc.company_name AS bitrix_company_name,
	toDate(parseDateTime32BestEffortOrNull(bdb.BEGINDATE)) AS date_start ,
	toDate(parseDateTime32BestEffortOrNull(bdb.CLOSEDATE)) AS date_finish,
	if(cidbc.partner_lk = 0,Null,cidbc.partner_lk) AS  partner_lk
FROM db1.deals_base_dir_bitrix_ch AS bdb 
LEFT JOIN (SELECT
			*
			FROM db1.category_id_dir_bitrix_ch
			WHERE category_id = 0
			)AS cidb ON cidb.stage_id = bdb.STAGE_ID
LEFT JOIN db1.companies_id_dir_bitrix_ch AS cidbc 
	ON toInt16(bdb.COMPANY_ID) = cidbc.company_id
WHERE bdb.CURRENCY_ID = 'RUB'
		AND stage_id = 'WON'
	)
--
SELECT
	t1.*,
	cdp.company_name AS partner_comapany_name
FROM t1
LEFT JOIN db1.companies_dir_partner  AS cdp 
	ON toInt16(t1.partner_lk) = toInt16(cdp.partner_lk)
ORDER BY date_finish DESC
"""

ch.get_schema(query_text)
```

```python
query_text = """--sql
CREATE TABLE db1.t_bitrix_deals_sales
(
    `id` UInt64,
    `title` String,
    `opportunity` Float64,
    `company_id` String,
    `bitrix_company_name` String,
    `date_start` Date,
    `date_finish` Date,
    `partner_lk` UInt32,
    `partner_comapany_name` String
)
    ENGINE = MergeTree()
    ORDER BY id
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.t_bitrix_deals_sales_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR 35 MINUTE TO db1.t_bitrix_deals_sales AS
    WITH t1 AS(
    SELECT 
        bdb.ID AS id,
        bdb.TITLE AS title,
        bdb.OPPORTUNITY AS opportunity ,
    --	bdb.STAGE_ID AS stage_id ,
    --	cidb.name AS stage_name,
    --	bdb.CURRENCY_ID AS currency_id, 
        bdb.COMPANY_ID AS company_id,
        cidbc.company_name AS bitrix_company_name,
        toDate(parseDateTime32BestEffortOrNull(bdb.BEGINDATE)) AS date_start ,
        toDate(parseDateTime32BestEffortOrNull(bdb.CLOSEDATE)) AS date_finish,
        if(cidbc.partner_lk = 0,Null,cidbc.partner_lk) AS  partner_lk
    FROM db1.deals_base_dir_bitrix_ch AS bdb 
    LEFT JOIN (SELECT
                *
                FROM db1.category_id_dir_bitrix_ch
                WHERE category_id = 0
                )AS cidb ON cidb.stage_id = bdb.STAGE_ID
    LEFT JOIN db1.companies_id_dir_bitrix_ch AS cidbc 
        ON toInt16(bdb.COMPANY_ID) = cidbc.company_id
    WHERE bdb.CURRENCY_ID = 'RUB'
            AND stage_id = 'WON'
        )
    --
    SELECT
        t1.*,
        cdp.company_name AS partner_comapany_name
    FROM t1
    LEFT JOIN db1.companies_dir_partner  AS cdp 
        ON toInt16(t1.partner_lk) = toInt16(cdp.partner_lk)
    ORDER BY date_finish DESC
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
    FROM db1.t_bitrix_deals_sales
    limit 10
    """

ch.query_run(query_text)

```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.t_bitrix_deals_sales DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.t_bitrix_deals_sales_mv
    """

ch.query_run(query_text)
```


### drop ch

```python
query_text = """--sql
    DROP TABLE db1.t_bitrix_deals_sales
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_uk_adresses_subscribtions_mv
"""

ch.query_run(query_text)
```
