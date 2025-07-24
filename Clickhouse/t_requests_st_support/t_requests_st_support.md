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

[[requests_st_support]]

[[intercoms_dir_asgard]]

[[intercoms_st_partner]]

[[companies_dir_partner]]

[[categories_st_support]]
___

```python
query_text = """--sql
CREATE TABLE db1.t_requests_st_support
    (
        `report_date` Date,
        `ticket_id` Int32,
        `intercom_id` String,
        `intercom_uuid` String,
        `camera_id` String,
        `server_stream_url` String,
        `detailed_category` String,
        `category` String,
        `subcategory` String,
        `maincategory` String,  
        `version_os` String,
        `version_app` String,
        `partner_uuid` String,
        `company_name` String,
        `partner_lk` String,
        `com_d_par.company_name` String,
        `device_type` String
    )
    ENGINE = MergeTree()
    ORDER BY report_date
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.t_requests_st_support_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR 35 MINUTE TO db1.t_requests_st_support AS
WITH t1 AS(
SELECT 
    req_s_sup.`report_date` AS report_date, 
    `ticket_id`, 
    `intercom_id`, 
    int_d_as.`intercom_uuid` AS intercom_uuid,
    `camera_id`,
    `server_stream_url`, 
    replaceAll(trim(arrayJoin(splitByChar(',',`category`))),'.','') AS `category`, 
    `version_os`, 
    `version_app`,
    int_s_par.`partner_uuid` AS partner_uuid,
    `company_name`,
    `partner_lk`,
    `device_type`
FROM `db1`.`requests_st_support_ch` AS req_s_sup
LEFT JOIN db1.intercoms_dir_asgard_ch AS int_d_as ON int_d_as.motherboard_id = req_s_sup.intercom_id
LEFT JOIN db1.intercoms_st_partner_ch AS int_s_par 
    ON  int_s_par.intercom_uuid = int_d_as.`intercom_uuid`
    AND int_s_par.report_date = req_s_sup.`report_date`
LEFT JOIN db1.companies_dir_partner_ch AS com_d_par
    ON int_s_par.partner_uuid = com_d_par.partner_uuid
   	),
--
t2 AS (
SELECT
	report_date,
	ticket_id,
	intercom_id,
	intercom_uuid,
	camera_id,
    server_stream_url,
    category,
    if(length(splitByChar(':',category)) = 2, trim(splitByChar(':',category)[2]),trim(splitByChar(':',category)[1])) AS `detailed_category`,
    version_os, 
    version_app,
    partner_uuid,
    company_name,
    partner_lk,
    device_type
FROM t1
)
--
SELECT
	t2.report_date AS report_date,
	ticket_id,
	intercom_id,
	intercom_uuid,
	camera_id,
    server_stream_url,
    category,
    `detailed_category`,
    if(splitByChar('|',Category)[2] = '','Main',splitByChar('|',Category)[2])  AS `subcategory`, 
    splitByChar('|',Category)[1] AS `maincategory`, 
    version_os, 
    version_app,
    partner_uuid,
    company_name,
    partner_lk,
    device_type
FROM t2
JOIN db1.categories_st_support_ch AS cat_s_sup 
	ON cat_s_sup.report_date = t2.report_date
	AND lowerUTF8(cat_s_sup.Details) = lowerUTF8(t2.detailed_category)
WHERE  lowerUTF8(subcategory) != 'info'
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
    FROM db1.t_requests_st_support
    WHERE subcategory != 'Info'
    ORDER BY report_date
    limit 10
    """

ch.query_run(query_text)

```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.t_requests_st_support DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.t_requests_st_support_mv
    """

ch.query_run(query_text)
```


### drop ch

```python
query_text = """--sql
    DROP TABLE db1.t_requests_st_support
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_requests_st_support_mv
"""

ch.query_run(query_text)
```
