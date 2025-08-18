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
## Tags: #Tables
___
## Links:
[[intercoms_st_partner]]

[[companies_st_partner]]

[[companies_dir_partner]]


___
## Table_creating

```python
query_text = """--sql
    CREATE TABLE db1.t_count_io_22_io_pro
    (
    `report_date` Date,
    `partner_uuid` String,
    `io_pro_count` UInt64,
    `gos22_count` UInt64,
    `io22_count` UInt64,
    `simple_count` UInt64,
    `status` String,
    `company_name` String,
    `partner_lk` String,
    `tin` String
    )
    ENGINE = MergeTree()
    ORDER BY partner_uuid
    """

ch.query_run(query_text)
```

___
## MV_creating

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.t_count_io_22_io_pro_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR 18 MINUTE TO db1.t_count_io_22_io_pro AS
	WITH intercom_count AS(
		SELECT
			report_date,
			partner_uuid,
			COUNT(DISTINCT IF(model_identifier = 'io_pro',intercom_uuid,null)) io_pro_count,
			COUNT(DISTINCT IF(model_identifier = 'gos22',intercom_uuid,null)) gos22_count,
			COUNT(DISTINCT IF(model_identifier = 'io22',intercom_uuid,null)) io22_count,
			COUNT(DISTINCT IF(model_identifier = '',intercom_uuid,null)) simple_count
		FROM db1.`intercoms_st_partner_ch` 
		GROUP BY 
			report_date,
			partner_uuid
		)
	SELECT
		intercom_count.report_date AS report_date,
		intercom_count.partner_uuid AS partner_uuid,
		io_pro_count,
		gos22_count,
		io22_count,
		simple_count,
		CASE 
			WHEN (`enterprise_subs` = 1 or `enterprise_not_paid` = 1) THEN 'Enterprise' 
			WHEN (`pro_subs` = 1) THEN 'PRO' 
			ELSE 'Start'
		END AS `status`,
		company_name,
		partner_lk,
		tin
	FROM intercom_count
	LEFT JOIN db1.`companies_st_partner_ch` ON intercom_count.partner_uuid = companies_st_partner_ch.partner_uuid 
											AND intercom_count.report_date = companies_st_partner_ch.report_date
	LEFT JOIN db1.`companies_dir_partner_ch` ON companies_dir_partner_ch.partner_uuid = intercom_count.partner_uuid
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
FROM db1.t_count_io_22_io_pro
WHERE gos22_count !=0
limit 100

"""

ch.query_run(query_text)
```

### refreash_mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_count_io_22_io_pro_mv
"""

ch.query_run(query_text)
```

___
### drop_table

```python
query_text = """ 
DROP TABLE db1.t_count_io_22_io_pro
"""

ch.query_run(query_text)
```

### drop_mv

```python
query_text = """ 
DROP TABLE db1.t_count_io_22_io_pro_mv
"""

ch.query_run(query_text)
```
