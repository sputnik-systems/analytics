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
### Tags: #Mobile_Report

### Links:

[[rep_mobile_citizens_id_city_partner]]

[[sessions_st_mobile]]

[[citizens_dir_mobile]]
___

```python
query_text = """--sql
CREATE TABLE db1.maf_rep_mobile_total
(
    `report_date` Date,
    `MAF` UInt64,
    `stricted_MAF` UInt64,
    `freemonetization_MAF` UInt64
)
ENGINE = MergeTree()
ORDER BY report_date
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.maf_rep_mobile_total_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR 45 MINUTE TO db1.maf_rep_mobile_total AS
    WITH t_m AS (SELECT
        report_date,
        flat_uuid,
        monetization,
        citizen_id
    FROM db1.rep_mobile_citizens_id_city_partner
    WHERE toDate(`flat_created_at`) <= report_date
        AND state = 'activated'
    ),
    t_s AS 
    (SELECT 
        report_date,
        sessions_st_mobile_ch.citizen_id AS citizen_id
    FROM db1.`sessions_st_mobile_ch` AS sessions_st_mobile
    LEFT ANY JOIN  db1.`citizens_dir_mobile_ch` AS citizens_dir_mobile
            ON citizens_dir_mobile.citizen_id = sessions_st_mobile.citizen_id 
    WHERE toDateOrNull(citizens_dir_mobile.`created_at`) <= sessions_st_mobile.report_date
        AND (toDate(`last_use`) BETWEEN toStartOfMonth(sessions_st_mobile.report_date) AND sessions_st_mobile.report_date) 
    )
    --
    SELECT
        t_m.report_date AS report_date,
        COUNT(t_m.flat_uuid) AS MAF,
        COUNT(if(monetization = 0, t_m.flat_uuid, null)) AS stricted_MAF,
        COUNT(if(monetization = 1 or monetization is null, t_m.flat_uuid, null)) AS freemonetization_MAF
    FROM t_m
    ANY JOIN t_s 
        ON t_m.citizen_id = t_s.citizen_id 
        AND t_m.report_date = t_s.report_date
    GROUP BY report_date
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
    FROM db1.maf_rep_mobile_total
    ORDER BY report_date desc
    limit 10
    """

ch.query_run(query_text)

```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.maf_rep_mobile_total DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.maf_rep_mobile_total_mv
    """

ch.query_run(query_text)
```

### drop table

```python
query_text = """--sql
    DROP TABLE db1.maf_rep_mobile_total
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.maf_rep_mobile_total_mv
"""

ch.query_run(query_text)
```
