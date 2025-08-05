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

[[total_active_users_per_day_full_table]]
___

```python
query_text = """--sql
CREATE TABLE db1.maf_rep_mobile_total
(
    `report_date` Date,
    `MAF` UInt64,
    `partner_uuid` String,
    `city` String,
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
    SELECT
        partner_uuid,
        city,
        report_date AS report_date,
        COUNT(DISTINCT flat_uuid) AS MAF,
        COUNT(DISTINCT if(monetization = 0, flat_uuid, null)) AS stricted_MAF,
        COUNT(DISTINCT if(monetization = 1 or monetization is null, flat_uuid, null)) AS freemonetization_MAF
    FROM db1.total_active_users_per_day_full_table
    GROUP BY report_date,
        partner_uuid,
        city
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
