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
### Tags: #Mobile_Report

### Links:  

[[rep_mobile_citizens_id_city_partner]]

[[citizens_dir_mobile]]
___
### Table

```python
query_text = """--sql
SELECT
    report_date,
    partner_uuid,
    city,
    COUNT(DISTINCT if(report_date = toDateOrNull(c.`created_at`), r.citizen_id, NULL)) AS `nuw_created_account_day`,
    COUNT(DISTINCT if(report_date = r.`activated_at`, r.citizen_id, NULL)) AS `nuw_activated_accoun_day`
FROM db1.rep_mobile_citizens_id_city_partner r
LEFT JOIN db1.`citizens_dir_mobile_ch` AS c ON c.`citizen_id`  = r.`citizen_id`
GROUP BY report_date,
         partner_uuid,
         city
    limit 10
    """

ch.get_schema(query_text)
```

```python
query_text = """--sql
CREATE TABLE db1.nuw_users_pd_rep_mobile_total
(
    `report_date` Date,
    `partner_uuid` String,
    `city` String,
    `nuw_created_account_day` UInt64,
    `nuw_activated_accoun_day` UInt64
)
ENGINE = MergeTree()
ORDER BY partner_uuid
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.nuw_users_pd_rep_mobile_total_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR 50 MINUTE TO db1.nuw_users_pd_rep_mobile_total AS
        SELECT
        report_date,
        partner_uuid,
        city,
        COUNT(DISTINCT if(report_date = toDateOrNull(c.`created_at`), r.citizen_id, NULL)) AS `nuw_created_account_day`,
        COUNT(DISTINCT if(report_date = r.`activated_at`, r.citizen_id, NULL)) AS `nuw_activated_accoun_day`
    FROM db1.rep_mobile_citizens_id_city_partner r
    LEFT JOIN db1.`citizens_dir_mobile_ch` AS c ON c.`citizen_id`  = r.`citizen_id`
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
    FROM db1.nuw_users_pd_rep_mobile_total
    WHERE nuw_created_account_day != 0
    limit 100
    """

ch.query_run(query_text)
```


### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.nuw_users_pd_rep_mobile_total DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)
```


### drop mv

```python
query_text = """--sql
    DROP TABLE db1.nuw_users_pd_rep_mobile_total_mv
    """

ch.query_run(query_text)
```

### drop ch

```python
query_text = """--sql
    DROP TABLE db1.nuw_users_pd_rep_mobile_total
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.nuw_users_pd_rep_mobile_total_mv
"""

ch.query_run(query_text)
```
