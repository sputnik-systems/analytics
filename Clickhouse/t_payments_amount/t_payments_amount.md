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
## Tags: #Tables

# Links:

[[rep_mobile_citizens_id_city_partner]]

[[citizen_payments_st_mobile_ch]]


```python
query_text = """--sql
    CREATE TABLE db1.t_payments_amount
    (
        `report_date` Date,
        `address_uuid` String,
        `payments_amount` UInt64
    )
    ENGINE = MergeTree()
    ORDER BY address_uuid
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
CREATE MATERIALIZED VIEW db1.t_payments_amount_mv
REFRESH EVERY 1 DAY OFFSET 5 HOUR 30 MINUTE TO db1.t_payments_amount AS 
SELECT
    report_date,
    address_uuid,
    sum(amount) AS payments_amount
FROM db1.rep_mobile_citizens_id_city_partner AS t_cit_id
JOIN db1.citizen_payments_st_mobile_ch AS citizen_payments_st_mobile
    ON citizen_payments_st_mobile.report_date = t_cit_id.report_date
    AND citizen_payments_st_mobile.citizen_id = t_cit_id.citizen_id
WHERE address_uuid != ''
GROUP BY report_date, address_uuid
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.t_payments_amount
order by report_date desc
LIMIT 10
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_payments_amount
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_payments_amount_mv
    """
ch.query_run(query_text)
```
