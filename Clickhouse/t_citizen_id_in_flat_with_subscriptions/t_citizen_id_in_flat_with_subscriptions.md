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
    display_name: Python 3 (ipykernel)
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

____
## Tags: #Tables

# Links:

[[t_full_citizen_id_in_flat_with_subscriptions]]

```python
query_text = """--sql
    CREATE TABLE db1.t_citizen_id_in_flat_with_subscriptions
    (
        `report_date` Date,
        `address_uuid` String,
        `citizen_id_in_flat_with_subscriptions` UInt64
    )
    ENGINE = MergeTree()
    ORDER BY address_uuid
    """
ch.query_run(query_text)

```

```python
query_text = """--sql
CREATE MATERIALIZED VIEW db1.t_citizen_id_in_flat_with_subscriptions_mv
REFRESH EVERY 1 DAY OFFSET 5 HOUR 27 MINUTE TO db1.t_citizen_id_in_flat_with_subscriptions AS 
SELECT
    report_date,
    address_uuid,
    COUNT(DISTINCT citizen_id) as citizen_id_in_flat_with_subscriptions
FROM db1.t_full_citizen_id_in_flat_with_subscriptions
WHERE address_uuid !=''
GROUP BY report_date,
        address_uuid
    """
ch.query_run(query_text)
```

```python
start_date = datetime.datetime.strptime('2023-09-01','%Y-%m-%d').date()
end_date = datetime.datetime.strptime('2025-09-05','%Y-%m-%d').date()

dates_pd = pd.DataFrame({
        'date': pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d'),
        'date_key': pd.date_range(start=start_date, end=end_date).strftime('%/year=%-Y/month=%m/%d%')
        })
dates_pd = dates_pd.iloc[::-1].reset_index(drop=True)
for day_index in dates_pd.index:
    date = dates_pd.loc[day_index,['date']].values[0]
    query_text = f"""
        INSERT INTO db1.t_citizen_id_in_flat_with_subscriptions
        SELECT
            report_date,
            address_uuid,
            COUNT(DISTINCT citizen_id) as citizen_id_in_flat_with_subscriptions
        FROM db1.t_full_citizen_id_in_flat_with_subscriptions
        WHERE address_uuid !=''
            AND report_date = '{date}'
        GROUP BY report_date,
                address_uuid
    """
    ch.query_run(query_text)
    print(date)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.t_citizen_id_in_flat_with_subscriptions
ORDER BY report_date desc
LIMIT 10
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_citizen_id_in_flat_with_subscriptions_mv
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_citizen_id_in_flat_with_subscriptions
    """
ch.query_run(query_text)
```

```python
query_text = """
    REFRASHE TABLE db1.t_citizen_id_in_flat_with_subscriptions_mv
    """
ch.query_run(query_text)
```
