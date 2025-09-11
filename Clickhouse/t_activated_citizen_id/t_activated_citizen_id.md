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

```python
query_text = """--sql
    CREATE TABLE db1.t_activated_citizen_id
    (
        `report_date` Date,
        `address_uuid` String,
        `activated_citizen_id` UInt64,
        `flat_uuid` UInt64
    )
    ENGINE = MergeTree()
    ORDER BY address_uuid
    """
ch.query_run(query_text)
```

```python
start_date = datetime.datetime.strptime('2025-09-04','%Y-%m-%d').date()
end_date = datetime.datetime.strptime('2025-09-09','%Y-%m-%d').date()

dates_pd = pd.DataFrame({
        'date': pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d'),
        'date_key': pd.date_range(start=start_date, end=end_date).strftime('%/year=%-Y/month=%m/%d%')
        })
dates_pd = dates_pd.iloc[::-1].reset_index(drop=True)
for day_index in dates_pd.index:
    date = dates_pd.loc[day_index,['date']].values[0]
    query_text = f"""
        INSERT INTO db1.t_activated_citizen_id
        SELECT
            report_date,
            address_uuid,
            countDistinctIf(citizen_id, citizen_id != 0) AS activated_citizen_id,	
            countDistinctIf(flat_uuid, flat_uuid != '') AS flat_uuid
        FROM db1.rep_mobile_citizens_id_city_partner
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
    ALTER TABLE db1.t_activated_citizen_id DELETE WHERE report_date = '2025-09-09'
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.t_activated_citizen_id_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR 35 MINUTE TO db1.t_activated_citizen_id AS 
    SELECT
        report_date,
        address_uuid,
        countDistinctIf(citizen_id, citizen_id != 0) AS activated_citizen_id,	
        countDistinctIf(flat_uuid, flat_uuid != '') AS flat_uuid
    FROM db1.rep_mobile_citizens_id_city_partner
    WHERE address_uuid != ''
    GROUP BY report_date, address_uuid
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.t_activated_citizen_id
ORDER BY report_date DESC
LIMIT 10
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_activated_citizen_id
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_activated_citizen_id_mv
    """
ch.query_run(query_text)
```
