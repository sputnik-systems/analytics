---
jupyter:
  jupytext:
    formats: ipynb,md
    main_language: python
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.17.2
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

[[subscriptions_report_citizens_flats_comerce_rep_mobile_total]]

```python
query_text = """--sql
    CREATE TABLE db1.t_subscribed_citizen_id
    (
        `report_date` Date,
        `address_uuid` String,
        `subscribed_citizen_id` UInt64
    )
    ENGINE = MergeTree()
    ORDER BY address_uuid
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.t_subscribed_citizen_id_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR 33 MINUTE TO db1.t_subscribed_citizen_id AS 
    SELECT
        report_date,
        address_uuid,
        countDistinctIf(citizen_id, citizen_id != 0) AS subscribed_citizen_id
    FROM db1.subscriptions_report_citizens_flats_comerce_rep_mobile_total
    WHERE state = 'activated' AND address_uuid != ''
    GROUP BY report_date, address_uuid
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.t_subscribed_citizen_id
order by report_date desc
LIMIT 10
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_subscribed_citizen_id
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_subscribed_citizen_id_mv
    """
ch.query_run(query_text)
```
