---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.17.2
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
### Tags: #Source #Asgard

### Links: 
___

```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.metrics_all_intercoms_asgard
(
    `report_date` Date,
    `uuid` String,
    `call_stop_type_finish_handset` Int32,
    `ring_type_ring_cloud` Int32,
    `talk_open_door_type_analog` Int32,
    `call_stop_type_cancel_cloud` Int32,
    `key_state_valid` Int32,
    `ring_cluster_error_type_entrance_offline` Int32,
    `connection` Int32,
    `ring_type_ring_info` Int32,
    `ring_error_type_cancel` Int32,
    `call_success_true` Int32,
    `digital_key_success_false` Int32,
    `key_state_invalid` Int32,
    `ring_cluster_error_type_wrong_flat` Int32,
    `ring_error_type_cancel_handset` Int32,
    `talk_type_sip` Int32,
    `call_stop_type_cancel_button` Int32,
    `call_success_false` Int32,
    `open_door_type_api` Int32,
    `call_stop_type_speak_timeout` Int32,
    `ring_type_analog` Int32,
    `talk_type_analog`  Int32,
    `digital_key_success_true` Int32,
    `open_door_type_analog` Int32,
    `ring_type_sip` Int32,
    `talk_type_flat` Int32,
    `key_state_auth_err` Int32,
    `open_door_type_DTMF` Int32,
    `ring_cluster` Int32,
    `talk_open_door_type_api` Int32
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/metrics_all_intercoms_asgard/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.metrics_all_intercoms_asgard_ch
(
    `report_date` Date,
    `uuid` String,
    `call_stop_type_finish_handset` Int32,
    `ring_type_ring_cloud` Int32,
    `talk_open_door_type_analog` Int32,
    `call_stop_type_cancel_cloud` Int32,
    `key_state_valid` Int32,
    `ring_cluster_error_type_entrance_offline` Int32,
    `connection` Int32,
    `ring_type_ring_info` Int32,
    `ring_error_type_cancel` Int32,
    `call_success_true` Int32,
    `digital_key_success_false` Int32,
    `key_state_invalid` Int32,
    `ring_cluster_error_type_wrong_flat` Int32,
    `ring_error_type_cancel_handset` Int32,
    `talk_type_sip` Int32,
    `call_stop_type_cancel_button` Int32,
    `call_success_false` Int32,
    `open_door_type_api` Int32,
    `call_stop_type_speak_timeout` Int32,
    `ring_type_analog` Int32,
    `talk_type_analog`  Int32,
    `digital_key_success_true` Int32,
    `open_door_type_analog` Int32,
    `ring_type_sip` Int32,
    `talk_type_flat` Int32,
    `key_state_auth_err` Int32,
    `open_door_type_DTMF` Int32,
    `ring_cluster` Int32,
    `talk_open_door_type_api` Int32
)
    ENGINE = MergeTree()
    ORDER BY uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.metrics_all_intercoms_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.metrics_all_intercoms_asgard_ch AS
    SELECT
        *
    FROM db1.metrics_all_intercoms_asgard
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
    FROM db1.metrics_all_intercoms_asgard_ch
    ORDER BY report_date desc
    limit 100
    """

ch.query_run(query_text)

```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.metrics_all_intercoms_asgard_ch DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.metrics_all_intercoms_asgard_mv
    """

ch.query_run(query_text)
```


### drop ch

```python
query_text = """--sql
    DROP TABLE db1.metrics_all_intercoms_asgard_ch
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.metrics_all_intercoms_asgard_mv
"""

ch.query_run(query_text)
```
