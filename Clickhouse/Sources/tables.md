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
## Drop the Table

```python
query_text = """
    DROP TABLE db1.sessions_st_mobile_mv
    """
ch.query_run(query_text)
```

___

## Refreshing the data

```python
query_text = """
SYSTEM REFRESH VIEW db1.sessions_st_mobile_mv
"""

ch.query_run(query_text)
```

___
## Change refresh time

```python
query_text = """
ALTER TABLE db1.companies_st_partner_mv
MODIFY REFRESH EVERY 1 DAY OFFSET 4 HOUR RANDOMIZE FOR 1 HOUR;
"""

ch.query_run(query_text)
```

___

## Show all tables

```python
query_text = """--sql
    SHOW TABLES FROM db1
"""
df = ch.query_run(query_text)

```

____

### Partner


[[uk_addresses_partner]]

[[users_st_partner]]

[[uk_addresses_st_partner]]

[[uk_dir_partner]]

[[uk_st_partner]]

[[flats_dir_partner]]

[[flats_st_partner]]

[[entries_installation_points_dir_partner]]

[[installation_point_st_partner]]

[[buildings_st_partner]]

[[gates_st_partner]]

[[accruals_dir_partner]]

[[billing_orders_dir_partner]]

[[billing_orders_devices_st_partner]]

[[companies_dir_partner]]

[[companies_st_partner]]

[[service_history_dir_partner]]

[[cameras_st_partner]]

[[cameras_dir_partner]]

[[intercoms_st_partner]]

[[intercoms_dir_partner]]

[[all_installetion_points_parquet]]

[[complaints_st_partner]]


___
### Asgard



[[no_video_on_stream_mobile_st_asgard]]

[[cameras_dir_asgard]]

[[cameras_st_asgard]]

[[intercoms_dir_asgard]]

[[intercoms_st_asgard]]

[[flussonic_stats_st_asgard]]

[[cameras_daily_percentage_online_st_asgard]]

[[intercoms_daily_percentage_online_st_asgard]]

[[reconnects_intercoms_st_asgard]]

[[hex_metrics_parquet_asgard]]

[[opendoor_types_mobile_st_asgard]]

[[metrics_all_intercoms_asgard]]

[[metrics_asgard]]


___
### Mobile


[[entries_st_mobile]]

[[citizen_payments_st_mobile]]

[[sessions_st_mobile]]

[[subscriptions_st_mobile]]

[[citizens_st_mobile]]

[[citizens_dir_mobile]]

[[citizen_payments_dir_mobile]]


___

### Modified sources


[[all_installetion_points_parquet]]


___

### Support


[[requests_st_support]]

[[categories_st_support]]

[[categories_sc_support]]


____

### Bitrix

[[category_id_dir_bitrix]]

[[companies_id_dir_bitrix]]

[[deals_base_dir_bitrix]]

[[productrows_dir_bitrix]]

