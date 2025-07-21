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


___
### Asgard



[[no_video_on_stream_mobile_st_asgard]]

[[cameras_dir_asgard]]

[[cameras_st_asgard]]

[[intercoms_dir_asgard]]


___
### Mobile


[[entries_st_mobile]]

[[citizen_payments_st_mobile]]

[[sessions_st_mobile]]

[[subscriptions_st_mobile]]

[[citizens_st_mobile]]

[[citizens_dir_mobile]]

[[citizen_payments_dir_mobile]]



____

## 



____

## [[intercoms_st_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.intercoms_st_asgard
(
    `report_date` Date,
    `intercom_uuid` String,
    `is_online` Int16,
    `last_online` DateTime,
    `last_offline` DateTime,
    `software_version` String,
    `digital_keys_count` Int16,
    `device_keys_count` Int16,
    `partner_uuid` String,
    `flat_range` String
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/intercoms_st_asgard/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.intercoms_st_asgard_ch
(
    `report_date` Date,
    `intercom_uuid` String,
    `is_online` Int16,
    `last_online` DateTime,
    `last_offline` DateTime,
    `software_version` String,
    `digital_keys_count` Int16,
    `device_keys_count` Int16,
    `partner_uuid` String,
    `flat_range` String
)
    ENGINE = MergeTree()
    ORDER BY intercom_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.intercoms_st_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.intercoms_st_asgard_ch AS
    SELECT
        *
    FROM db1.intercoms_st_asgard
    WHERE partner_uuid not like '%main:tokens:%'
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.intercoms_st_asgard_ch
limit 100

"""

ch.query_run(query_text)
```

____

## [[flussonic_stats_st_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.flussonic_stats_st_asgard
(
    `report_date` DateTime,
    `stream_name` String,
    `disabled` String,
    `status` String,
    `alive` String,
    `lifetime` Int64,
    `bitrate` Int32,
    `agent_status` String,
    `server` String,
    `dvr_depth` Int16,
    `dvr_depth_minute` Int64
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/flussonic_stats_st_asgard/year=*/month=*/day=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.flussonic_stats_st_asgard_ch
(
    `report_date` DateTime,
    `stream_name` String,
    `disabled` String,
    `status` String,
    `alive` String,
    `lifetime` Int64,
    `bitrate` Int32,
    `agent_status` String,
    `server` String,
    `dvr_depth` Int16,
    `dvr_depth_minute` Int64
)
    ENGINE = MergeTree()
    ORDER BY stream_name
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.flussonic_stats_st_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.flussonic_stats_st_asgard_ch AS
    SELECT
        *
    FROM db1.flussonic_stats_st_asgard
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.flussonic_stats_st_asgard_ch
limit 100

"""

ch.query_run(query_text)
```

____

## [[cameras_daily_percentage_online_st_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.cameras_daily_percentage_online_st_asgard
(
    `report_date` Date,
    `camera_uuid` String,
    `onlinePercent` Int16
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/cameras_daily_percentage_online_st_asgard/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.cameras_daily_percentage_online_st_asgard_ch
(
    `report_date` Date,
    `camera_uuid` String,
    `onlinePercent` Int16
)
    ENGINE = MergeTree()
    ORDER BY camera_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.cameras_daily_percentage_online_st_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.cameras_daily_percentage_online_st_asgard_ch AS
    SELECT
        *
    FROM db1.cameras_daily_percentage_online_st_asgard
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.cameras_daily_percentage_online_st_asgard_ch
limit 100

"""

ch.query_run(query_text)
```

____

## [[reconnects_intercoms_st_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.reconnects_intercoms_st_asgard
(
    `report_date` Date,
    `intercom_uuid` String,
    `count` Int16
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/reconnects_intercoms_st_asgard/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.reconnects_intercoms_st_asgard_ch
(
    `report_date` Date,
    `intercom_uuid` String,
    `count` Int16
)
    ENGINE = MergeTree()
    ORDER BY intercom_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.reconnects_intercoms_st_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.reconnects_intercoms_st_asgard_ch AS
    SELECT
        *
    FROM db1.reconnects_intercoms_st_asgard
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.reconnects_intercoms_st_asgard_ch
limit 100

"""

ch.query_run(query_text)
```

____

## [[intercoms_daily_percentage_online_st_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.intercoms_daily_percentage_online_st_asgard
(
    `report_date` Date,
    `intercom_uuid` String,
    `onlinePercent` Int16
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/intercoms_daily_percentage_online_st_asgard/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.intercoms_daily_percentage_online_st_asgard_ch
(
    `report_date` Date,
    `intercom_uuid` String,
    `onlinePercent` Int16
)
    ENGINE = MergeTree()
    ORDER BY intercom_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.intercoms_daily_percentage_online_st_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.intercoms_daily_percentage_online_st_asgard_ch AS
    SELECT
        *
    FROM db1.intercoms_daily_percentage_online_st_asgard
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.intercoms_daily_percentage_online_st_asgard_ch
ORDER BY report_date DESC
limit 100

"""

ch.query_run(query_text)
```




____

## [[opendoor_types_mobile_st_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.opendoor_types_mobile_st_asgard
(
    `report_date` Date,
    `opendoor_type` String,
    `count` Int16
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/opendoor_types_mobile_st_asgard/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.opendoor_types_mobile_st_asgard_ch
(
    `report_date` Date,
    `opendoor_type` String,
    `count` Int16
)
    ENGINE = MergeTree()
    ORDER BY opendoor_type
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.opendoor_types_mobile_st_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.opendoor_types_mobile_st_asgard_ch AS
    SELECT
        *
    FROM db1.opendoor_types_mobile_st_asgard
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.opendoor_types_mobile_st_asgard_ch
ORDER BY report_date DESC
limit 100

"""

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    DISTINCT
    opendoor_type
FROM db1.opendoor_types_mobile_st_asgard_ch
ORDER BY report_date DESC
limit 100

"""

ch.query_run(query_text)
```

____

## [[hex_metrics_parquet_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.hex_metrics_parquet_asgard
(
    `report_date` Date,
    `intercom_uuid` String,
    `key_hex` String,
    `count` Int64
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/hex_metrics_parquet_asgard/year=*/month=*/*.parquet','Parquet')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.hex_metrics_parquet_asgard_ch
(
    `report_date` Date,
    `intercom_uuid` String,
    `key_hex` String,
    `count` Int64
)
    ENGINE = MergeTree()
    ORDER BY intercom_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.hex_metrics_parquet_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.hex_metrics_parquet_asgard_ch AS
    SELECT
        *
    FROM db1.hex_metrics_parquet_asgard
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.hex_metrics_parquet_asgard_ch
WHERE report_date = '2025-03-01'
ORDER BY report_date DESC
limit 100

"""

ch.query_run(query_text)
```

____

## [[metrics_all_intercoms_asgard]]


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

```python
query_text = """--sql
SELECT
    *
FROM db1.metrics_all_intercoms_asgard_ch
WHERE report_date = '2025-03-01'
ORDER BY report_date DESC
limit 100

"""

ch.query_run(query_text)
```

```python

```

____

## [[metrics_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.metrics_asgard
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
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/metrics_asgard/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.metrics_asgard_ch
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
    CREATE MATERIALIZED VIEW db1.metrics_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.metrics_asgard_ch AS
    SELECT
        *
    FROM db1.metrics_asgard
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.metrics_asgard_ch
WHERE report_date = '2025-03-01'
ORDER BY report_date DESC
limit 100

"""

ch.query_run(query_text)
```
____
## [[all_installetion_points_parquet]]






бакет aggregated-data

```python
query_text = """--sql
CREATE TABLE db1.all_installetion_points_parquet
(
    `address_uuid` String,
    `city` String,
    `country` String,
    `created_at` String,
    `full_address` String,
    `installation_point_id` Int32,
    `parent_uuid` String ,
    `region` String,
    `report_date` Date
)

ENGINE = S3('https://storage.yandexcloud.net/aggregated-data/all_installetion_points_parquet/year=*/month=*/day=*/*.parquet','parquet')
"""

ch.query_run(query_text)
```

```python
csjue85puvc1ihvk38v4_0_3Y3oRgr8BUOStQmzePNR6Ux1JZ9SIpFr.parquet
```

```python
query_text="""--sql
SELECT * FROM db1.all_installetion_points_parquet
limit 10
"""
ch.query_run(query_text)
```

```python
query_text = """--sql
CREATE TABLE db1.all_installetion_points_parquet_ch
    (
    `address_uuid` String,
    `city` String,
    `country` String,
    `created_at` String,
    `full_address` String,
    `installation_point_id` Int32,
    `parent_uuid` String ,
    `region` String,
    `report_date` Date
    )
    ENGINE = MergeTree()
    ORDER BY installation_point_id
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.all_installetion_points_parquet_mv
    REFRESH EVERY 1 DAY OFFSET 4 HOUR TO db1.all_installetion_points_parquet_ch AS
    SELECT
        `address_uuid` String,
        `city` String,
        `country` String,
        `created_at` String,
        `full_address` String,
        `installation_point_id` Int32,
        `parent_uuid` String ,
        `region` String,
        `report_date` Date
    FROM db1.all_installetion_points_parquet
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        *
    FROM db1.all_installetion_points_parquet_ch
    limit 10
    """

ch.query_run(query_text)
```

```python
query_text = """
SYSTEM REFRESH VIEW db1.all_installetion_points_parquet_mv
"""

ch.query_run(query_text)
```

```python
query_text = """
DROP TABLE db1.all_installetion_points_parquet_mv
"""

ch.query_run(query_text)
```
