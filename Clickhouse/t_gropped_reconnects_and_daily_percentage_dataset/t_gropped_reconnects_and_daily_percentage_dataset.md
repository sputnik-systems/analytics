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
### Tags: #Tables 

### Links:

[[intercoms_st_asgard]]

[[cameras_st_asgard]]

[[cameras_dir_asgard]]

[[intercoms_dir_asgard]]

[[intercoms_daily_percentage_online_st_asgard]]

[[reconnects_intercoms_st_asgard]]

[[companies_dir_partner]]
___

```python
query_text = """--sql
    SELECT
    report_date,
    software_version,
    partner_uuid,
    hardware_version,
    camera_fw_version,
    company_name,
    partner_lk,
    tin,
    kpp,
    SUM(is_online) AS daily_online,
    AVG(onlinePercent) AS avg_onlinePercent,
    AVG(reconnects) AS avg_reconnects,
    COUNT(intercom_uuid) AS intercom_count,
    countIf(onlinePercent != 0) AS if_onlinePercent_not_0_count
FROM
(
    SELECT
        isa.report_date   AS report_date,
        isa.intercom_uuid  AS intercom_uuid,
        isa.is_online  AS is_online,
        isa.software_version   AS software_version,
        isa.partner_uuid AS partner_uuid,
        hardware_version AS hardware_version,
        ifNull(rdp.onlinePercent, 0) AS onlinePercent,
        ifNull(rdp.reconnects, 0) AS reconnects,
        cs.camera_fw_version  AS camera_fw_version,
        company_name   AS company_name,
        partner_lk  AS partner_lk,
        tin  AS tin,
        kpp  AS kpp
    FROM db1.intercoms_st_asgard_ch AS isa
    LEFT JOIN
    (
        SELECT
            csa.report_date,
            cda.intercom_uuid,
            csa.camera_fw_version
        FROM db1.cameras_st_asgard_ch AS csa
        LEFT JOIN db1.cameras_dir_asgard_ch AS cda USING (camera_uuid)
    ) AS cs
        ON isa.intercom_uuid = cs.intercom_uuid
       AND isa.report_date = cs.report_date
    LEFT JOIN db1.intercoms_dir_asgard_ch AS idp
        ON isa.intercom_uuid = idp.intercom_uuid
    LEFT JOIN
    (
        SELECT
            dpo.intercom_uuid  AS intercom_uuid,
            dpo.report_date  AS report_date,
            dpo.onlinePercent AS onlinePercent,
            ifNull(ri.count, 0) AS reconnects
        FROM db1.intercoms_daily_percentage_online_st_asgard_ch AS dpo
        LEFT JOIN db1.reconnects_intercoms_st_asgard_ch AS ri
            ON ri.report_date = dpo.report_date
           AND ri.intercom_uuid = dpo.intercom_uuid
        WHERE dpo.report_date >= toDate('2024-02-06')
    ) AS rdp
        ON isa.report_date = rdp.report_date
       AND isa.intercom_uuid = rdp.intercom_uuid
    LEFT JOIN db1.companies_dir_partner_ch AS cdp
        ON isa.partner_uuid = cdp.partner_uuid
)
GROUP BY
    report_date,
    software_version,
    camera_fw_version,
    partner_uuid,
    hardware_version,
    company_name,
    partner_lk,
    tin,
    kpp
    limit 10
    """

ch.get_schema(query_text)
```

```python
query_text = """--sql
CREATE TABLE db1.t_gropped_reconnects_and_daily_percentage_dataset
(
    `report_date` Date,
    `software_version` String,
    `partner_uuid` String,
    `hardware_version` String,
    `camera_fw_version` String,
    `company_name` String,
    `partner_lk` String,
    `tin` String,
    `kpp` String,
    `daily_online` Int64,
    `avg_onlinePercent` Float64,
    `avg_reconnects` Float64,
    `intercom_count` UInt64,
    `if_onlinePercent_not_0_count` UInt64
)
ENGINE = MergeTree()
ORDER BY partner_uuid
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.t_gropped_reconnects_and_daily_percentage_dataset_mv
    REFRESH EVERY 1 DAY OFFSET 4 HOUR 5 MINUTE TO db1.t_gropped_reconnects_and_daily_percentage_dataset AS
      SELECT
    report_date,
    software_version,
    partner_uuid,
    hardware_version,
    camera_fw_version,
    company_name,
    partner_lk,
    tin,
    kpp,
    SUM(is_online) AS daily_online,
    AVG(onlinePercent) AS avg_onlinePercent,
    AVG(reconnects) AS avg_reconnects,
    COUNT(intercom_uuid) AS intercom_count,
    countIf(onlinePercent != 0) AS if_onlinePercent_not_0_count
FROM
(
    SELECT
        isa.report_date   AS report_date,
        isa.intercom_uuid  AS intercom_uuid,
        isa.is_online  AS is_online,
        isa.software_version   AS software_version,
        isa.partner_uuid AS partner_uuid,
        hardware_version AS hardware_version,
        ifNull(rdp.onlinePercent, 0) AS onlinePercent,
        ifNull(rdp.reconnects, 0) AS reconnects,
        cs.camera_fw_version  AS camera_fw_version,
        company_name   AS company_name,
        partner_lk  AS partner_lk,
        tin  AS tin,
        kpp  AS kpp
    FROM db1.intercoms_st_asgard_ch AS isa
    LEFT JOIN
    (
        SELECT
            csa.report_date,
            cda.intercom_uuid,
            csa.camera_fw_version
        FROM db1.cameras_st_asgard_ch AS csa
        LEFT JOIN db1.cameras_dir_asgard_ch AS cda USING (camera_uuid)
    ) AS cs
        ON isa.intercom_uuid = cs.intercom_uuid
       AND isa.report_date = cs.report_date
    LEFT JOIN db1.intercoms_dir_asgard_ch AS idp
        ON isa.intercom_uuid = idp.intercom_uuid
    LEFT JOIN
    (
        SELECT
            dpo.intercom_uuid  AS intercom_uuid,
            dpo.report_date  AS report_date,
            dpo.onlinePercent AS onlinePercent,
            ifNull(ri.count, 0) AS reconnects
        FROM db1.intercoms_daily_percentage_online_st_asgard_ch AS dpo
        LEFT JOIN db1.reconnects_intercoms_st_asgard_ch AS ri
            ON ri.report_date = dpo.report_date
           AND ri.intercom_uuid = dpo.intercom_uuid
        WHERE dpo.report_date >= toDate('2024-02-06')
    ) AS rdp
        ON isa.report_date = rdp.report_date
       AND isa.intercom_uuid = rdp.intercom_uuid
    LEFT JOIN db1.companies_dir_partner_ch AS cdp
        ON isa.partner_uuid = cdp.partner_uuid
)
GROUP BY
    report_date,
    software_version,
    camera_fw_version,
    partner_uuid,
    hardware_version,
    company_name,
    partner_lk,
    tin,
    kpp
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
    FROM db1.t_gropped_reconnects_and_daily_percentage_dataset
    ORDER BY report_date DESC
    limit 100 """

ch.query_run(query_text)

```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.rep_mobile_citizens_id_city_partner DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.rep_mobile_citizens_id_city_partner_mv
    """

ch.query_run(query_text)
```

### drop ch

```python
query_text = """--sql
    DROP TABLE db1.rep_mobile_citizens_id_city_partner
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.rep_mobile_citizens_id_city_partner_mv
"""

ch.query_run(query_text)
```
