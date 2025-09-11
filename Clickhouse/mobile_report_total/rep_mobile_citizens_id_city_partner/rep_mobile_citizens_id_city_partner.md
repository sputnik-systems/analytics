---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.17.3
  kernelspec:
    display_name: myenv
    language: python
    name: python3
---

## Start

```python jupyter={"is_executing": false}
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

[[citizens_st_mobile]]

[[flats_dir_partner]]

[[entries_installation_points_dir_partner]]

[[installation_point_st_partner]]

[[companies_st_partner]]
___

```python jupyter={"is_executing": false}
query_text = """--sql
    SELECT
        citizens_st_mobile.report_date AS report_date,
        citizens_st_mobile.citizen_id AS citizen_id,
        citizens_st_mobile.trial_available AS trial_available,
        citizens_st_mobile.state AS state,
        citizens_st_mobile.flat_uuid AS flat_uuid,
        flats_dir_partner.address_uuid AS address_uuid,
        entries_installation_points_dir_partner.city AS city,
        installation_point_st_partner.partner_uuid AS partner_uuid,
        installation_point_st_partner.monetization AS monetization,
        installation_point_st_partner.monetization_is_allowed AS monetization_is_allowed,
        flats_dir_partner.created_at AS flat_created_at,
        citizens_st_mobile.activated_at AS activated_at
    FROM db1.citizens_st_mobile_ch AS citizens_st_mobile
    LEFT JOIN db1.flats_dir_partner_ch AS flats_dir_partner 
        ON flats_dir_partner.flat_uuid = citizens_st_mobile.flat_uuid
    LEFT JOIN db1.entries_installation_points_dir_partner_ch AS entries_installation_points_dir_partner 
        ON flats_dir_partner.address_uuid = entries_installation_points_dir_partner.address_uuid
    LEFT JOIN db1.installation_point_st_partner_ch AS installation_point_st_partner
        ON installation_point_st_partner.installation_point_id = entries_installation_points_dir_partner.installation_point_id
        AND installation_point_st_partner.report_date = citizens_st_mobile.report_date
    LEFT JOIN db1.companies_st_partner_ch AS companies_st_partner
        ON installation_point_st_partner.partner_uuid = companies_st_partner.partner_uuid
        AND companies_st_partner.report_date = citizens_st_mobile.report_date
    WHERE citizens_st_mobile.state = 'activated'
    limit 10
    """

ch.get_schema(query_text)
```

```python
query_text = """--sql
CREATE TABLE db1.rep_mobile_citizens_id_city_partner
(
    `report_date` Date,
    `citizen_id` Int32,
    `trial_available` Int32,
    `state` String,
    `flat_uuid` String,
    `address_uuid` String,
    `city` String,
    `partner_uuid` String,
    `monetization` Int16,
    `monetization_is_allowed` Int16,
    `flat_created_at` DateTime,
    `activated_at` DateTime
)
ENGINE = MergeTree()
ORDER BY citizen_id
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.rep_mobile_citizens_id_city_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR 55 MINUTE TO db1.rep_mobile_citizens_id_city_partner AS
   SELECT
        citizens_st_mobile.report_date AS report_date,
        citizens_st_mobile.citizen_id AS citizen_id,
        citizens_st_mobile.trial_available AS trial_available,
        citizens_st_mobile.state AS state,
        citizens_st_mobile.flat_uuid AS flat_uuid,
        flats_dir_partner.address_uuid AS address_uuid,
        entries_installation_points_dir_partner.city AS city,
        installation_point_st_partner.partner_uuid AS partner_uuid,
        installation_point_st_partner.monetization AS monetization,
        installation_point_st_partner.monetization_is_allowed AS monetization_is_allowed,
        flats_dir_partner.`created_at` AS flat_created_at,
        citizens_st_mobile_ch.activated_at AS activated_at
    FROM db1.citizens_st_mobile_ch AS citizens_st_mobile
    LEFT JOIN db1.flats_dir_partner_ch AS flats_dir_partner 
        ON flats_dir_partner.flat_uuid = citizens_st_mobile.flat_uuid
    LEFT JOIN db1.entries_installation_points_dir_partner_ch AS entries_installation_points_dir_partner 
        ON flats_dir_partner.address_uuid = entries_installation_points_dir_partner.address_uuid
    LEFT JOIN db1.installation_point_st_partner_ch AS installation_point_st_partner
        ON installation_point_st_partner.installation_point_id = entries_installation_points_dir_partner.installation_point_id
        AND installation_point_st_partner.report_date = citizens_st_mobile.report_date
    LEFT JOIN db1.companies_st_partner_ch AS companies_st_partner
        ON installation_point_st_partner.partner_uuid = companies_st_partner.partner_uuid
        AND companies_st_partner.report_date = citizens_st_mobile.report_date
    WHERE citizens_st_mobile.state = 'activated'
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
    FROM db1.rep_mobile_citizens_id_city_partner
    ORDER BY report_date DESC
    limit 100
    """

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
