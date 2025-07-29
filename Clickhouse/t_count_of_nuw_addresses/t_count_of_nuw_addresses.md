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
```

___
## Tags: #Tables
___
## Links:

[[installation_point_st_partner]]

[[entries_installation_points_dir_partner]]

[[intercoms_st_partner]]

[[companies_st_partner]]

[[citizens_st_mobile]]

[[citizens_dir_mobile]]

____
### Table

```python
query_text = """--sql
CREATE TABLE db1.t_count_of_nuw_addresses_ch 
(
	report_date Date,
    addresses UInt32,
    buildings UInt32,
    addresses_pro UInt32,
    addresses_entesprice UInt32,
    addresses_start UInt32,
    nuw_addresses_day UInt32,
    nuw_addresses_day_entesprice UInt32,
    nuw_addresses_day_start UInt32,
    nuw_addresses_day_pro UInt32,
    nuw_created_account_day UInt32,
    nuw_activated_account_day UInt32,
    nuw_buildings_day UInt32
)
ENGINE = MergeTree()
ORDER BY report_date
"""
ch.query_run(query_text)
```

### Materialized view

```python
query_text = """--sql
CREATE MATERIALIZED VIEW db1.t_count_of_nuw_addresses_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR TO db1.t_count_of_nuw_addresses_ch AS
SELECT
	report_date,
    addresses,
    buildings,
    addresses_pro,
    addresses_entesprice,
    addresses_start,
    nuw_addresses_day,
    nuw_addresses_day_entesprice,
    nuw_addresses_day_start,
    nuw_addresses_day_pro,
    nuw_created_account_day,
    nuw_activated_account_day,
    nuw_buildings_day
FROM (SELECT
    installation_point_st_partner.report_date AS report_date,
    COUNT(DISTINCT entries_installation_points_dir_partner.`address_uuid`) AS `addresses`,
    COUNT(DISTINCT entries_installation_points_dir_partner.`parent_uuid`) AS `buildings`,
    COUNT(DISTINCT IF(pro_subs = 1,entries_installation_points_dir_partner.`address_uuid` ,null)) AS `addresses_pro`,
    COUNT(DISTINCT IF(enterprise_subs = 1 OR enterprise_not_paid = 1 OR enterprise_test = 1,
                    entries_installation_points_dir_partner.`address_uuid` ,null)) AS `addresses_entesprice`,
    COUNT(DISTINCT IF(enterprise_subs != 1 AND enterprise_not_paid != 1 AND enterprise_test != 1 AND pro_subs != 1, 
                        entries_installation_points_dir_partner.`address_uuid` ,null)) AS `addresses_start`,
    COUNT(DISTINCT if(installation_point_st_partner.report_date = toDate(parseDateTimeBestEffortOrNull(`created_at`)),
                    entries_installation_points_dir_partner.`address_uuid`,null)) AS `nuw_addresses_day`,
    COUNT(DISTINCT if(installation_point_st_partner.report_date = toDate(parseDateTimeBestEffortOrNull(`created_at`)),
                    entries_installation_points_dir_partner.`parent_uuid`, null)) AS `nuw_buildings_day`,
    COUNT(DISTINCT IF(installation_point_st_partner.report_date = toDate(parseDateTimeBestEffortOrNull(`created_at`)) 
                    AND pro_subs = 1,entries_installation_points_dir_partner.`address_uuid` ,null)) AS `nuw_addresses_day_pro`,
    COUNT(DISTINCT IF(installation_point_st_partner.report_date = toDate(parseDateTimeBestEffortOrNull(`created_at`))  
                    AND (enterprise_subs = 1 OR enterprise_not_paid = 1 OR enterprise_test = 1),
                    entries_installation_points_dir_partner.`address_uuid` ,null)) AS `nuw_addresses_day_entesprice`,
    COUNT(DISTINCT IF(installation_point_st_partner.report_date = toDate(parseDateTimeBestEffortOrNull(`created_at`))  
                    AND enterprise_subs != 1 AND enterprise_not_paid != 1 AND enterprise_test != 1 AND pro_subs != 1, 
                        entries_installation_points_dir_partner.`address_uuid` ,null)) AS `nuw_addresses_day_start`
FROM db1.installation_point_st_partner_ch AS installation_point_st_partner
LEFT JOIN db1.entries_installation_points_dir_partner_ch AS entries_installation_points_dir_partner
    ON installation_point_st_partner.installation_point_id = entries_installation_points_dir_partner.installation_point_id
LEFT JOIN db1.intercoms_st_partner_ch AS intercoms_st_partner 
        ON intercoms_st_partner.installation_point_id = installation_point_st_partner.installation_point_id
        AND intercoms_st_partner.report_date = installation_point_st_partner.report_date
LEFT JOIN db1.companies_st_partner_ch AS companies_st_partner
        ON companies_st_partner.partner_uuid = installation_point_st_partner.partner_uuid
        AND companies_st_partner.report_date = installation_point_st_partner.report_date
WHERE intercom_uuid IS NOT NULL AND installation_point_st_partner.report_date > DATE('2023-08-28')
GROUP BY installation_point_st_partner.report_date AS report_date) AS addreses
LEFT JOIN 
(SELECT
    report_date,
    COUNT(DISTINCT if(report_date = toDate(created_at), citizens_st_mobile.citizen_id, NULL)) AS nuw_created_account_day,
    COUNT(DISTINCT if(report_date = toDate(activated_at), citizens_st_mobile.citizen_id, NULL)) AS nuw_activated_account_day
FROM db1.citizens_st_mobile_ch AS citizens_st_mobile
JOIN db1.citizens_dir_mobile_ch AS citizens_dir_mobile ON citizens_st_mobile.citizen_id = citizens_dir_mobile.citizen_id
GROUP BY citizens_st_mobile.report_date AS report_date) AS accounts ON accounts.report_date = addreses.report_date
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
FROM db1.t_count_of_nuw_addresses_ch
ORDER BY report_date DESC
limit 10

"""

ch.query_run(query_text)
```

### refreash_mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_count_of_nuw_addresses_mv
"""

ch.query_run(query_text)
```

___
### drop_table

```python
query_text = """ 
DROP TABLE db1.t_count_of_nuw_addresses_ch
"""

ch.query_run(query_text)
```

### drop_mv

```python
query_text = """ 
DROP TABLE db1.t_count_of_nuw_addresses_mv
"""

ch.query_run(query_text)
```
