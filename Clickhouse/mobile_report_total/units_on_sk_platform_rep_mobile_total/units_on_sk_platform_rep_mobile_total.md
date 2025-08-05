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

<!-- #region -->
___
### Tags: #Mobile_Report

### Links:  

[[installation_point_st_partner]]

[[intercoms_st_partner]]

[[entries_installation_points_dir_partner]]

[[companies_st_partne]]


### Table
<!-- #endregion -->

```python
query_text = """--sql
CREATE TABLE db1.units_on_sk_platform_rep_mobile_total
(
    `report_date` Date,
    `partner_uuid` String,
    `city` String,
    `units_on_platform` UInt64,
    `units_stricted monetization` UInt64,
    `units_free_monetization` UInt64,
    `units_free_monetization_pro` UInt64,
    `units_free_monetization_start` UInt64
)
ENGINE = MergeTree()
ORDER BY partner_uuid
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.units_on_sk_platform_rep_mobile_total_mv
    REFRESH EVERY 1 DAY OFFSET 6 HOUR 8 MINUTE TO db1.units_on_sk_platform_rep_mobile_total AS
    SELECT
        inst_p_st.report_date AS report_date,
        inst_p_st.`partner_uuid` AS partner_uuid,
        entr_p_dir.city AS city,
        count(distinct if(intercom_uuid is not NULL, `address_uuid`,null)) as `units_on_platform`,
        count(distinct if(monetization = 0 and intercom_uuid is not NULL, `address_uuid`,null)) as `units_stricted monetization`,
        count(distinct if(monetization = 1 and intercom_uuid is not NULL, `address_uuid`,null)) as `units_free_monetization`,
        count(distinct if(monetization = 1 and intercom_uuid is not NULL  and pro_subs = 1, `address_uuid`,null)) as `units_free_monetization_pro`,
        count(distinct if(monetization = 1 
                        and intercom_uuid is not NULL 
                        and pro_subs = 1
                        and (enterprise_subs = 0 or enterprise_subs is null) 
                        and (enterprise_not_paid = 0 or enterprise_not_paid is null)  
                        and (enterprise_test = 0 or enterprise_test is null)
                        , `address_uuid`,null)) as `units_free_monetization_start`
    FROM db1.`installation_point_st_partner_ch` AS inst_p_st
    ANY JOIN db1.`intercoms_st_partner_ch` AS int_st
                    ON int_st.installation_point_id = inst_p_st.installation_point_id
                    AND int_st.report_date = inst_p_st.report_date
    LEFT ANY JOIN db1.`entries_installation_points_dir_partner_ch` AS entr_p_dir 
        ON inst_p_st.installation_point_id = entr_p_dir.installation_point_id
    LEFT ANY JOIN db1.`companies_st_partner_ch` AS comp_st
                    ON comp_st.`partner_uuid` = int_st.`partner_uuid`
                    AND comp_st.`report_date` = int_st.`report_date`
    GROUP BY  
        report_date,
        partner_uuid,
        city
    """
ch.query_run(query_text)
```

```python
mobile_report_rep_mobile_full
```

___
## Tools
___
### query


```python
query_text = """--sql
    SELECT
        *
    FROM db1.units_on_sk_platform_rep_mobile_total
    ORDER BY report_date DESC
    limit 100
    """

ch.query_run(query_text)
```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.units_on_sk_platform_rep_mobile_total DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)
```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.units_on_sk_platform_rep_mobile_total_mv
    """

ch.query_run(query_text)
```

### drop ch

```python
query_text = """--sql
    DROP TABLE db1.units_on_sk_platform_rep_mobile_total
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.units_on_sk_platform_rep_mobile_total_mv
"""

ch.query_run(query_text)
```
