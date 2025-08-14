---
jupyter:
  jupytext:
    default_lexer: ipython3
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

[[rep_mobile_citizens_id_city_partner]]

[[citizens_dir_mobile]]
___
### Table

```python
query_text = """--sql
CREATE TABLE db1.new_users_pd_rep_mobile_total
(
    `report_date` Date,
    `partner_uuid` String,
    `city` String,
    `new_created_account_day` UInt64,
    `new_created_account` UInt64,
    `new_activated_account_day` UInt64,
    `new_activated_account` UInt64
)
ENGINE = MergeTree()
ORDER BY partner_uuid
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.new_users_pd_rep_mobile_total_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR 30 MINUTE TO db1.new_users_pd_rep_mobile_total AS
        WITH t_created_at AS(
            SELECT
                report_date,
                partner_uuid,
                city,
                activated_at,
                created_at,
                citizen_id,
                ROW_NUMBER() OVER (
                            PARTITION BY
                                toStartOfMonth(report_date),
                                citizen_id
                            ORDER BY report_date
                ) = 1 AS is_first_mention_in_day
            FROM
                (SELECT
                    report_date,
                    partner_uuid,
                    city,
                    total_act.activated_at AS activated_at,
                    toDateOrNull(cit_dir_m.`created_at`) AS created_at,
                    citizen_id
                FROM db1.rep_mobile_citizens_id_city_partner AS total_act
                LEFT JOIN db1.`citizens_dir_mobile_ch` AS cit_dir_m
                            ON `cit_dir_m`.`citizen_id` = total_act.`citizen_id`
                )
            WHERE created_at = report_date
        ),
        --
        t_activated_at AS(
            SELECT
                report_date,
                partner_uuid,
                city,
                activated_at,
                created_at,
                citizen_id,
                ROW_NUMBER() OVER (
                            PARTITION BY
                                toStartOfMonth(report_date),
                                citizen_id
                            ORDER BY report_date
                ) = 1 AS is_first_mention_in_day
            FROM
                (SELECT
                    report_date,
                    partner_uuid,
                    city,
                    toDate(total_act.activated_at) AS activated_at,
                    toDateOrNull(cit_dir_m.`created_at`) AS created_at,
                    citizen_id
                FROM db1.rep_mobile_citizens_id_city_partner AS total_act
                LEFT JOIN db1.`citizens_dir_mobile_ch` AS cit_dir_m
                            ON `cit_dir_m`.`citizen_id` = total_act.`citizen_id`
                )
            WHERE activated_at = report_date
        ),
        --
        dec_created_at AS (SELECT
            DISTINCT
            t1.report_date  AS report_date,
            t2.city  AS city,
            t2.partner_uuid  AS partner_uuid
        FROM
            (SELECT DISTINCT report_date FROM t_created_at) AS t1
        CROSS JOIN
            (SELECT DISTINCT city, partner_uuid FROM t_created_at ) AS t2
        ),
        --
        dec_activated_at AS (SELECT
            DISTINCT
            t1.report_date  AS report_date,
            t2.city  AS city,
            t2.partner_uuid  AS partner_uuid
        FROM
            (SELECT DISTINCT report_date FROM t_activated_at) AS t1
        CROSS JOIN
            (SELECT DISTINCT city, partner_uuid FROM t_activated_at ) AS t2
        ),
        --
        dec_and_t_created_at AS (
            SELECT
                dec_created_at.report_date AS report_date,
                dec_created_at.partner_uuid AS partner_uuid,
                dec_created_at.city AS city,
                new_created_account_day
            FROM dec_created_at
            LEFT join (
                SELECT 
                    report_date,
                    partner_uuid,
                    city,
                    count(DISTINCT if(is_first_mention_in_day = 1,citizen_id,NULL)) AS new_created_account_day
                FROM t_created_at 
                GROUP BY
                    report_date,
                    partner_uuid,
                    city) AS full_table
                ON dec_created_at.report_date = full_table.report_date 
                AND dec_created_at.partner_uuid = full_table.partner_uuid
                AND dec_created_at.city = full_table.city
        ),
        --
        dec_and_t_activated_at AS (
            SELECT
                dec_activated_at.report_date AS report_date,
                dec_activated_at.partner_uuid AS partner_uuid,
                dec_activated_at.city AS city,
                new_activated_account_day
            FROM dec_activated_at
            LEFT join (
                SELECT 
                    report_date,
                    partner_uuid,
                    city,
                    count(DISTINCT if(is_first_mention_in_day = 1,citizen_id,NULL)) AS new_activated_account_day
                FROM t_activated_at 
                GROUP BY
                    report_date,
                    partner_uuid,
                    city
                    ) AS full_table
                ON dec_activated_at.report_date = full_table.report_date 
                AND dec_activated_at.partner_uuid = full_table.partner_uuid
                AND dec_activated_at.city = full_table.city
        ),
        --
        dec_and_t_activated_at_month AS (
            SELECT
                report_date,
                partner_uuid,
                city,
                new_activated_account_day,
                sum(new_activated_account_day) OVER (PARTITION BY toStartOfMonth(report_date),partner_uuid, city ORDER BY report_date) AS new_activated_account
            FROM dec_and_t_activated_at
        ),
        --
        dec_and_t_created_at_month AS (
            SELECT
                report_date,
                partner_uuid,
                city,
                new_created_account_day,
                sum(new_created_account_day) OVER (PARTITION BY toStartOfMonth(report_date),partner_uuid, city ORDER BY report_date) AS new_created_account
            FROM dec_and_t_created_at
        ),
        --
        t_dec AS(
        SELECT
            t1.report_date  AS report_date,
            t2.city  AS city,
            t2.partner_uuid  AS partner_uuid
        FROM
            (SELECT DISTINCT report_date FROM db1.installation_point_st_partner_ch) AS t1
        CROSS JOIN
            (SELECT DISTINCT city,inst.partner_uuid AS partner_uuid
            FROM db1.installation_point_st_partner_ch AS inst
            LEFT JOIN db1.entries_installation_points_dir_partner AS eipdp ON inst.installation_point_id  = eipdp.installation_point_id ) AS t2
        )
        --
        SELECT
            t_dec.report_date AS report_date,
            t_dec.partner_uuid AS partner_uuid,
            t_dec.city AS city,
            new_activated_account_day,
            new_activated_account,
            new_created_account_day,
            new_created_account
        FROM t_dec 
        LEFT JOIN dec_and_t_activated_at_month AS a_a_m 
            ON a_a_m.report_date = t_dec.report_date 
            AND a_a_m.city = t_dec.city
            AND a_a_m.partner_uuid = t_dec.partner_uuid
        LEFT JOIN dec_and_t_created_at_month AS c_a_m
            ON c_a_m.report_date = t_dec.report_date 
            AND c_a_m.city = t_dec.city
            AND c_a_m.partner_uuid = t_dec.partner_uuid
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
    FROM db1.new_users_pd_rep_mobile_total
    WHERE new_created_account !=0
    ORDER BY report_date DESC
    limit 100
    """

ch.query_run(query_text)
```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.nuw_users_pd_rep_mobile_total DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)
```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.new_users_pd_rep_mobile_total_mv
    """

ch.query_run(query_text)
```

### drop ch

```python
query_text = """--sql
    DROP TABLE db1.new_users_pd_rep_mobile_total
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.nuw_users_pd_rep_mobile_total_mv
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
        WITH t_activated_at AS(
            SELECT
                report_date,
                partner_uuid,
                city,
                activated_at,
                created_at,
                citizen_id,
                ROW_NUMBER() OVER (
                            PARTITION BY
                                toStartOfMonth(report_date),
                                citizen_id
                            ORDER BY report_date
                ) = 1 AS is_first_mention_in_day
            FROM
                (SELECT
                    report_date,
                    partner_uuid,
                    city,
                    toDate(total_act.activated_at) AS activated_at,
                    toDateOrNull(cit_dir_m.`created_at`) AS created_at,
                    citizen_id
                FROM db1.rep_mobile_citizens_id_city_partner AS total_act
                LEFT JOIN db1.`citizens_dir_mobile_ch` AS cit_dir_m
                            ON `cit_dir_m`.`citizen_id` = total_act.`citizen_id`
                WHERE report_date BETWEEN '2025-07-01' AND '2025-07-31'
                )
            WHERE activated_at = report_date
        ),
        --
        dec_activated_at AS (SELECT
            DISTINCT
            t1.report_date  AS report_date,
            t2.city  AS city,
            t2.partner_uuid  AS partner_uuid
        FROM
            (SELECT DISTINCT report_date FROM t_activated_at) AS t1
        CROSS JOIN
            (SELECT DISTINCT city, partner_uuid FROM t_activated_at ) AS t2
        ),
        --
        dec_and_t_activated_at AS (
            SELECT
                dec_activated_at.report_date AS report_date,
                dec_activated_at.partner_uuid AS partner_uuid,
                dec_activated_at.city AS city,
                new_activated_account_day
            FROM dec_activated_at
            LEFT join (
                SELECT 
                    report_date,
                    partner_uuid,
                    city,
                    count(DISTINCT if(is_first_mention_in_day = 1,citizen_id,NULL)) AS new_activated_account_day
                FROM t_activated_at 
                GROUP BY
                    report_date,
                    partner_uuid,
                    city
                    ) AS full_table
                ON dec_activated_at.report_date = full_table.report_date 
                AND dec_activated_at.partner_uuid = full_table.partner_uuid
                AND dec_activated_at.city = full_table.city
        ),
        --
        dec_and_t_activated_at_month AS (
        SELECT
            report_date,
            partner_uuid,
            city,
            new_activated_account_day,
            sum(new_activated_account_day) OVER (PARTITION BY toStartOfMonth(report_date),partner_uuid, city ORDER BY report_date) AS new_activated_account
        FROM dec_and_t_activated_at
        )
        SELECT
            report_date,
            sum(new_activated_account)
        FROM dec_and_t_activated_at_month
        GROUP BY report_date
        ORDER BY report_date DESC
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
        WITH t_created_at AS(
            SELECT
                report_date,
                partner_uuid,
                city,
                activated_at,
                created_at,
                citizen_id,
                ROW_NUMBER() OVER (
                            PARTITION BY
                                toStartOfMonth(report_date),
                                citizen_id
                            ORDER BY report_date
                ) = 1 AS is_first_mention_in_day
            FROM
                (SELECT
                    report_date,
                    partner_uuid,
                    city,
                    total_act.activated_at AS activated_at,
                    toDateOrNull(cit_dir_m.`created_at`) AS created_at,
                    citizen_id
                FROM db1.rep_mobile_citizens_id_city_partner AS total_act
                LEFT JOIN db1.`citizens_dir_mobile_ch` AS cit_dir_m
                            ON `cit_dir_m`.`citizen_id` = total_act.`citizen_id`
                WHERE report_date BETWEEN '2025-07-01' AND '2025-07-31'
                )
            WHERE created_at = report_date
        ),
        --
        dec_created_at AS (SELECT
            DISTINCT
            t1.report_date  AS report_date,
            t2.city  AS city,
            t2.partner_uuid  AS partner_uuid
        FROM
            (SELECT DISTINCT report_date FROM t_created_at) AS t1
        CROSS JOIN
            (SELECT DISTINCT city, partner_uuid FROM t_created_at ) AS t2
        ),
        --
        dec_and_t_created_at AS (
            SELECT
                dec_created_at.report_date AS report_date,
                dec_created_at.partner_uuid AS partner_uuid,
                dec_created_at.city AS city,
                new_created_account_day
            FROM dec_created_at
            LEFT join (
                SELECT 
                    report_date,
                    partner_uuid,
                    city,
                    count(DISTINCT if(is_first_mention_in_day = 1,citizen_id,NULL)) AS new_created_account_day
                FROM t_created_at 
                GROUP BY
                    report_date,
                    partner_uuid,
                    city) AS full_table
                ON dec_created_at.report_date = full_table.report_date 
                AND dec_created_at.partner_uuid = full_table.partner_uuid
                AND dec_created_at.city = full_table.city
        ),
        --
        dec_and_t_created_at_month AS (
        SELECT
            report_date,
            partner_uuid,
            city,
            sum(new_created_account_day) OVER (PARTITION BY toStartOfMonth(report_date),partner_uuid, city ORDER BY report_date) AS new_created_account
        FROM dec_and_t_created_at
        )
        SELECT
            report_date,
            sum(new_created_account)
        FROM dec_and_t_created_at_month
        GROUP BY report_date
        ORDER BY report_date DESC
    """

ch.query_run(query_text)
```
