---
jupyter:
  jupytext:
    cell_metadata_filter: -all
    formats: ipynb,md
    main_language: python
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.17.1
---


```python
query_text = """--sql
    CREATE TABLE db1.uk_addresses_partner
    (
        `patrner_uuid_uk` String,
        `address_uuid`  String,
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/uk_addresses_partner/uk_addresses_partner.csv','CSVWithNames')
    """

ch.query_run(query_text)

```

```python
query_text = """--sql
    CREATE TABLE db1.uk_addresses_partner_ch
    (
        `patrner_uuid_uk` String,
        `address_uuid`  String,
    )
    ENGINE = MergeTree()
    ORDER BY patrner_uuid_uk
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.uk_addresses_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.uk_addresses_partner_ch AS
    SELECT
        *
    FROM db1.uk_addresses_partner
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        *
    FROM db1.uk_addresses_partner_ch
    LIMIT 2
    """

ch.query_run(query_text)
```
