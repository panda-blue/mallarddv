# 🦆 MallardDV - A Portable Data Vault

A lightweight Python implementation of a Data Vault data warehouse pattern using DuckDB.

## 🧐 Overview

This library provides a portable implementation of the Data Vault 2.0 methodology for data warehousing.
It uses DuckDB as the underlying database engine, making it very portable and easy to set up without requiring a full database server.

Key features:
- Create Data Vault structures (hubs ⚙️, links 🔗, satellites 🛰️) from metadata
- Load data from staging tables into Data Vault structures
- Automatic hash key generation
- Support for delta and full loads

## ⚠️ Disclaimer ⚠️

This library should only be used for rapid mockup and prototyping of a DataVault type DWH. It uses extensive SQL templating that are not safe against SQL injection.

## 🤖 Installation

It is recommanded to create and activate a virtual environement

on linux:

```bash
python3 -m venv .env
source .env/bin/activate
```

on windows:

```bash
python -m venv .env
source .env/Scripts/activate
```

install the requirements:

```bash
pip install -r requirements.txt
```

to run the tests, or develop modifications:

```bash
pip install -r dev-requirements.txt
pre-commit install
```

## 👉 Usage

```python
from mallarddv import MallardDataVault

# Open a connection to the database
with MallardDataVault("./demo/demo.db", scripts_path="./demo/models") as mdv:
    errors = []

    # initialise database with information from transitions and tables csv.
    errors.extend(mdv.init_mallard_db(
        meta_only=False,
        meta_transitions_path="./demo/transitions.csv",
        meta_tables_path="./demo/tables.csv",
        verbose=False,
    ))

    # execute the customer demo flow
    errors.extend(mdv.execute_flow(
                "customer", f"demo-customer", f"demo/data/customer.csv",force_load=False, verbose=False
            ))


```

## 💪 Metadata Structure

The library uses two main metadata tables:

1. `metadata.tables`: Defines Data Vault table structures
2. `metadata.transitions`: Maps staging tables to Data Vault structures

### 👤 metadata.tables

fields:
- base_name: base table name
- rel_type: one of
  - stg: staging table
  - stg_vw: staging view reference
  - hub: hub
  - link: link
  - nhl: non historized link
  - hsat: sat of hub
  - lsat: sat of link
- column_name: the base name of the column
- column_type: type of the column when applicable (not used for stg_vw, h/lsat.hk, link/nhl.ll)
- column_position: used to order field in ddl and for hash computation
- mapping: the type of field
  - stg -> c: staging column
  - hub -> bk: hub business key
  - (h/l)sat -> hk: sat hash key (only one can be defined per satellite)
  - (h/l)sat -> f: sat field
  - (link/nhl) -> ll: link leg (column storing reference to a hub hash key)
  - (link/nhl) -> dk: degenerated key

example:

| base_name            | rel_type | column_name          | column_type   | column_position | mapping |
| -------------------- | -------- | -------------------- | ------------- | --------------- | ------- |
| customer             | stg      | id                   | INTEGER       | 1               | c       |
| customer             | stg      | first_name           | VARCHAR(255)  | 2               | c       |
| customer             | stg      | last_name            | VARCHAR(255)  | 3               | c       |
| customer             | stg      | email                | VARCHAR(255)  | 4               | c       |
| customer             | stg      | created_date         | TIMESTAMP     | 5               | c       |
| customer             | stg      | referenced_by        | INTEGER       | 6               | c       |
| customer             | stg      | reference_code       | INTEGER       | 7               | c       |
| customer             | hub      | id                   | INTEGER       | 1               | bk      |
| customer_details     | hsat     | customer             |               | 0               | hk      |
| customer_details     | hsat     | first_name           | VARCHAR(255)  | 1               | c       |
| customer_details     | hsat     | last_name            | VARCHAR(255)  | 2               | c       |
| customer_details     | hsat     | email                | VARCHAR(255)  | 3               | c       |
| customer_details     | hsat     | created_date         | TIMESTAMP     | 4               | c       |
| customer_vw          | stg_vw   | stg                  |               | 0               | vwdef   |
| customer__referencer | link     | customer             |               | 1               | ll      |
| customer__referencer | link     | referencer           |               | 2               | ll      |
| customer__referencer | link     | reference_code       | INTEGER       | 3               | dk      |
| customer__referencer | lsat     | customer__referencer |               | 0               | hk      |
| product              | stg      | id                   | INTEGER       | 1               | c       |
| product              | stg      | name                 | VARCHAR(255)  | 2               | c       |
| product              | stg      | description          | VARCHAR(1023) | 3               | c       |
| product              | hub      | id                   | INTEGER       | 1               | bk      |
| product              | hub      | product_type         | VARCHAR(255)  | 2               | bk      |
| product_details      | hsat     | product              |               | 0               | hk      |
| product_details      | hsat     | name                 | VARCHAR(255)  | 1               | f       |
| product_details      | hsat     | description          | VARCHAR(1023) | 2               | f       |

### 👣 metadata.transitions

fields:
- source_table: staging table or view used as the source of the transition
- source_field:
  - for hub->bk, (link/nhl)->dk (h/l)sat->f: field of the source
  - for (link/nhl).ll: group_name of the corresponding hub transition
  - for (h/l)sat->sat_(delta/full): hash field of the corresponding hub/link (group_name + _hk)
- target table: fully name of the target table in the dv layer (without schema)
- target_field: target column of the target table
- group_name: reference used to group records to the same transition, it is also the base used to calculate hashes in the staging hash_vw view
- raw: true if the source field is a text and not a column name (only used for hub business keys)
- transformation: field transformation, # are replaced with the source field
- transfer_type: type of transition
  - hub->bk: field is used as a hub business key
  - (h/l)sat->sat_delta: field is used to define the hash key reference of the hub or link the sat depends on (unique for a given satellite), it also denotes the transfer type: full or delta mode
  - (h/l)sat->f: used to fill sat fields and compute sat hashdiff
  - (h/l)sat->ll: used to fill link/nhl leg with the hash key of the corresponding hub (uses the hub group name)
  - (h/l)sat->dk: used to fill degenerate keys

example:
| source_table | source_field   | target_table              | target_field         | group_name       | position | raw   | transformation | transfer_type |
| ------------ | -------------- | ------------------------- | -------------------- | ---------------- | -------- | ----- | -------------- | ------------- |
| customer     | id             | hub_customer              | id_bk                | customer         | 1        | false |                | bk            |
| customer     | first_name     | hsat_customer_details     | first_name           | customer_details | 1        | false |                | f             |
| customer     | last_name      | hsat_customer_details     | last_name            | customer_details | 2        | false |                | f             |
| customer     | email          | hsat_customer_details     | email                | customer_details | 3        | false |                | f             |
| customer     | customer_hk    | hsat_customer_details     | customer             | customer_details | 0        | false |                | sat_delta     |
| customer     | referenced_by  | hub_customer              | id_bk                | referencer       | 1        | false |                | bk            |
| customer     | customer       | link_customer__referencer | customer_hk          | l_reference      | 1        | false |                | ll            |
| customer     | referencer     | link_customer__referencer | referencer_hk        | l_reference      | 2        | false |                | ll            |
| customer     | reference_code | link_customer__referencer | reference_code_dk    | l_reference      | 3        | false |                | dk            |
| customer     | l_reference_hk | lsat_customer__referencer | customer__referencer | s_reference      | 1        | false |                | sat_delta     |
| product      | id             | hub_product               | id_cbk               | product          | 1        | false |                | bk            |
| product      | base_product   | hub_product               | product_type_cbk     | product          | 2        | true  |                | bk            |
| product      | product_hk     | hsat_product_details      | product              | product_details  | 0        | false |                | sat_full      |
| product      | name           | hsat_product_details      | name                 | product_details  | 1        | false | trim(#)        | f             |
| product      | description    | hsat_product_details      | description          | product_details  | 2        | false | trim(#)        | f             |

## 🪲 Testing

![coverage badge](/reports/coverage/coverage-badge.svg)

Run the tests using coverage and pytest:

```bash
coverage run -m pytest -xvs
coverage html
```
