# MallardDV - Sample Code and Examples

This document provides code examples and data samples to help you understand and write about MallardDV.

## Basic Usage Example

This example shows how to initialize MallardDV and load customer data:

```python
from mallarddv import MallardDataVault

# Open a connection to the database
with MallardDataVault("./myproject.db", scripts_path="./models") as mdv:
    errors = []

    # Initialize database with metadata from CSV files
    errors.extend(mdv.init_mallard_db(
        meta_only=False,
        meta_transitions_path="./transitions.csv",
        meta_tables_path="./tables.csv",
        verbose=False,
    ))

    # Execute a data loading flow for customer data
    errors.extend(mdv.execute_flow(
                "customer", "source-system-a", "data/customer.csv",
                force_load=False, verbose=False
            ))

    # Check for errors
    if errors:
        print(f"Encountered {len(errors)} errors during processing")
        for sql, error in errors:
            print(f"Error: {error}")
            print(f"SQL: {sql}")
    else:
        print("Data loaded successfully into Data Vault structures")
```

## Data Structure Examples

### Sample Source Data

**customer.csv**:
```csv
id,first_name,last_name,email,created_date,referenced_by,reference_code
1,john,doe,john.doe@example.com,2025-01-15 10:30:00,,
2,jane,smith,jane.smith@example.com,2025-01-16 14:45:00,1,101
3,robert,johnson,robert.j@example.com,2025-01-17 09:15:00,1,102
4,sarah,williams,sarah.w@example.com,2025-01-18 16:20:00,2,201
```

**product.csv**:
```csv
id,name,description,price,category
P001,Laptop,High-performance laptop,1299.99,Electronics
P002,Smartphone,Latest smartphone model,899.99,Electronics
P003,Coffee Maker,Automatic coffee machine,149.99,Kitchen
P004,Running Shoes,Sports running shoes,89.99,Sportswear
```

### Metadata Definition Examples

**tables.csv**:
```csv
base_name,rel_type,column_name,column_type,column_position,mapping
customer,stg,id,INTEGER,1,c
customer,stg,first_name,VARCHAR(255),2,c
customer,stg,last_name,VARCHAR(255),3,c
customer,stg,email,VARCHAR(255),4,c
customer,stg,created_date,TIMESTAMP,5,c
customer,stg,referenced_by,INTEGER,6,c
customer,stg,reference_code,INTEGER,7,c
customer,hub,id,INTEGER,1,bk
customer_details,hsat,customer,,0,hk
customer_details,hsat,first_name,VARCHAR(255),1,c
customer_details,hsat,last_name,VARCHAR(255),2,c
customer_details,hsat,email,VARCHAR(255),3,c
customer_details,hsat,created_date,TIMESTAMP,4,c
customer__referencer,link,customer,,1,ll
customer__referencer,link,referencer,,2,ll
customer__referencer,link,reference_code,INTEGER,3,dk
customer__referencer,lsat,customer__referencer,,0,hk
product,stg,id,VARCHAR(20),1,c
product,stg,name,VARCHAR(255),2,c
product,stg,description,VARCHAR(1023),3,c
product,stg,price,DECIMAL(10,2),4,c
product,stg,category,VARCHAR(100),5,c
product,hub,id,VARCHAR(20),1,bk
product_details,hsat,product,,0,hk
product_details,hsat,name,VARCHAR(255),1,f
product_details,hsat,description,VARCHAR(1023),2,f
product_details,hsat,price,DECIMAL(10,2),3,f
product_details,hsat,category,VARCHAR(100),4,f
```

**transitions.csv**:
```csv
source_table,source_field,target_table,target_field,group_name,position,raw,transformation,transfer_type
customer,id,hub_customer,id_bk,customer,1,false,,bk
customer,first_name,hsat_customer_details,first_name,customer_details,1,false,,f
customer,last_name,hsat_customer_details,last_name,customer_details,2,false,,f
customer,email,hsat_customer_details,email,customer_details,3,false,,f
customer,created_date,hsat_customer_details,created_date,customer_details,4,false,,f
customer,customer_hk,hsat_customer_details,customer,customer_details,0,false,,sat_delta
customer,referenced_by,hub_customer,id_bk,referencer,1,false,,bk
customer,customer,link_customer__referencer,customer_hk,l_reference,1,false,,ll
customer,referencer,link_customer__referencer,referencer_hk,l_reference,2,false,,ll
customer,reference_code,link_customer__referencer,reference_code_dk,l_reference,3,false,,dk
customer,l_reference_hk,lsat_customer__referencer,customer__referencer,s_reference,1,false,,sat_delta
product,id,hub_product,id_bk,product,1,false,,bk
product,product_hk,hsat_product_details,product,product_details,0,false,,sat_delta
product,name,hsat_product_details,name,product_details,1,false,trim(#),f
product,description,hsat_product_details,description,product_details,2,false,trim(#),f
product,price,hsat_product_details,price,product_details,3,false,,f
product,category,hsat_product_details,category,product_details,4,false,,f
```

## Generated Data Vault Structure

### Hub Tables

```sql
-- Generated hub_customer table
CREATE TABLE IF NOT EXISTS raw.hub_customer (
    customer_hk CHAR(40) PRIMARY KEY,
    id_bk INTEGER,
    load_date TIMESTAMP,
    record_source VARCHAR(100),
    run_id INTEGER
);

-- Generated hub_product table
CREATE TABLE IF NOT EXISTS raw.hub_product (
    product_hk CHAR(40) PRIMARY KEY,
    id_bk VARCHAR(20),
    load_date TIMESTAMP,
    record_source VARCHAR(100),
    run_id INTEGER
);
```

### Link Tables

```sql
-- Generated link_customer__referencer table
CREATE TABLE IF NOT EXISTS raw.link_customer__referencer (
    customer__referencer_hk CHAR(40) PRIMARY KEY,
    customer_hk CHAR(40),
    referencer_hk CHAR(40),
    reference_code_dk INTEGER,
    load_date TIMESTAMP,
    record_source VARCHAR(100),
    run_id INTEGER
);
```

### Satellite Tables

```sql
-- Generated hsat_customer_details table
CREATE TABLE IF NOT EXISTS raw.hsat_customer_details (
    customer_hk CHAR(40),
    hashdiff CHAR(40),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    created_date TIMESTAMP,
    load_date TIMESTAMP,
    record_source VARCHAR(100),
    run_id INTEGER,
    effective_from TIMESTAMP,
    deleted_flag BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (customer_hk, hashdiff)
);

-- Generated hsat_product_details table
CREATE TABLE IF NOT EXISTS raw.hsat_product_details (
    product_hk CHAR(40),
    hashdiff CHAR(40),
    name VARCHAR(255),
    description VARCHAR(1023),
    price DECIMAL(10,2),
    category VARCHAR(100),
    load_date TIMESTAMP,
    record_source VARCHAR(100),
    run_id INTEGER,
    effective_from TIMESTAMP,
    deleted_flag BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (product_hk, hashdiff)
);
```

## SQL Examples for Data Queries

### Query Current Customer Details

```sql
-- Get the current view of all customer details
SELECT c.id_bk AS customer_id,
       d.first_name,
       d.last_name,
       d.email,
       d.created_date
FROM raw.hub_customer c
JOIN raw.current_hsat_customer_details d ON c.customer_hk = d.customer_hk
ORDER BY c.id_bk;
```

### Query Customer Referrals

```sql
-- Find all customer referrals
SELECT
    c1.id_bk AS customer_id,
    cd1.first_name || ' ' || cd1.last_name AS customer_name,
    c2.id_bk AS referrer_id,
    cd2.first_name || ' ' || cd2.last_name AS referrer_name,
    l.reference_code_dk AS reference_code
FROM raw.link_customer__referencer l
JOIN raw.hub_customer c1 ON l.customer_hk = c1.customer_hk
JOIN raw.hub_customer c2 ON l.referencer_hk = c2.customer_hk
JOIN raw.current_hsat_customer_details cd1 ON c1.customer_hk = cd1.customer_hk
JOIN raw.current_hsat_customer_details cd2 ON c2.customer_hk = cd2.customer_hk;
```

### Historical Data Query

```sql
-- Get historical changes for a specific customer
SELECT
    c.id_bk AS customer_id,
    s.first_name,
    s.last_name,
    s.email,
    s.effective_from,
    s.load_date,
    s.deleted_flag
FROM raw.hub_customer c
JOIN raw.hsat_customer_details s ON c.customer_hk = s.customer_hk
WHERE c.id_bk = 1
ORDER BY s.effective_from;
```

## Advanced Usage Examples

### Custom Staging View

**models/stg/customer_vw.sql**:
```sql
-- Create a custom view that joins customer with additional data
CREATE OR REPLACE VIEW stg.customer_vw AS
SELECT
    c.id,
    c.first_name,
    c.last_name,
    c.email,
    c.created_date,
    c.referenced_by,
    c.reference_code,
    a.street,
    a.city,
    a.state,
    a.postal_code
FROM
    stg.customer c
LEFT JOIN
    stg.customer_address a ON c.id = a.customer_id;
```

### Loading the View

```python
# Register the view in metadata
# This would be included in your tables.csv
# customer_vw,stg_vw,stg,,0,vwdef

# Apply the view definition from SQL file
mdv.apply_script_from_metadata(rel_type="stg_vw", verbose=True)

# Execute flow using the view instead of the table
mdv.execute_flow("customer_vw", "source-system-a", force_load=False, verbose=True)
```

### Data Transformation Example

```python
# Custom transformation function
def transform_customer_data(df):
    """Apply transformations to customer data."""
    # Normalize names
    df['first_name'] = df['first_name'].str.title()
    df['last_name'] = df['last_name'].str.title()

    # Format email addresses
    df['email'] = df['email'].str.lower()

    # Create derived columns
    df['full_name'] = df['first_name'] + ' ' + df['last_name']

    return df

# In your application code
import pandas as pd

# Load data
customers = pd.read_csv('data/customer.csv')

# Apply transformations
customers = transform_customer_data(customers)

# Save transformed data
customers.to_csv('data/customer_transformed.csv', index=False)

# Load with MallardDV
mdv.execute_flow("customer", "source-system-a", "data/customer_transformed.csv")
```

## Use Case Scenarios

### Data Integration Use Case

```python
# Example: Integrating data from multiple sources into a unified Data Vault
sources = [
    {"table": "customer", "source": "crm-system", "file": "data/crm_customers.csv"},
    {"table": "customer", "source": "e-commerce", "file": "data/ecommerce_customers.csv"},
    {"table": "product", "source": "inventory", "file": "data/inventory_products.csv"},
    {"table": "product", "source": "e-commerce", "file": "data/ecommerce_products.csv"},
]

with MallardDataVault("./integrated_dv.db", scripts_path="./models") as mdv:
    # Initialize the database
    mdv.init_mallard_db(
        meta_only=False,
        meta_transitions_path="./transitions.csv",
        meta_tables_path="./tables.csv",
    )

    # Process each data source
    for source in sources:
        mdv.execute_flow(
            source_table=source["table"],
            record_source=source["source"],
            file_path=source["file"],
            verbose=True
        )

    print("Data integration complete!")
```

### Historical Tracking Use Case

```python
# Example: Tracking customer data changes over time
import datetime

# Initial load - Day 1
with MallardDataVault("./historical_dv.db", scripts_path="./models") as mdv:
    mdv.init_mallard_db(
        meta_only=False,
        meta_transitions_path="./transitions.csv",
        meta_tables_path="./tables.csv",
    )

    # Load initial data
    day1 = datetime.datetime(2025, 1, 1).strftime("%Y-%m-%d %H:%M:%S")
    mdv.execute_flow(
        "customer",
        "daily-feed",
        "data/customers_day1.csv",
        load_date_overwrite=day1
    )

# Day 2 - Some customers changed
with MallardDataVault("./historical_dv.db", scripts_path="./models") as mdv:
    day2 = datetime.datetime(2025, 1, 2).strftime("%Y-%m-%d %H:%M:%S")
    mdv.execute_flow(
        "customer",
        "daily-feed",
        "data/customers_day2.csv",
        load_date_overwrite=day2
    )

# Day 3 - More changes
with MallardDataVault("./historical_dv.db", scripts_path="./models") as mdv:
    day3 = datetime.datetime(2025, 1, 3).strftime("%Y-%m-%d %H:%M:%S")
    mdv.execute_flow(
        "customer",
        "daily-feed",
        "data/customers_day3.csv",
        load_date_overwrite=day3
    )

# Query point-in-time data
with MallardDataVault("./historical_dv.db") as mdv:
    point_in_time = datetime.datetime(2025, 1, 2, 12, 0, 0).strftime("%Y-%m-%d %H:%M:%S")
    result = mdv.sql(f"""
        SELECT
            hub.id_bk,
            sat.first_name,
            sat.last_name,
            sat.email
        FROM
            raw.hub_customer hub
        JOIN
            raw.hsat_customer_details sat ON hub.customer_hk = sat.customer_hk
        WHERE
            sat.effective_from <= '{point_in_time}'
            AND (sat.effective_to > '{point_in_time}' OR sat.effective_to IS NULL)
    """)

    print(f"Customer data as of {point_in_time}:")
    for row in result.fetchall():
        print(row)
```

These examples should provide a solid foundation for writing a comprehensive article about MallardDV's capabilities and use cases.
