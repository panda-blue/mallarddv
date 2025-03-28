# MallardDV - A Portable Data Vault Implementation Using DuckDB

This document contains comprehensive information and examples to help you write an article about MallardDV.

## Introduction to MallardDV

MallardDV is a lightweight Python implementation of the Data Vault 2.0 methodology using DuckDB as the underlying database engine. It's designed for rapid prototyping and mockups of Data Vault type data warehouses without requiring a full database server.

### Key Features

- Create Data Vault structures (hubs ⚙️, links 🔗, satellites 🛰️) from metadata
- Load data from staging tables into Data Vault structures
- Automatic hash key generation
- Support for delta and full loads
- Portable, file-based approach using DuckDB

## Core Concepts of Data Vault 2.0

Data Vault 2.0 is a methodology for designing enterprise data warehouses that focuses on:

1. **Hubs** - Core business entities identified by business keys
2. **Links** - Relationships between hubs
3. **Satellites** - Descriptive attributes that belong to hubs or links

MallardDV implements these concepts in a lightweight, portable way using DuckDB.

## System Architecture

MallardDV follows a modular architecture with specialized components:

1. **Core Class** (`MallardDataVault`) - Initializes and coordinates all components
2. **Database Layer** - Manages connections and SQL execution
3. **Metadata Layer** - Defines and manages the structure through metadata tables
4. **Data Vault Components** - Implement hub, link, and satellite functionality
5. **ETL Components** - Handle data loading workflows

### Component Breakdown

- `DatabaseConnection`: Manages connections to DuckDB
- `MetadataManager`: Handles metadata tables that define the Data Vault structure
- `SchemaManager`: Creates and manages database schemas
- `HashGenerator`: Generates hash keys for Data Vault entities
- `DVHubManager`: Creates and loads hub tables
- `DVLinkManager`: Creates and loads link tables
- `DVSatelliteManager`: Creates and loads satellite tables
- `ETLService`: Manages data extraction, transformation, and loading
- `FlowExecutor`: Orchestrates the entire data loading process

## Metadata-Driven Approach

MallardDV uses a metadata-driven approach, with two main metadata tables:

1. **`metadata.tables`**: Defines Data Vault table structures
2. **`metadata.transitions`**: Maps staging tables to Data Vault structures

### metadata.tables Structure

This table defines the Data Vault structure with these key fields:
- `base_name`: Base table name
- `rel_type`: Relationship type (stg, hub, link, hsat, lsat, etc.)
- `column_name`: The base name of the column
- `column_type`: Data type of the column
- `column_position`: Order of fields in DDL and hash computation
- `mapping`: Field type (bk for business key, hk for hash key, etc.)

Example:

```
| base_name | rel_type | column_name | column_type  | column_position | mapping |
|-----------|----------|-------------|--------------|-----------------|---------|
| customer  | stg      | id          | INTEGER      | 1               | c       |
| customer  | stg      | first_name  | VARCHAR(255) | 2               | c       |
| customer  | hub      | id          | INTEGER      | 1               | bk      |
```

### metadata.transitions Structure

This table defines how data flows from staging to Data Vault with these key fields:
- `source_table`: Staging table or view used as the source
- `source_field`: Field in the source table
- `target_table`: Target table in the Data Vault layer
- `target_field`: Target column in the Data Vault layer
- `group_name`: Groups records into the same transition
- `transfer_type`: Type of transition (bk, sat_delta, sat_full, etc.)

Example:

```
| source_table | source_field | target_table      | target_field | group_name | transfer_type |
|--------------|--------------|-------------------|--------------|------------|---------------|
| customer     | id           | hub_customer      | id_bk        | customer   | bk            |
| customer     | first_name   | hsat_customer_det | first_name   | customer_det | f           |
```

## Workflow Example

Here's a complete workflow example using MallardDV:

### Step 1: Initialize the Database

```python
from mallarddv import MallardDataVault

with MallardDataVault("./demo/demo.db", scripts_path="./demo/models") as mdv:
    # Initialize database with metadata
    mdv.init_mallard_db(
        meta_only=False,
        meta_transitions_path="./demo/transitions.csv",
        meta_tables_path="./demo/tables.csv",
        verbose=True,
    )
```

This initializes the database with metadata definitions and creates all the necessary structures.

### Step 2: Load Data and Execute Flow

```python
# Continue from previous example
with MallardDataVault("./demo/demo.db", scripts_path="./demo/models") as mdv:
    # Execute the customer data flow
    mdv.execute_flow(
        "customer",
        "demo-customer",
        "demo/data/customer.csv",
        force_load=False,
        verbose=True
    )
```

The `execute_flow` method performs the following steps:
1. Loads data from CSV into staging tables
2. Computes hash keys
3. Loads data from staging into hubs
4. Loads data from staging into links
5. Loads data from staging into satellites
6. Tracks metadata about the load process

### Example Data

**Sample data (customer.csv):**
```
id,first_name,last_name,email,created_date,referenced_by,reference_code
1,jhon,doe,jhon.doe@example.com,2025-03-25 15:16:33,,
2,jane,smith,jane.smith@example.com,2025-03-25 15:17:24,1,352
```

**Result in Data Vault:**

Hubs:
- `hub_customer`: Stores unique customer entities with business key (id)

Links:
- `link_customer__referencer`: Stores relationships between customers (who referenced whom)

Satellites:
- `hsat_customer_details`: Stores descriptive attributes of customers (first_name, last_name, email)

## Technical Implementation Details

### Hash Key Generation

MallardDV automatically generates hash keys for entities based on business keys:

```python
# Example from HashViewGenerator
hash_field = f"md5(lower(trim(coalesce(cast({field} as varchar), ''))))::char(32)"
```

### Current Value Views

The system automatically creates "current" views that show the most recent satellite values:

```sql
CREATE OR REPLACE VIEW {schema}.current_{table_type}_{table_base_name} AS
SELECT
    src.{hub_key} AS {hub_key}
    {comma}
    {fields}
FROM {schema}.{table_type}_{table_base_name} src
```

### Delta vs. Full Loads

MallardDV supports both delta and full loading patterns:

- **Delta**: Only loads changes from the source system
- **Full**: Completely refreshes satellite data, including marking deleted records

## Advanced Use Cases

### Multi-entity Loading

MallardDV can handle complex scenarios with multiple entities:

```python
# Load customer and order data in sequence
mdv.execute_flow("customer", "demo-customer", "data/customer.csv")
mdv.execute_flow("order", "demo-order", "data/order.csv")
```

### Custom SQL Views

You can extend the system with custom SQL views:

```python
# Apply custom views
mdv.apply_script_from_metadata(rel_type="stg_vw", verbose=True)
```

### Performance Considerations

For larger datasets, consider:
- Using the DuckDB COPY command for fast loading
- Creating appropriate indexes on hash keys
- Using parallel processing for multiple flows

## Why Use MallardDV?

### Benefits

1. **Portability**: File-based database using DuckDB
2. **Simplicity**: Lightweight implementation of Data Vault concepts
3. **Rapid Prototyping**: Quick to set up and experiment with
4. **Metadata-Driven**: Easy to modify without changing code
5. **Python Integration**: Seamlessly integrates with data science workflows

### Limitations

1. Not suitable for production environments with high security requirements
2. Uses SQL templating that's not safe against SQL injection
3. Limited scalability compared to enterprise data warehouse solutions

## Conclusion

MallardDV offers a lightweight, portable implementation of Data Vault 2.0 methodologies. It's perfect for:
- Rapid prototyping of data warehouse concepts
- Learning Data Vault principles
- Creating quick mockups before implementing in enterprise systems
- Data scientists who need a portable data warehouse

The metadata-driven approach makes it flexible and easy to modify, while the DuckDB integration ensures portability and ease of setup.
