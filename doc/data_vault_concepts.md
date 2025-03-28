# Data Vault 2.0 Concepts

This document explains the core Data Vault 2.0 concepts that MallardDV implements, which will be helpful for your article.

## What is Data Vault?

Data Vault is a database modeling methodology designed for enterprise data warehousing. It was developed by Dan Linstedt and has evolved into Data Vault 2.0, which incorporates best practices for handling big data, NoSQL, and other modern data challenges.

## Core Components of Data Vault

### 1. Hubs

Hubs represent business entities and contain:

- A surrogate key (usually a hash)
- Business keys that uniquely identify the entity
- Metadata (load date, record source, etc.)

Hubs never store descriptive attributes - only keys that identify core business objects.

**Example Hub (hub_customer)**:
```
customer_hk | id_bk | load_date           | record_source | run_id
------------|-------|---------------------|---------------|-------
abc123...   | 1     | 2025-03-25 15:16:33 | CRM-system    | 1001
def456...   | 2     | 2025-03-25 15:17:24 | CRM-system    | 1001
```

### 2. Links

Links represent relationships between business entities (hubs) and contain:

- A surrogate key (usually a hash)
- Foreign keys to the related hubs
- Optional degenerate keys (transaction IDs, etc.)
- Metadata (load date, record source, etc.)

Links capture the relationships between business concepts.

**Example Link (link_customer__referencer)**:
```
link_hk     | customer_hk | referencer_hk | reference_code | load_date          | record_source | run_id
------------|-------------|---------------|----------------|-------------------|---------------|-------
ghi789...   | def456...   | abc123...     | 352            | 2025-03-25 15:17:24 | CRM-system | 1001
```

### 3. Satellites

Satellites contain descriptive attributes (context) for hubs or links:

- They reference a hub or link via its hash key
- They contain descriptive attributes
- They include a hashdiff that tracks changes
- They include metadata (effective dates, etc.)

Satellites capture the historical state of business entities.

**Example Satellite (hsat_customer_details)**:
```
customer_hk | hashdiff   | first_name | last_name | email               | created_date        | load_date           | effective_from      | deleted_flag
------------|------------|------------|-----------|---------------------|---------------------|---------------------|---------------------|-------------
abc123...   | jkl012...  | John       | Doe       | john.doe@example.com| 2025-03-25 15:16:33 | 2025-03-25 15:20:00 | 2025-03-25 15:16:33 | false
```

## Key Data Vault 2.0 Principles

### 1. Separation of Concerns

Data Vault separates:
- **Business Keys** (in Hubs)
- **Relationships** (in Links)
- **Context/Descriptive Data** (in Satellites)

This separation makes the model more adaptable to change and ensures better historical tracking.

### 2. Immutability and Auditability

- Records are never physically updated or deleted
- Changes create new records in satellites
- Original data is preserved for auditability

### 3. Parallel Loading

- Hubs, links, and satellites can be loaded in parallel
- Different parts of the model can be loaded independently

### 4. Scalability

- Data Vault structures scale horizontally
- New business entities or attributes can be added without altering existing structures

## Data Vault Loading Patterns

### 1. Initial Load

- Creates hub records for new business keys
- Creates initial satellite records for descriptive data

### 2. Delta Load

- Adds only new hub records (if business keys don't exist)
- Adds new satellite records only when attribute values change
- Common for incremental loading

### 3. Full Load

- Refreshes a complete satellite
- Marks records as deleted if they no longer exist in the source
- Useful for completely replacing reference data

## Hash Keys in Data Vault

Hash keys are central to Data Vault:

1. **Business Key Hash (Hub Hash Key)**
   - Created from business keys
   - Uniquely identifies a business entity

2. **Relationship Hash (Link Hash Key)**
   - Created from the combination of related hub hash keys
   - Uniquely identifies a relationship

3. **Hashdiff**
   - Created from all descriptive attributes in a satellite
   - Used to detect changes

### Hash Key Benefits

- Enables parallel loading
- Provides natural distribution for big data platforms
- Eliminates the need for surrogate key lookups
- Allows for deterministic key generation

## Current vs. Historical Data

Data Vault manages historical data through:

1. **Point-in-Time (PIT) Structures**
   - Represent the state of entities at specific times

2. **Current Views**
   - Show the latest state of entities
   - Created by filtering to the most recent satellite records

## How MallardDV Implements Data Vault 2.0

MallardDV follows Data Vault 2.0 principles with some implementation-specific features:

1. **Metadata-Driven Approach**
   - Tables and relationships defined in metadata
   - Simplifies changes to the model

2. **Automated Hash Key Generation**
   - MD5 hash keys generated automatically
   - Consistent handling across the model

3. **Automated DDL Generation**
   - SQL for creating hubs, links, and satellites generated from metadata

4. **ETL Flow Orchestration**
   - Complete process from staging to Data Vault structures
   - Support for both delta and full loads

5. **Current Views**
   - Automatically generated views for the latest satellite data

## Differences from Traditional Data Warehousing

### Data Vault vs. Star Schema

| Feature | Data Vault | Star Schema |
|---------|------------|-------------|
| Focus | Integration and historization | Analysis and reporting |
| Normalization | Highly normalized | Denormalized |
| Changes | Adaptable to source changes | Requires schema changes |
| History | Preserves all history | Often limited history |
| Load patterns | Parallel, incremental | Often full dimension loads |
| Query complexity | More complex for end users | Optimized for end users |

### Data Vault vs. Third Normal Form (3NF)

| Feature | Data Vault | 3NF |
|---------|------------|-----|
| Primary focus | Business keys and relationships | Functional dependencies |
| History | Built-in historical tracking | No built-in history |
| Performance | Optimized for loading speed | Optimized for transactional use |
| Adaptability | Easily add new sources | More rigid structure |

## Why Use Data Vault?

1. **Agility**: Adapts easily to changing business requirements
2. **Auditability**: Preserves data lineage and history
3. **Scalability**: Handles growth in data volume and complexity
4. **Integration**: Designed for integrating multiple source systems
5. **Historical Tracking**: Built-in mechanisms for historical data

## Data Vault Challenges

1. **Complexity**: More complex than star schemas for querying
2. **Performance**: May require additional presentation layer for analytics
3. **Learning Curve**: Different approach from traditional modeling
4. **Storage**: Uses more storage due to historical preservation

## DuckDB and Data Vault

DuckDB provides several advantages for a lightweight Data Vault implementation:

1. **Portability**: File-based, embedded database
2. **Performance**: Columnar storage, optimized for analytical queries
3. **SQL Support**: Full SQL support including complex queries
4. **Integration**: Easy integration with Python
5. **Low Overhead**: No server setup required

This combination makes MallardDV ideal for rapid prototyping and learning Data Vault concepts.
