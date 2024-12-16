# KTL Autovault Package

This dbt package provides macros to automate the creation of Raw Vault models following the Data Vault 2.0 architecture. It includes utilities for generating Hub, Link, Satellite, and Link Satellite models through configuration-driven development.

## Installation

Add the following to your `packages.yml`:

```yaml
# packages.yml

packages:
  - git: "http://reader:7PmXk-DsZo8bX_Y6zKZH@14.241.249.100:7979/de-team/ktl_autovault.git"
    revision: main
```

## Configuration

### System Variables

All Raw Vault models require specific system columns. Configure these in your `dbt_project.yml`:

```yaml
# dbt_project.yml
...
vars:
  dv_system:
    columns:
      - target: dv_src_ldt
        dtype: timestamp
        description: 'Source system load datetime'
        source:
          name: load_datetime
          dtype: timestamp

      - target: dv_src_rec
        dtype: string
        description: 'Source record system name'
        source:
          name: record_source
          dtype: string
          
      - target: dv_cdc_ops
        dtype: string
        description: 'CDC operation type (R=read, I=insert, U=update, D=delete)'
        source:
          name: cdc_operation
          dtype: string

      - target: dv_kaf_ldt
        dtype: timestamp
        description: 'Kafka load datetime'
        source:
          name: current_timestamp()
          dtype: timestamp

      - target: dv_kaf_ofs
        dtype: bigint
        description: 'Kafka offset'
        source:
          name: "1"
          dtype: bigint

      - target: dv_ldt
        dtype: timestamp
        description: 'Raw Vault load datetime'
        source:
          name: current_timestamp()
          dtype: timestamp
```

System columns must be present in all source tables with consistent naming

### Raw Vault - Hub Configuration

Hubs represent business entities and require configuration of business keys and their corresponding hash keys.

Example configurations as macros:

```yaml
-- macros/hub_customer_dv_config.sql

{%- macro hub_customer_dv_config() -%}
{%- set model_yml -%}

target_entity_type: hub
target_schema: duytk_test
target_table: hub_customer
sources:
  - collision_code: loan
    source_schema: duytk_test
    source_table: loan_info
    columns:
      - target: dv_hkey_hub_customer
        key_type: hash_key_hub
        source:
          - cst_no
      - target: cst_no
        dtype: double
        key_type: biz_key
        source:
          name: cst_no
          dtype: string

{%- endset -%}
{%- set model = fromyaml(model_yml) -%}
{{ return(model) }}
{%- endmacro -%}
```

Key components:
- `target_entity_type`: Must be 'hub'
- `target_schema`: Target schema name
- `target_table`: Target table name
- `sources`: List of source tables
  - `collision_code`: Unique identifier for the source
  - `source_schema`: Source schema name
  - `source_table`: Source table name
  - `columns`: Column mappings
    - `key_type`: Either 'hash_key_hub' or 'biz_key'
    - `source`: Source column configuration

To generate a Hub model:

```sql
-- models/hub_customer.sql

{%- set model = dv_config('hub_customer') -%}
{%- set dv_system = var("dv_system") -%}
{{ ktl_autovault.hub_transform(model=model, dv_system=dv_system) }}
```

Remember to define your sources in `sources.yml`:

```yaml
# models/sources.yml

sources:
  - name: duytk_test
    schema: duytk_test
    tables:
      - name: loan_info
```

Notes

1. Hub models support multiple sources with consistent target column names

    ```yaml
    # hub configs
    ...
    sources:
      - collision_code: loan
        source_schema: duytk_test
        source_table: loan_info
        columns:
          - target: dv_hkey_hub_customer
            key_type: hash_key_hub
            source:
              - cst_no
          - target: cst_no
            dtype: double
            key_type: biz_key
            source:
              name: cst_no
              dtype: string

      - collision_code: loan_1
        source_schema: duytk_test
        source_table: loan_info_1
        columns:
          - target: dv_hkey_hub_customer
            key_type: hash_key_hub
            source:
              - cst_no
          - target: cst_no
            dtype: double
            key_type: biz_key
            source:
              name: cst_no
              dtype: string
    ```

2. Required column types:
   - `hash_key_hub`: Hash key for the hub
   - `biz_key`: Business key columns

3. Set `from_psa_model=true` when using ref models instead of sources.yml

    ```sql
    {{ ktl_autovault.hub_transform(model=model, dv_system=dv_system, from_psa_model=true) }}
    ```

### Raw Vault - Link and Satellite Configuration

[Placeholder for Link and Satellite configuration documentation - to be added]

### Columns Configuration

The columns configuration defines how source columns are mapped to target columns in Data Vault models. Each column definition specifies the transformation rules, data types, and key types for Data Vault structures.

Basic Structure

```yaml
columns:
  - target: <target_column_name>
    key_type: <key_type>
    dtype: <data_type>
    source: <source_configuration>
```

Column Types

1. Hash Key Hub

    Used for generating unique identifiers in Hub models.

    ```yaml
    - target: dv_hkey_hub_customer    # Target column name in the vault
      key_type: hash_key_hub          # Specifies this is a hub hash key
      source:                         # List of source columns to hash
        - cst_no                      # Source column(s) to generate hash from
    ```

    Key points:
    - `target`: Names the resulting hash key column
    - `key_type`: Must be `hash_key_hub` for hub hash keys
    - `source`: List format for multiple columns to be included in hash generation
    - No `dtype` needed as hash keys are automatically handled

2. Business Key
    Represents the natural business keys from source systems.

    ```yaml
    - target: cst_no                  # Target column name in the vault
      dtype: double                   # Target data type
      key_type: biz_key               # Specifies this is a business key
      source:                         # Source column configuration
        name: cst_no                  # Source column name
        dtype: string                 # Source data type
    ```

    Key points:
    - `target`: The column name in your vault model
    - `dtype`: The desired data type in the target
    - `key_type`: Must be `biz_key` for business keys
    - `source`: Object format with:
      - `name`: Source column name
      - `dtype`: Source data type (for type casting if needed)

## Contributing

[Your contribution guidelines here]

## License

[Your license information here]