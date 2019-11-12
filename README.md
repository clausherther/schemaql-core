# SchemaQL

A testing and auditing tool inspired by dbt, for those not using [dbt](https://www.getdbt.com).

## Installation
1. Fork and clone to repo to a local folder.
2. Create a new Python virtual environment in this folder and activate it.
3. In the local repo folder, run `pip install -r  requirements` to install the dependent packages in this virtualenv.
4. In the local repo folder, run `pip install -e .` to install a dev version locally.

You should now be able to run `schemaql -h` from the command line.

```bash
schemaql -h
usage: schemaql [-h] [-p None] [-c config.yml] [-x connections.yml] [action]

positional arguments:
  action                Action ('test', or 'generate')

optional arguments:
  -h, --help            show this help message and exit
  -p None, --project None
                        Project
  -c config.yml, --config-file config.yml
                        Config file
  -x connections.yml, --connections-file connections.yml
                        Connections file
```

## Configuration

SchemaQL needs to `*.yml` for configuration:

In `connections.yml` you define how to connect to one or more of your data warehouse connections like so:

```yaml
project-1-snowflake:
type: snowflake
  account: <my_account>
  user: <my_user>
  password: <my_password>
  database: <my_database>
  # optional:
  warehouse: <my_warehouse>
  role: <my_role>
  schema: <my_initial_schema>

project-2-bigquery:
type: bigquery
  database: <my_bigquery_project>
  credentials_path: <path_to_my_service_account_credentials.json>
  supports_multi_insert: False
```

In `config.yml`, you define the following:

- Logging, where `output` is a directory relative to the project path. 
```yaml
logs:
  output: logs
```
- a `collector` which is a definition for how you want test results to be collected. Currently supported are:
  - `json`
  - `csv`
  - `database`

For `json` and `csv`, you only need to provide an `output` path.

```yaml
collector:
  type: csv
  output: output 
```

For `database` collection, you need to provide the name of a connection (from `connections.yml`) and a destination table via `output`:

```yaml
collector:
  type: database
  connection: project-1-snowflake
  output: test_results
```
The `collector` connection does not need to the same connection, or even the same connection type as the project connection. So, tests could be run against BigQuery but tests results could be collected using Snowflake or Postgres.

- `projects` which is a combination of a connection and a list of which databases and schemas you want to work with. If you don't define any schemas within the database key, all schemas will be processed.

```yaml
projects:
  project-1:
    connection: project-1-snowflake
    schema:
      database_1:
      - schema_1
      - schema_2
  project-2:
    connection: project-2-bigquery
    schema:
      my_bg_project_1
      my_bg_project_2
      - data_set_1
      - data_set_2
```

## Usage

### Generate

SchemaQL runs tests against schema information contained in `yml` files. You can either write these from scratch, use your existing `dbt` schema files, or use `schemaql` to generate them. 

Generates schema files for all projects:
```bash
schemaql generate
```

Generates schema files for `"my_project"` only:
```bash
schemaql generate -p my_project
```

### Test

```bash
schemaql test
```

Test `"my_project"` only:
```bash
schemaql test -p my_project
```

## Built-in Tests

### Schema Tests
#### not_null
Checks if column values are `NULL`
```yaml
models:
  - name: my_table
    columns:
      - name: col_1
        tests:
          - not_null
```
#### relationships
Checks if column values match values from column in other entity
```yaml
models:
  - name: my_table
    columns:
      - name: col_1
        tests:
          - relationships:
              to: my_other_table
              field: col_1
```
#### unique
Checks if column values are unique
```yaml
models:
  - name: my_table
    columns:
      - name: col_1
        tests:
          - unique
```

### Data Tests
#### accepted_values
Checks if column values match a predefined list of accepted values
```yaml
models:
  - name: my_table
    columns:
      - name: day_of_week
        description: 
        tests:
          - accepted_values:
            values: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
```

#### at_least_one
Checks if column has at least one value
```yaml
models:
  - name: my_table
    columns:
      - name: col_1
        tests:
          - at_least_one
```
#### equal_expression (TBD)
#### frequency (TBD)
#### not_constant
Checks if column has at more than one value
```yaml
models:
  - name: my_table
    columns:
      - name: col_1
        tests:
          - not_constant
```
#### recency (TBD)
#### unique_rows
Checks if table rows are unique. 
If `columns` are not specified, uses all columns.

```yaml
models:
  - name: my_table
    tests:
      - unique_rows:
          columns: [col_1, col_2, col_3]

```
