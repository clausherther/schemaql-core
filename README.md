# SchemaQL

A testing and auditing tool inspired by dbt, for those not using [dbt](https://www.getdbt.com).

## Installation
1. Fork and clone to repo to a local folder.
2. In the local repo folder, run `pip install -e .` to install a dev version locally.

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

## Usage

### Configuration

SchemaQL needs to `*.yml` for configuration:

In `connections.yml` you define how to connect to one or more of your data warehouse connections like so:

```yaml
project-1-snowflake:
type: snowflake
  account: theblacktux
  # warehouse: DBT
  role: SYSADMIN
  user: tbtadmin
  password: W@rehous3
  database: TPCH
  schema: WH

project-2-bigquery:
type: bigquery
  database: dw-dev
  credentials_path: "/Users/claus/dev/schemaql-bigquery-service-account.json"
```

In `config.yml`, you define the following
- a `collector` which is a definition for how you want test results to be collected. Currently only 'json` is supported
- `projects` which is a combination of a connection and a list of which databases and schemas you want to work with. If you don't define any schemas within the database key, all schemas will be processed.

```
collector:
    type: json
      output: output 

projects:
  project-1:
    connection: project-1-snowflake
    schema:
      TPCH:
      - ODS
      - WH
  project-2:
    connection: project-2-bigquery
    schema:
      DW
```

### Generate

SchemaQL runs tests against schema information contained in `yml` files. You can either write these from scratch, use your existing `dbt` schema files, or use `schemaql` to generate them. 

Generates schema files for all projects:
```bash
schemaql generate
```

Generates schema files for `"my_project"` only:
```bash
schemaql generate -p"my_project"
```

### Test

```bash
schemaql test
```

Test `"my_project"` only:
```bash
schemaql test -p"my_project"
```