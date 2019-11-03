import pytest
from schemaql.helper import read_yaml
from schemaql.connections.snowflake import SnowflakeConnection
from schemaql.connections.bigquery import BigQueryConnection

class TestConnections(object):

    def _get_connection(self, connection_name):
        connections_file = "connections.yml"
        connections = read_yaml(connections_file)

        connection_info = connections[connection_name]
        connection_type = connection_info["type"]

        supported_connections = {
            "snowflake": SnowflakeConnection,
            "bigquery": BigQueryConnection,
        }

        connector = supported_connections[connection_type](connection_info)

        return connector 

    def test_bigquery_connection(self):

        conn = self._get_connection("tpch-snowflake")
        assert conn is not None
        cur = conn.engine.connect()

        assert cur is not None


    def test_snowflake_connection(self):

        conn = self._get_connection("calogica-bq")
        assert conn is not None
        cur = conn.engine.connect()

        assert cur is not None    

