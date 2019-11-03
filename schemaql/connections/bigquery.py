import sqlalchemy
from sqlalchemy import create_engine

from schemaql.connections.base_connection import Connection


class BigQueryConnection(Connection):
    """
    BigQuery Connection
    """

    def __init__(self, connection_info):

        self._credentials_path = connection_info["credentials_path"]
        super().__init__(connection_info)

    def _make_engine(self, connect=False):

        if self._credentials_path:
            self._engine = create_engine(
                "bigquery://", credentials_path=self._credentials_path
            )
        else:
            self._engine = create_engine("bigquery://")

        if connect:
            return self._engine.connect()
        else:
            return self._engine
