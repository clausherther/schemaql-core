import sqlalchemy
from sqlalchemy import create_engine

from schemaql.connections.base_connection import Connection

class BigQueryConnection(Connection):
    """
    Database Connection
    """

    def __init__(self, connection_info):

        super().__init__(connection_info)
        self._credentials_path = connection_info["credentials_path"] #sif "credentials_path" in connection_info else None

        
    def make_engine(self, connect=False):

        if self._credentials_path:
            self._engine = create_engine(self.connect_url, credentials_path=self._credentials_path)
        else:
            self._engine = create_engine(self.connect_url)

        if connect:
            return self._engine.connect()
        else:
            return self._engine