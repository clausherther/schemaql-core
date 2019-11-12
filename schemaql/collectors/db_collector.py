import datetime
import json
import uuid
from pathlib import Path

import pandas as pd

from schemaql.collectors.base_collector import Collector
from schemaql.helpers.logger import Back, Fore, Style, logger


class DbCollector(Collector):
    """
    Database Collector
    """

    def __init__(self, collector_config, connector):
        super().__init__(collector_config)

        self._output = collector_config["output"]
        self._connector = connector

    def save_test_results(self, project_name, test_results):

        df_results = self.convert_to_dataframe(test_results)

        logger.info(f"Collecting on {self._connector.connector_type}")
        if self._connector.supports_multi_insert:
            insert_method = "multi"
            logger.info("Using multi-row inserts")
        else:
            insert_method = None
            logger.info("Using single-row inserts")

        with self._connector.connect() as con:

            df_results.to_sql(
                name=self._output,
                con=con,
                if_exists="append",
                index=False,
                method=insert_method,
            )
