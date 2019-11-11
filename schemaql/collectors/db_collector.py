import datetime
import json
# import time
from pathlib import Path
import uuid

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

        df_results = pd.DataFrame.from_dict(test_results)

        df_results["test_result"] = df_results["test_result"].astype(float)
        df_results["test_batch_id"] = str(uuid.uuid1())
        df_results["test_batch_timestamp"] = datetime.datetime.now()

        df_results["test_result_id"] = df_results.apply(lambda x: str(uuid.uuid1()), axis=1)

        logger.info(df_results)
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
                method=insert_method
            )
