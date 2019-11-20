import datetime
import json
from pathlib import Path
import uuid

import pandas as pd

from schemaql.helpers.fileio import check_directory_exists
from schemaql.helpers.logger import logger


class Collector(object):
    """
    Base Collector
    """

    def __init__(self, collector_config):

        self._collector_type = collector_config["type"]
        self._fail_only = collector_config.get("fail_only", False)

    @property
    def collector_type(self):
        return self._collector_type

    @property
    def collect_failures_only(self):
        return self._fail_only

    def convert_to_dataframe(self, test_results):

        df_results = pd.DataFrame.from_dict(test_results)

        if self.collect_failures_only:
            if "aggregation_passed" in list(df_results.columns):
                df_results = df_results[df_results["aggregation_passed"] is False]

        df_results["aggregation_result"] = df_results["aggregation_result"].astype(
            float
        )
        df_results["_batch_id"] = str(uuid.uuid1())
        df_results["_batch_timestamp"] = datetime.datetime.now()

        df_results["_result_id"] = df_results.apply(lambda x: str(uuid.uuid1()), axis=1)

        return df_results


class JsonCollector(Collector):
    """
    Json Collector
    """

    def __init__(self, collector_config):
        super().__init__(collector_config)

        self._output_location = collector_config["output"]
        self._output_file = "results.json"

    def save_test_results(self, project_name, test_results):

        logger.info(f"Collecting to {self._output_file}")

        test_results_json = json.dumps(test_results, indent=4, sort_keys=True)

        output_directory = Path(self._output_location).joinpath(project_name)
        check_directory_exists(output_directory)
        json_output_file = output_directory.joinpath(self._output_file)
        json_output_file.write_text(test_results_json)


class CsvCollector(Collector):
    """
    CSV Collector
    """

    def __init__(self, collector_config):
        super().__init__(collector_config)

        self._output_location = collector_config["output"]
        self._output_file = "results.csv"

    def save_test_results(self, project_name, test_results):

        logger.info(f"Collecting to {self._output_file}")

        df_results = self.convert_to_dataframe(test_results)

        output_directory = Path(self._output_location).joinpath(project_name)
        check_directory_exists(output_directory)
        csv_output_file = output_directory.joinpath(self._output_file)
        df_results.to_csv(csv_output_file, index=False)


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
