import datetime
import json
from pathlib import Path
import uuid

import pandas as pd

from schemaql.helpers.fileio import check_directory_exists, read_yaml, schemaql_path
from schemaql.helpers.logger import Back, Fore, Style, logger


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
            if "test_passed" in list(df_results.columns):
                df_results = df_results[df_results["test_passed"] == False]

        df_results["test_result"] = df_results["test_result"].astype(float)
        df_results["test_batch_id"] = str(uuid.uuid1())
        df_results["test_batch_timestamp"] = datetime.datetime.now()

        df_results["test_result_id"] = df_results.apply(
            lambda x: str(uuid.uuid1()), axis=1
        )

        return df_results


class JsonCollector(Collector):
    """
    Json Collector
    """

    def __init__(self, collector_config):
        super().__init__(collector_config)

        self._output_location = collector_config["output"]

    def save_test_results(self, project_name, test_results):

        test_results_json = json.dumps(test_results, indent=4, sort_keys=True)

        output_directory = Path(self._output_location).joinpath(project_name)
        check_directory_exists(output_directory)
        json_output_file = output_directory.joinpath("test_results.json")
        json_output_file.write_text(test_results_json)
