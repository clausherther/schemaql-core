import json
from pathlib import Path

from schemaql.collectors.base_collector import Collector
from schemaql.helpers.fileio import check_directory_exists, read_yaml, schemaql_path
from schemaql.helpers.logger import Back, Fore, Style, logger


class CsvCollector(Collector):
    """
    CSV Collector
    """

    def __init__(self, collector_config):
        super().__init__(collector_config)

        self._output_location = collector_config["output"]

    def save_test_results(self, project_name, test_results):

        df_results = self.convert_to_dataframe(test_results)

        output_directory = Path(self._output_location).joinpath(project_name)
        check_directory_exists(output_directory)
        csv_output_file = output_directory.joinpath("test_results.csv")
        df_results.to_csv(csv_output_file, index=False)
