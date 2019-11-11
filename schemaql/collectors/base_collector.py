
from pathlib import Path
import json

from schemaql.helpers.fileio import check_directory_exists, read_yaml, schemaql_path
from schemaql.helpers.logger import logger, Fore, Back, Style

class Collector(object):
    """
    Base Collector
    """

    def __init__(self, collector_config):

        self._type = collector_config["type"]

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
