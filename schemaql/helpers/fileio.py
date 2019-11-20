from pathlib import Path
import yaml

from schemaql.helpers.logger import logger

schemaql_path = Path(__file__).parent.parent


def read_yaml(yaml_path, storage_model='local'):

    try:
        with open(Path(yaml_path).resolve(), 'r') as f:
            yml = yaml.load(f, Loader=yaml.FullLoader)
    except FileNotFoundError:
        logger.info(f"Could not find {yaml_path}. Please check that {yaml_path} exists.")
        raise

    return yml


def check_directory_exists(directory):
    Path(directory).mkdir(parents=True, exist_ok=True)
