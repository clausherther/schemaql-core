from pathlib import Path
import yaml
import inspect

schemaql_path = Path(__file__).parent

def read_yaml(yaml_path, storage_model='local'):

    with open(Path(yaml_path).resolve(), 'r') as f:
        yml = yaml.load(f, Loader=yaml.FullLoader)

    return yml


def check_directory_exists(directory):
    Path(directory).mkdir(parents=True, exist_ok=True)
