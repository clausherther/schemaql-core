import os
import yaml
import inspect

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

def read_yaml(yaml_path, storage_model='local'):

    with open(os.path.abspath(yaml_path), 'r') as f:
        yml = yaml.load(f, Loader=yaml.FullLoader)

    return yml