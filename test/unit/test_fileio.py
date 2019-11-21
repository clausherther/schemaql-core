import pytest
import yaml
from schemaql.helpers.fileio import check_directory_exists, read_yaml


@pytest.fixture(scope="module")
def directory():
    from pathlib import Path
    d = Path("test/test_dir").resolve()
    yield d

    print(f"removing {d}")

    if Path(d).exists():
        Path(d).rmdir()


class TestFileIO(object):

    def test_check_directory_exists(self, directory):

        p = check_directory_exists(directory)
        assert p == directory

    def test_read_yaml(self):

        test_yml = """
        projects:
            project-1:
                connection: project-1-snowflake
                schema:
                    database_1:
                    - schema_1
                    - schema_2
        """
        yml_expected = yaml.load(test_yml)

        yml = read_yaml("test/unit/test.yml")

        assert yml == yml_expected
