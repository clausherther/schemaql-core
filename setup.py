from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()


package_name = "schemaql"
package_version = "0.0.1"
description = "A testing and auditing tool inspired by dbt, for those not using dbt."

setup(
    name=package_name,
    version=package_version,
    author="Calogica, LLC",
    author_email="info@calogica.com",
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/schemaql",
    packages=find_packages(),
    scripts=['bin/schemaql'],
    python_requires='>=3.6',
)
