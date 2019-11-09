from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()


package_name = "schemaql"
package_version = "0.0.2"
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
    install_requires=[
        'cffi==1.13.0',
        'Jinja2>=2.10',
        'PyYAML>=5.1',
        'plac>=1.0.0',
        'pybigquery>=0.4.11',
        'colorama==0.3.9',
        'snowflake-connector-python==2.0.3',
        'snowflake-sqlalchemy>=1.1.14',
        'SQLAlchemy>=1.3.7',
        'sqlalchemy-redshift>=0.7.5',
        'psycopg2>=2.7.5,<2.8',
    ]
)
