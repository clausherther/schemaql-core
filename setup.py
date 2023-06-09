from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()


package_name = "schemaql"
package_version = "0.0.4"
description = "A data quality and auditing tool inspired by dbt test"

setup(
    name=package_name,
    version=package_version,
    author="Calogica, LLC",
    author_email="info@calogica.com",
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    # download_url
    url="https://schemaql.com",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        # If any package contains *.txt or *.rst files, include them:
        '': ['*.sql', '*.yml', '*.md', '*.html'],
    },
    scripts=['bin/schemaql'],
    python_requires='>=3.6',
    install_requires=[
        'colorama==0.3.9',
        'cffi==1.13.0',
        'Jinja2>=2.10',
        'pandas>=0.25.3',
        'plac>=1.0.0',
        'psycopg2>=2.7.5,<2.8',
        'pybigquery>=0.4.11',
        'PyYAML>=5.1',
        'snowflake-connector-python==3.0.2',
        'snowflake-sqlalchemy>=1.1.14',
        'SQLAlchemy>=1.3.11',
        'sqlalchemy-redshift>=0.7.5',
    ],
    zip_safe=False,
    classifiers=[
        'Development Status :: 3 - Alpha',

        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',

        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',

        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)
