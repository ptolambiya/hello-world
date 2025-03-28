from setuptools import setup, find_packages

setup(
    name="etl",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'sqlalchemy>=1.4.0',
        'cx_Oracle>=8.3.0',
        'pymssql>=2.2.7',
        'ibm_db>=3.1.0',
        'psycopg2-binary>=2.9.3',
        'kafka-python>=2.0.2',
        'requests>=2.26.0',
        'jsonpath-ng>=1.5.3',
        'lxml>=4.6.3',
        'python-dateutil>=2.8.2',
        'pymysql>=1.1.1',
        'jaydebeapi>=1.2.3'
    ],
    entry_points={
        'console_scripts': [
            'run-etl=etl.services.etl_service:main',
        ],
    },
)
