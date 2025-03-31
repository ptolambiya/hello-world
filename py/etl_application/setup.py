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


        # # Database connectors
        # "sqlalchemy>=1.4.0",  # ORM and core SQL support
        # "cx_oracle>=8.3.0",  # Oracle
        # "pymssql>=2.2.5",  # SQL Server (alternative: pyodbc)
        # "pyodbc>=4.0.0",  # SQL Server, DB2, Sybase
        # "mysql-connector-python>=8.0.0",  # MySQL
        # "cryptography"
        # "ibm-db>=3.1.0",  # DB2
        # "sqlalchemy-access>=1.0.0",  # Sybase (if needed)
        
        # # REST API
        # "requests>=2.27.0",  # HTTP client
        # "fastapi>=0.75.0",  # For creating REST APIs (optional)
        # "uvicorn>=0.17.0",  # ASGI server (if using FastAPI)
        
        # # Kafka
        # "confluent-kafka>=1.8.2",  # Kafka client
        # "kafka-python>=2.0.2",  # Alternative Kafka client
        
        # # Utilities
        # "pandas>=1.3.0",  # Data manipulation
        # "numpy>=1.21.0",  # Numerical operations
        # "tenacity>=8.0.0",  # Retry logic
        # "tqdm>=4.62.0",  # Progress bars
        # "python-json-logger>=2.0.0",  # JSON formatted logs