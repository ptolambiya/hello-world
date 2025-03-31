from sqlalchemy import create_engine, text, URL, MetaData, Table, bindparam
from sqlalchemy.exc import SQLAlchemyError
from etl.connections.base_connection import BaseConnection
from sqlalchemy.engine import URL
import json
import jaydebeapi
import cx_Oracle
import os
from etl.utils.logger import log

from etl.entities.config import (
    SystemConfig,
    ServiceGroupConfig,
    ServiceDetailConfig,
    ServiceColumnMapping
)

class DatabaseConnection(BaseConnection):
    DIALECT_MAP = {
        'ORACLE': 'oracle',
        'MSSQL': 'mssql',
        'DB2': 'db2',
        'POSTGRES': 'postgresql',
        'MYSQL': 'mysql',
        'JDBC': 'jdbc' 
    }
    
    DRIVER_MAP = {
        'ORACLE': 'cx_oracle',
        'MSSQL': 'pymssql',
        'DB2': 'ibm_db',
        'POSTGRES': 'psycopg2',
        'MYSQL': 'pymysql',
        'JDBC': 'jaydebeapi'
    }

    def __init__(self):
        self.engine = None
        self.session = None
        self.system_type = None

    def connect(self, config: dict):
        if self.system_type == 'JDBC':
            self._connect_jdbc(json.loads(config))
        else:
            dialect = self.DIALECT_MAP[self.system_type]
            driver = self.DRIVER_MAP[self.system_type]
            conn_str = self._build_connection_string(dialect, driver, json.loads(config))

            if self.system_type == 'ORACLE':
                oracle_params = {
                    'pool_size': 5,
                    'max_overflow': 10,
                    'pool_recycle': 3600,
                    #'connect_args': {'auto_convert_lobs': False}  # Disable LOB conversion
                    'connect_args': {"threaded": True}  # Disable LOB conversion
                                }
                self.engine = create_engine(conn_str, **oracle_params)
            else:
                self.engine = create_engine(conn_str)

            self.session = self.engine.connect()

    def _connect_jdbc(self, config):
        """JDBC-specific connection using JayDeBeApi"""
        jdbc_class = config['jdbc_class']
        jdbc_url = config['jdbc_url']
        user = config.get('user', '')
        password = config.get('password', '')
        libs_dir = os.path.join(os.getcwd(), "etl\libs")
        jar_files = []
        for jar in config.get('jars', '').split(','):
            jar_files.append(os.path.join(libs_dir,jar))
        
        # Convert list to a classpath-separated string (":" for Linux/macOS, ";" for Windows)
        classpath_separator = ";" if os.name == "nt" else ":"
        jdbc_driver_path = classpath_separator.join(jar_files)
        print(jdbc_url)
        print("jdbc_driver_path :" + jdbc_driver_path)
        # Create JDBC connection
        self.engine = create_engine(
            "jdbc+jaydebeapi://",
            creator=lambda: jaydebeapi.connect(
                jdbc_class,
                jdbc_url,
                {'user': user, 'password': password},
                [jdbc_driver_path]
            )
        )

        self.session = self.engine.connect()

    def _build_connection_string(self, dialect, driver, config):
        if self.system_type == 'ORACLE':
            return f"{dialect}+{driver}://{config['user']}:{config['password']}@" \
                   f"{config['host']}:{config['port']}/?service_name={config['service_name']}"
        elif self.system_type == 'MSSQL':
            return URL.create(
                f"{dialect}+{driver}",
                username=config['user'],
                password=config['password'],
                host=config['host'],
                port=config['port'],
                database=config['database']
            )
        elif self.system_type == 'POSTGRES':
            return f"{dialect}+{driver}://{config['user']}:{config['password']}@" \
                   f"{config['host']}:{config['port']}/{config['database']}"
        elif self.system_type == 'DB2':
            return f"{dialect}+{driver}://{config['user']}:{config['password']}@" \
                   f"{config['host']}:{config['port']}/{config['database']}"
        elif self.system_type == 'MYSQL':
            # MySQL connection string format
            return f"{dialect}+{driver}://{config['user']}:{config['password']}@" \
                   f"{config['host']}:{config['port']}/{config['database']}?charset=utf8mb4"        

    def extract_data(self, detail: ServiceDetailConfig):
        result = self.session.execute(text(detail.extraction_query))
        return [row._asdict() for row in result]
        #return result

    def load_data(self, target_transaction,table_name, data):
        if not data:
            return
            
        # Reflect destination table structure
        metadata = MetaData()

        # Reflect the destination table using engine
        #with self.engine.connect() as conn:
        #metadata.reflect(bind=target_transaction, only=[table_name])
        table = Table(table_name, metadata,autoload_with=self.engine)
        
        # Get destination column names (case-insensitive)
        dest_columns = {col.name.upper(): col.name for col in table.columns}
        #print(dest_columns)
        # Map source data to destination columns
        mapped_data = []
        for row in data:
            mapped_row = {}
            for src_key, value in row.items():
                normalized_key = src_key.upper()
                if normalized_key in dest_columns:
                    dest_col = dest_columns[normalized_key]
                    mapped_row[dest_col] = value
            if mapped_row:
                mapped_data.append(mapped_row)
        
        #print(mapped_data)
        if not mapped_data:
            log.error("No columns matched between source and destination")
            return

        try:
            # Use SQLAlchemy Core for Oracle-optimized bulk insert
            column_list = list(mapped_data[0].keys())
            #print(column_list)
            #print(mapped_data)
            stmt = table.insert().values({col: f":{col}" for col in column_list})  # Use SQLAlchemy insert()
            BATCH_SIZE = 5000  # Tune this based on DB performance
            # Process in batches
            for i in range(0, len(mapped_data), BATCH_SIZE):
                batch = mapped_data[i:i + BATCH_SIZE]
                target_transaction.execute(stmt,batch)  # executemany() runs automatically
                #target_transaction.commit()  # Commit after each batch
                #log.info(f"Inserted {len(batch)} rows in Batch.")
            #log.info(f"Successfully inserted {len(mapped_data)} rows into {table_name}")
        except Exception as e:
            log.error(f"Database insert error: {str(e)}")
            raise

    def truncate_table(self,target_transaction,destination_table:str):
            target_transaction.execute(text('DELETE FROM ' + destination_table)) #Changed from Truncate to support rollback

    def close(self):
        if self.session:
            self.session.close()
        if self.engine:
            self.engine.dispose()
