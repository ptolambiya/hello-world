from datetime import datetime
from sqlalchemy.exc import DatabaseError
from typing import List

from etl.db.base import DBMixin
from etl.repositories import (
    SystemConfigRepository,
    ServiceGroupRepository,
    ServiceDetailRepository,
    ServiceColumnMappingRepository
)

from etl.connections import DatabaseConnection, APIConnection, KafkaConnection
from etl.parsers import TableParser, JsonParser, XmlParser, CsvParser

class ETLService(DBMixin):
    CONNECTION_MAP = {
        'ORACLE': DatabaseConnection,
        'MSSQL': DatabaseConnection,
        'MYSQL': DatabaseConnection,
        'DB2': DatabaseConnection,
        'POSTGRES': DatabaseConnection,
        'JDBC': DatabaseConnection,
        'API': APIConnection,
        'KAFKA': KafkaConnection
    }
    
    PARSER_MAP = {
        'TABLE': TableParser,
        'JSON': JsonParser,
        'XML': XmlParser,
        'CSV': CsvParser
    }

    def run_etl_group(self, group_name: str):
        local_session = self.get_local_session()
        source_conn = None
        target_conn = None
        target_transaction = None        
        try:
            group = ServiceGroupRepository(local_session).get_by_name(group_name)

            source_system = SystemConfigRepository(local_session).get_by_id(group.source_system_id)
            source_conn = self._get_connection(source_system.system_type)
            source_conn.system_type = source_system.system_type
            source_conn.connect(source_system.connection_config)

            target_system = SystemConfigRepository(local_session).get_by_id(group.target_system_id)
            target_conn = self._get_connection(target_system.system_type)
            target_conn.system_type = target_system.system_type
            target_conn.connect(target_system.connection_config)
            #target_transaction = target_conn.engine.begin()
            details = ServiceDetailRepository(local_session).get_by_group_ordered(group.id)
            with target_conn.engine.begin() as target_transaction:
                for detail in details:
                    #print(detail)
                    self._process_detail(local_session, source_conn, target_conn, target_transaction, detail)

            target_transaction.commit()    
            group.last_execution = datetime.now()
            local_session.commit()
            
        except Exception as e:
            local_session.rollback()
            if target_transaction:
                target_transaction.rollback()
            if target_conn and target_conn.engine:
                target_conn.engine.dispose()
            raise
        finally:
            if source_conn:
                source_conn.close()
            if target_conn:
                target_conn.close()
            local_session.close()

    def _process_detail(self, session, source_conn, target_conn, target_transaction, detail):
        
        # Extraction
        raw_data = source_conn.extract_data(detail)

        # Transformation
        mappings = ServiceColumnMappingRepository(session).get_by_service_detail(detail.id)
        parser = self.PARSER_MAP[detail.source_data_type](mappings)
        processed_data = parser.parse(raw_data)

        if detail.trim_col_list:
            processed_data = self._apply_trimming(processed_data, detail.trim_col_list)
        
        # Target process
        if detail.target_truncate == 'Y':
            target_conn.truncate_table(target_transaction,detail.destination_table)

        # Loading
        if processed_data:
            target_conn.load_data(target_transaction,detail.destination_table, processed_data)

    def _get_connection(self, system_type):
        conn_class = self.CONNECTION_MAP.get(system_type)
        if not conn_class:
            raise ValueError(f"Unsupported system type: {system_type}")
        return conn_class()

    def _apply_trimming(self, data, trim_cols):
        return [
            {k: v.strip() if k in trim_cols and isinstance(v, str) else v
             for k, v in row.items()}
            for row in data
        ]

    


