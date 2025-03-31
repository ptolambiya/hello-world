from datetime import datetime
from sqlalchemy.exc import DatabaseError
from etl.utils.logger import logger
from etl.entities.config import ExecutionErrorLog

from etl.db.base import DBMixin
from etl.repositories import (
    SystemConfigRepository,
    ServiceGroupRepository,
    ServiceDetailRepository,
    ServiceColumnMappingRepository
)

from etl.connections import DatabaseConnection, APIConnection, KafkaConnection
from etl.parsers import TableParser, JsonParser, XmlParser, CsvParser

class ETLService1(DBMixin):
    CONNECTION_MAP = {
        'ORACLE': DatabaseConnection,
        'MSSQL': DatabaseConnection,
        'DB2': DatabaseConnection,
        'POSTGRES': DatabaseConnection,
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
        try:
            group = ServiceGroupRepository(local_session).get_by_name(group_name)

            sys_repo = SystemConfigRepository(local_session)
            source_system = sys_repo.get_by_id(group.source_system_id)

            target_system = sys_repo.get_by_id(group.target_system_id)

            details = ServiceDetailRepository(local_session).get_by_group_ordered(group.id)
            for detail in details:
                self._process_detail(local_session, source_system, target_system, detail)
                
            group.last_execution = datetime.now()
            local_session.commit()
            
        except Exception as e:
            local_session.rollback()
            raise
        finally:
            local_session.close()

    def _process_detail(self, session, source_system, target_system, detail):
        source_conn = self._get_connection(source_system.system_type)
        source_conn.system_type = source_system.system_type
        source_conn.connect(source_system.connection_config)
        
        target_conn = self._get_connection(target_system.system_type)
        target_conn.system_type = target_system.system_type
        target_conn.connect(target_system.connection_config)
        
        try:
            # Extraction
            raw_data = source_conn.extract_data(detail.extraction_query)

            # if detail.source_type == 'ORACLE':
            #     raw_data = source_conn.extract_data(detail.extraction_query)
            # else:
            #     raw_data = source_conn.extract_data()

            # Transformation
            mappings = ServiceColumnMappingRepository(session).get_by_service_detail(detail.id)
            parser = self.PARSER_MAP[detail.source_data_type](mappings)
            processed_data = parser.parse(raw_data)

            if detail.trim_col_list:
                processed_data = self._apply_trimming(processed_data, detail.trim_col_list)
            
            # Target process
            if detail.target_truncate == 'Y':
                target_conn.truncate_table(detail.destination_table)

            # Loading
            if processed_data:
                target_conn.load_data(detail.destination_table, processed_data)

        finally:
            source_conn.close()
            target_conn.close()

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
    
    # def _transform_data(self, raw_data, data_type, mappings, trim_cols):
    #     # Create parser with ordered mappings
    #     parser = self.PARSER_MAP.get(data_type, TableParser)(mappings)
    #     parsed_data = parser.parse(raw_data)
        
    #     # Apply trimming and other transformations
    #     transformed_data = []
    #     for row in parsed_data:
    #         transformed_row = {}
    #         for mapping in mappings:
    #             value = row.get(mapping.source_path)
                
    #             # Apply data type conversion
    #             if mapping.data_type:
    #                 value = self._cast_type(value, mapping.data_type)
                
    #             # Apply transformation function
    #             if mapping.transformation:
    #                 value = self._apply_transformation(value, mapping.transformation)
                
    #             transformed_row[mapping.destination_column] = value
                
    #         # Apply column trimming
    #         if trim_cols:
    #             transformed_row = {
    #                 k: v.strip() if k in trim_cols and isinstance(v, str) else v
    #                 for k, v in transformed_row.items()
    #             }
            
    #         transformed_data.append(transformed_row)
        
    #     return transformed_data
    


