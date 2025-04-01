from datetime import datetime
from sqlalchemy.exc import DatabaseError
from typing import List
from etl.utils.logger import log
from etl.entities.config import ExecutionErrorLog

from etl.db.base import DBMixin
from etl.repositories import (
    SystemConfigRepository,
    ServiceGroupRepository,
    ServiceDetailRepository,
    ServiceColumnMappingRepository,
    ErrorLogRepository
)

from etl.connections import BaseConnection, DatabaseConnection, APIConnection, KafkaConnection
from etl.parsers import BaseParser, TableParser, JsonParser, XmlParser, CsvParser

class ETLService(DBMixin):

    def __init__(self, config_db_uri: str):
        super().__init__(config_db_uri)
        # Initialize repositories
        self.session = self.get_local_session()
        self.system_repo = SystemConfigRepository(self.session)
        self.group_repo = ServiceGroupRepository(self.session)
        self.detail_repo = ServiceDetailRepository(self.session)
        self.mapping_repo = ServiceColumnMappingRepository(self.session)
        self.error_repo = ErrorLogRepository(self.session)

    # CONNECTION_MAP = {
    #     'ORACLE': DatabaseConnection,
    #     'MSSQL': DatabaseConnection,
    #     'MYSQL': DatabaseConnection,
    #     'DB2': DatabaseConnection,
    #     'POSTGRES': DatabaseConnection,
    #     'JDBC': DatabaseConnection,
    #     'API': APIConnection,
    #     'KAFKA': KafkaConnection
    # }
    
    # PARSER_MAP = {
    #     'TABLE': TableParser,
    #     'JSON': JsonParser,
    #     'XML': XmlParser,
    #     'CSV': CsvParser
    # }


    
    def run_etl_group(self, group_name: str):

        source_conn = None
        target_conn = None
        target_transaction = None        
        try:
            group = self.group_repo.get_by_name(group_name)

            if group.is_active == 'N':
                log.info(f"Group {group.group_name} is deactivated.")
            else:
                source_system = self.system_repo.get_by_id(group.source_system_id)
                source_conn = self._get_connection(source_system.system_type)
                source_conn.system_type = source_system.system_type
                source_conn.connect(source_system.connection_config)

                target_system = self.system_repo.get_by_id(group.target_system_id)
                target_conn = self._get_connection(target_system.system_type)
                target_conn.system_type = target_system.system_type
                target_conn.connect(target_system.connection_config)
                #target_transaction = target_conn.engine.begin()

                details = self.detail_repo.get_by_group_ordered(group.id)
                with target_conn.engine.begin() as target_transaction:
                    for detail in details:
                        self._process_detail(source_conn, target_conn, target_transaction, detail)

                target_transaction.commit()    
                group.last_execution = datetime.now()
                self.session.commit()
        except Exception as e:
            self.session.rollback()
            if target_transaction:
                target_transaction.rollback()
            if target_conn and target_conn.engine:
                target_conn.engine.dispose()
            #raise
        finally:
            if source_conn:
                source_conn.close()
            if target_conn:
                target_conn.close()
            self.session.close()

    def _process_detail(self, source_conn, target_conn, target_transaction, detail):
        log.info(f"START - Group: {detail.service_group.group_name} | Service : {detail.service_detail_name}")
        record_count = 0
        
        try:
            # Extraction
            raw_data = source_conn.extract_data(detail)
            # Transformation
            mappings = self.mapping_repo.get_by_service_detail(detail.id)
            #parser = self.PARSER_MAP[detail.source_data_type](mappings)
            #parser = BaseParser.get_parser_class(detail.source_data_type)(mappings)
            parser_cls = BaseParser.get_parser_class(detail.source_data_type)
            parser = parser_cls(mappings)
            processed_data = parser.parse(raw_data)
            record_count = len(processed_data)

            if detail.trim_col_list:
                processed_data = self._apply_trimming(processed_data, detail.trim_col_list)
            
            # Target process
            if detail.target_truncate == 'Y':
                target_conn.truncate_table(target_transaction,detail.destination_table)

            # Loading
            if processed_data:
                target_conn.load_data(target_transaction,detail.destination_table, processed_data)

            # Reset error count on success
            self.group_repo.reset_error_count(detail.service_group)

            log.info(f"END - Group: {detail.service_group.group_name} | Service : {detail.service_detail_name} | Records: {record_count}")
        except Exception as e:
            log.error(f"Group {detail.service_group.group_name} | Service : {detail.service_detail_name} | Execution Failed." )
            self._handle_error(detail, e)
            raise

    def _get_connection(self, system_type):
        # conn_class = self.CONNECTION_MAP.get(system_type)
        conn_class = BaseConnection.get_connection_class(system_type)
        if not conn_class:
            raise ValueError(f"Unsupported system type: {system_type}")
        return conn_class()

    def _apply_trimming(self, data, trim_cols):
        return [
            {k: v.strip() if k in trim_cols and isinstance(v, str) else v
             for k, v in row.items()}
            for row in data
        ]

    def _handle_error(self, detail, error):
        try:
            # Create error log through repository
            error_log = ExecutionErrorLog(
                group_id=detail.group_id,
                service_detail_id=detail.id,
                error_detail=str(error)[:4000]  # Truncate for Oracle VARCHAR2
            )
            self.error_repo.create(error_log)
            
            # Update error count through repository
            updated_group = self.group_repo.increment_error_count(detail.service_group)
            
            if updated_group.is_active == 'N':
                log.info(f"Group {updated_group.group_name} deactivated due to error threshold")
                
            self.session.commit()
        except Exception as e:
            log.error(f"Failed to log error: {str(e)}")
            self.session.rollback()
