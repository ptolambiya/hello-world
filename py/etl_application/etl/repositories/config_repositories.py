from sqlalchemy.orm import Session
from typing import List, Optional
from etl.entities.config import (
    SystemConfig,
    ServiceGroupConfig,
    ServiceDetailConfig,
    ServiceColumnMapping,
    ExecutionErrorLog
)
from datetime import datetime, timedelta

class SystemConfigRepository:
    def __init__(self, session: Session):
        self._session = session
    
    def get_by_name(self, name: str) -> Optional[SystemConfig]:
        return self._session.query(SystemConfig)\
            .filter(SystemConfig.system_name == name)\
            .first()
    
    def get_by_id(self, config_id: int) -> Optional[SystemConfig]:
        return self._session.query(SystemConfig)\
            .filter(SystemConfig.id == config_id)\
            .first()
    
    def get_all_active(self) -> List[SystemConfig]:
        return self._session.query(SystemConfig)\
            .filter(SystemConfig.is_active == True)\
            .all()

class ServiceGroupRepository:
    def __init__(self, session: Session):
        self._session = session
    
    def get_by_name(self, name: str) -> Optional[ServiceGroupConfig]:
        return self._session.query(ServiceGroupConfig)\
            .filter(ServiceGroupConfig.group_name == name)\
            .first()
    
    def get_scheduled_groups(self) -> List[ServiceGroupConfig]:
        return self._session.query(ServiceGroupConfig)\
            .filter(ServiceGroupConfig.execution_mode == 'SCHEDULED')\
            .filter(ServiceGroupConfig.is_active == 'Y')\
            .all()
    
    def get_continuous_groups(self) -> List[ServiceGroupConfig]:
        return self._session.query(ServiceGroupConfig)\
            .filter(ServiceGroupConfig.execution_mode == 'CONTINUOUS')\
            .filter(ServiceGroupConfig.is_active == 'Y')\
            .all()
    
    def increment_error_count(self, group: ServiceGroupConfig) -> ServiceGroupConfig:
        group.error_count += 1
        if group.error_count >= group.error_threshold:
            group.is_active = 'N'
        self._session.commit()
        return group
    
    def reset_error_count(self, group: ServiceGroupConfig) -> ServiceGroupConfig:
        group.error_count = 0
        self._session.commit()
        return group
        
class ServiceDetailRepository:
    def __init__(self, session: Session):
        self._session = session
    
    def get_by_group_ordered(self, group_id: int) -> List[ServiceDetailConfig]:
        return self._session.query(ServiceDetailConfig)\
            .filter(ServiceDetailConfig.group_id == group_id)\
            .order_by(ServiceDetailConfig.process_order)\
            .all()

class ServiceColumnMappingRepository:
    def __init__(self, session: Session):
        self._session = session
    
    def get_by_service_detail(self, detail_id: int) -> List[ServiceColumnMapping]:
        return self._session.query(ServiceColumnMapping)\
            .filter(ServiceColumnMapping.service_detail_id == detail_id)\
            .order_by(ServiceColumnMapping.process_order)\
            .all()
    
class ErrorLogRepository:
    def __init__(self, session):
        self.session = session
    
    def create(self, error_log: ExecutionErrorLog) -> ExecutionErrorLog:
        self.session.add(error_log)
        return error_log
    
    def get_recent_errors(self, group_id: int, hours: int = 24) -> list[ExecutionErrorLog]:
        return self.session.query(ExecutionErrorLog)\
            .filter(ExecutionErrorLog.group_id == group_id,
                    ExecutionErrorLog.log_time >= datetime.date.today() - timedelta(hours=hours))\
            .all()
