from sqlalchemy.orm import Session
from typing import List, Optional
from etl.entities.config import (
    SystemConfig,
    ServiceGroupConfig,
    ServiceDetailConfig,
    ServiceColumnMapping
)

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
    
    def get_active_groups(self) -> List[ServiceGroupConfig]:
        return self._session.query(ServiceGroupConfig)\
            .filter(ServiceGroupConfig.is_running == False)\
            .all()

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