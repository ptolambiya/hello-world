# from sqlalchemy.orm import Session
# from etl.entities.config import SystemConfig, ServiceGroupConfig, ServiceDetailConfig, ServiceColumnMapping

# from sqlalchemy.orm import Session

# class SystemConfigRepository:
#     def __init__(self, session: Session):
#         self.session = session
        
#     def get_by_name(self, name: str):
#         return self.session.query(SystemConfig).filter(SystemConfig.system_name == name).first()

#     def get_by_id(self, id: int):
#         return self.session.query(SystemConfig).filter(SystemConfig.id == id).first()

# class ServiceGroupRepository:
#     def __init__(self, session: Session):
#         self.session = session
        
#     def get_by_name(self, name: str):
#         return self.session.query(ServiceGroupConfig).filter(ServiceGroupConfig.group_name == name).first()

# class ServiceDetailRepository:
#     def __init__(self, session: Session):
#         self.session = session
        
#     def get_by_group_ordered(self, group_id: int):
#         return self.session.query(ServiceDetailConfig)\
#             .filter(ServiceDetailConfig.group_id == group_id)\
#             .order_by(ServiceDetailConfig.process_order)\
#             .all()

# class ServiceColumnMappingRepository:
#     def __init__(self, session: Session):
#         self.session = session
        
#     def get_by_service_detail(self, detail_id: int):
#         return self.session.query(ServiceColumnMapping)\
#             .filter(ServiceColumnMapping.service_detail_id == detail_id)\
#             .order_by(ServiceColumnMapping.process_order)\
#             .all()