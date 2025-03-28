from sqlalchemy import Column, Integer, String, Text, ForeignKey, Boolean, DateTime
from sqlalchemy.orm import relationship
from etl.db.base import Base
from datetime import datetime

class SystemConfig(Base):
    __tablename__ = 'system_config'
    id = Column(Integer, primary_key=True)
    system_name = Column(String(50), unique=True, nullable=False)
    system_type = Column(String(20), nullable=False)  # ORACLE, MSSQL, DB2, POSTGRES, API, KAFKA
    connection_config = Column(Text, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return "id: " + str(self.id) + ", system_name: " +  self.system_name +  ", system_type: " + self.system_type + ", connection_config: "+ self.connection_config
    
class ServiceGroupConfig(Base):
    __tablename__ = 'service_group_config'
    id = Column(Integer, primary_key=True)
    group_name = Column(String(50), unique=True, nullable=False)
    source_system_id = Column(Integer, ForeignKey('system_config.id'), nullable=False)
    target_system_id = Column(Integer, ForeignKey('system_config.id'), nullable=False)
    schedule_cron = Column(String(20), nullable=False)
    last_execution = Column(DateTime)
    is_running = Column(Boolean, default=False)

    #source_system = relationship("SystemConfig", foreign_keys=[source_system_id])
    #target_system = relationship("SystemConfig", foreign_keys=[target_system_id])
    #details = relationship("ServiceDetailConfig", back_populates="service_group")

    def __repr__(self):
        return "id: " + str(self.id) + ", group_name: " + self.group_name +  ", source_system_id: " + str(self.source_system_id) + ", target_system_id: "+str(self.target_system_id)

class ServiceDetailConfig(Base):
    __tablename__ = 'service_detail_config'
    id = Column(Integer, primary_key=True)
    group_id = Column(Integer, ForeignKey('service_group_config.id'), nullable=False)
    source_type = Column(String(20), nullable=False)  # DATABASE, API, KAFKA
    source_data_type = Column(String(10))  # TABLE, JSON, XML, CSV
    extraction_query = Column(Text)
    destination_table = Column(String(30), nullable=False)
    target_truncate = Column(String(1), default='N')
    trim_col_list = Column(Text)
    process_order = Column(Integer, nullable=False)

    #service_group = relationship("ServiceGroupConfig", back_populates="details")
    #column_mappings = relationship("ServiceColumnMapping", back_populates="service_detail")

    def __repr__(self):
        return "id: " + str(self.id) + ", group_id: " + str(self.group_id) +  ", source_type: " + self.source_type + ", source_data_type: " + self.source_data_type + ", extraction_query: "+ self.extraction_query
    
class ServiceColumnMapping(Base):
    __tablename__ = 'service_column_mapping'
    id = Column(Integer, primary_key=True)
    service_detail_id = Column(Integer, ForeignKey('service_detail_config.id'), nullable=False)
    source_path = Column(String(400), nullable=False)
    destination_column = Column(String(30), nullable=False)
    data_type = Column(String(20))
    transformation = Column(String(50))
    process_order = Column(Integer, nullable=False)

    #service_detail = relationship("ServiceDetailConfig", back_populates="column_mappings")

    def __repr__(self):
        return "id: " + str(self.id) + ", service_detail_id: " + str(self.service_detail_id) +  ", source_path: " + self.source_path + ", destination_column: " + self.destination_column + ", transformation: "+ self.transformation
