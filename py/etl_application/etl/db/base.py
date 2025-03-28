from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine

Base = declarative_base()

class DBMixin:
    def __init__(self, local_db_uri: str):
        self.local_engine = create_engine(local_db_uri)
        self.local_session_factory = sessionmaker(bind=self.local_engine)
        self.LocalSession = scoped_session(self.local_session_factory)
        
    def get_local_session(self):
        return self.LocalSession()
    
    def shutdown_session(self, session):
        if session:
            session.close()
            self.LocalSession.remove()

