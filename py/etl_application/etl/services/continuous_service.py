import threading
import time
from etl.db.base import DBMixin
from etl.repositories import ServiceGroupRepository
from etl.services.etl_service import ETLService
from etl.entities.config import ServiceGroupConfig
from etl.utils.logger import log

class ContinuousETLProcessor(DBMixin):
    def __init__(self, oracle_uri: str):
        super().__init__(oracle_uri)
        self.running = False
        self.threads = {}

    def start(self):
        """Start continuous processing"""
        self.running = True
        self._load_continuous_groups()

    def _load_continuous_groups(self):
        """Initialize all continuous groups"""
        session = self.get_local_session()
        try:
            repo = ServiceGroupRepository(session)
            groups = repo.get_continuous_groups()
            
            for group in groups:
                self._start_group_processor(group)
        finally:
            self.shutdown_session(session)

    def _start_group_processor(self, group: ServiceGroupConfig):
        """Start a thread for continuous processing"""
        def processor():
            while self.running:
                etl = ETLService(self.local_db_uri)
                try:
                    etl.run_etl_group(group.group_name)
                except Exception as e:
                    log.error(f"Error in group {group.group_name}: {str(e)}")
                    time.sleep(10)  # Backoff on error
                finally:
                    etl.shutdown_session(etl.get_local_session())
        
        thread = threading.Thread(
            target=processor,
            daemon=True
        )
        self.threads[group.group_name] = thread
        thread.start()

    def stop(self):
        """Stop all continuous processors"""
        self.running = False
        for thread in self.threads.values():
            thread.join(timeout=30)