from apscheduler.schedulers.background import BackgroundScheduler
from etl.db.base import DBMixin
from etl.repositories import ServiceGroupRepository
from etl.entities.config import ServiceGroupConfig

class ETLScheduler(DBMixin):
    def __init__(self, oracle_uri: str):
        super().__init__(oracle_uri)
        self.scheduler = BackgroundScheduler()
        self.jobs = {}

    def start(self):
        """Initialize and start the scheduler"""
        self._load_scheduled_groups()
        self.scheduler.start()

    def _load_scheduled_groups(self):
        """Load all scheduled groups from database"""
        session = self.get_local_session()
        try:
            repo = ServiceGroupRepository(session)
            groups = repo.get_scheduled_groups()
            
            for group in groups:
                self._schedule_group(group)
        finally:
            self.shutdown_session(session)

    def _schedule_group(self, group: ServiceGroupConfig):
        """Add a group to the scheduler"""
        from etl.services.etl_service import ETLService  # Avoid circular imports
        
        def job_wrapper():
            etl = ETLService(self.local_db_uri)
            try:
                etl.run_etl_group(group.group_name)
            finally:
                etl.shutdown_session(etl.get_local_session())
        
        job = self.scheduler.add_job(
            job_wrapper,
            'cron',
            **self._parse_cron(group.schedule_cron)
        )
        self.jobs[group.group_name] = job

    def _parse_cron(self, cron_str: str) -> dict:
        """Convert cron string to APScheduler format"""
        parts = cron_str.split()
        return {
            'minute': parts[0],
            'hour': parts[1],
            'day': parts[2],
            'month': parts[3],
            'day_of_week': parts[4]
        }

    def shutdown(self):
        """Graceful shutdown"""
        self.scheduler.shutdown()