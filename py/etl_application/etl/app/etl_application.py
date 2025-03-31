from etl.services.scheduler_service import ETLScheduler
from etl.services.continuous_service import ContinuousETLProcessor
import signal
import time

class ETLApplication:
    def __init__(self, config_db_uri: str):
        self.config_db_uri = config_db_uri
        self.scheduler = ETLScheduler(config_db_uri)
        self.continuous = ContinuousETLProcessor(config_db_uri)

    def start(self):
        """Start all services"""
        # Start scheduled jobs
        self.scheduler.start()
        
        # Start continuous processors
        self.continuous.start()
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        
        # Keep main thread alive
        while True:
            time.sleep(1)

    def shutdown(self, signum, frame):
        """Graceful shutdown"""
        print("Shutting down ETL services...")
        self.scheduler.shutdown()
        self.continuous.stop()
        exit(0)

