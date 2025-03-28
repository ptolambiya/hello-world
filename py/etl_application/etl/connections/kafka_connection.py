from kafka import KafkaConsumer
import json
from etl.connections.base_connection import BaseConnection
from typing import List, Dict

class KafkaConnection(BaseConnection):
    def __init__(self, batch_size=1000, timeout_ms=5000):
        self.consumer = None
        self.batch_size = batch_size
        self.timeout_ms = timeout_ms
        
    def connect(self, config: dict):
        self.consumer = KafkaConsumer(
            config['topic'],
            bootstrap_servers=config['bootstrap_servers'],
            group_id=config['group_id'],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
    def extract_data(self, timeout_ms: int = 5000):
        messages = []
        for _ in range(self.batch_size):
            batch = self.consumer.poll(timeout_ms=self.timeout_ms)
            for _, records in batch.items():
                messages.extend([msg.value for msg in records])
        return messages

    def commit_offset(self):
        """Manually commit offsets"""
        self.consumer.commit()
            
    def close(self):
        if self.consumer:
            self.consumer.close()
