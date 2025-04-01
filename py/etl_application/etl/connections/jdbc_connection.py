import jaydebeapi
from etl.connections.base_connection import BaseConnection

class JDBCConnection(BaseConnection, system_types=["JDBC"]):
    def __init__(self):
        self.conn = None
        self.jars = None
        
    def connect(self, config: dict):
        self.jars = config['jdbc_jars'].split(',')
        self.conn = jaydebeapi.connect(
            jclassname=config['jdbc_class'],
            url=config['jdbc_url'],
            driver_args={
                'user': config['user'],
                'password': config['password']
            },
            jars=self.jars
        )
        
    def extract_data(self, query: str):
        cursor = self.conn.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def close(self):
        if self.conn:
            self.conn.close()
