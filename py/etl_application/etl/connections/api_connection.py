import requests
from json import JSONDecodeError
from etl.connections.base_connection import BaseConnection

class APIConnection(BaseConnection, system_types=["API"]):
    def __init__(self):
        self.session = None
        self.config = None
        
    def connect(self, config: dict):
        self.config = config
        self.session = requests.Session()
        if 'auth' in config:
            self.session.auth = (config['auth']['user'], config['auth']['password'])
        if 'headers' in config:
            self.session.headers.update(config['headers'])
            
    def extract_data(self, endpoint: str = None):
        try:
            url = f"{self.config['base_url']}/{endpoint}" if endpoint else self.config['base_url']
            response = self.session.request(
                method=self.config.get('method', 'GET'),
                url=url,
                params=self.config.get('params'),
                data=self.config.get('data')
            )
            response.raise_for_status()
            
            content_type = response.headers.get('Content-Type', '')
            if 'json' in content_type:
                return response.json()
            elif 'xml' in content_type:
                return response.text
            elif 'csv' in content_type:
                return response.text
            return response.text
            
        except JSONDecodeError:
            return response.text
        except Exception as e:
            print(f"API Connection error: {str(e)}")
            raise
            
    def close(self):
        if self.session:
            self.session.close()
