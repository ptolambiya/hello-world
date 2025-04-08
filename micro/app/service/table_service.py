from azure.data.tables import TableServiceClient, TableEntity
from config import AZURE_TABLE_STORAGE_CONN_STR
import json

class ConfigTableService:
    def __init__(self, connection_string: str):
        self.service_client = TableServiceClient.from_connection_string(conn_str=connection_string)

    def get_table_client(self, table_name: str):
        return self.service_client.get_table_client(table_name)

    def get_flow_details(self, flow_id: str, group_name: str) -> dict | None:
        table_client = self.get_table_client("Flows")
        try:
            entity = table_client.get_entity(partition_key=group_name, row_key=flow_id)
            # Deserialize JSON string properties if needed
            entity['SourceConfig'] = json.loads(entity.get('SourceConfig', '{}'))
            entity['DestinationConfig'] = json.loads(entity.get('DestinationConfig', '{}'))
            entity['MappingConfig'] = json.loads(entity.get('MappingConfig', '{}'))
            return entity
        except Exception as e: # Be more specific in production (ResourceNotFoundError)
            print(f"Error getting flow {flow_id}: {e}")
            return None

    def get_flows_by_group(self, group_name: str) -> list[dict]:
         table_client = self.get_table_client("Flows")
         entities = table_client.query_entities(query_filter=f"PartitionKey eq '{group_name}'")
         flows = []
         for entity in entities:
             entity['SourceConfig'] = json.loads(entity.get('SourceConfig', '{}'))
             entity['DestinationConfig'] = json.loads(entity.get('DestinationConfig', '{}'))
             entity['MappingConfig'] = json.loads(entity.get('MappingConfig', '{}'))
             flows.append(entity)
         return flows

    # --- Add methods for other tables (Groups, Mappings, Schedules, KafkaListeners) ---
    def get_schedules(self) -> list[dict]:
        table_client = self.get_table_client("Schedules")
        entities = table_client.list_entities() # Get all schedules
        # Add parsing if needed
        return list(entities)

    def get_kafka_listeners(self) -> list[dict]:
        table_client = self.get_table_client("KafkaListeners")
        entities = table_client.list_entities() # Get all listeners
        # Add parsing if needed
        return list(entities)

config_service = ConfigTableService(AZURE_TABLE_STORAGE_CONN_STR)
