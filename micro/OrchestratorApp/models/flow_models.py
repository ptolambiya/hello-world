from pydantic import BaseModel

class FlowRunRequest(BaseModel):
    flow_id: str
    group_name: str

class KafkaFlowRunRequest(FlowRunRequest):
    message_data: str # Raw message data as string
