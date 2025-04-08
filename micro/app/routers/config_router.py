from fastapi import APIRouter, HTTPException
from services.table_service import config_service

router = APIRouter()

@router.get("/flows/{group_name}/{flow_id}")
async def read_flow(group_name: str, flow_id: str):
    flow = config_service.get_flow_details(flow_id, group_name)
    if flow is None:
        raise HTTPException(status_code=404, detail="Flow not found")
    return flow

@router.get("/groups/{group_name}/flows")
async def read_group_flows(group_name: str):
    flows = config_service.get_flows_by_group(group_name)
    return {"flows": flows}

@router.get("/schedules")
async def read_schedules():
    schedules = config_service.get_schedules()
    return {"schedules": schedules}

@router.get("/kafka_listeners")
async def read_kafka_listeners():
    listeners = config_service.get_kafka_listeners()
    return {"listeners": listeners}

# --- Add endpoints for other configurations ---
