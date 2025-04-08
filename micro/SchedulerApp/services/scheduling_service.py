from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import httpx
import asyncio
from config import CONFIG_SERVICE_URL, ORCHESTRATOR_SERVICE_URL
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

async def trigger_flow(flow_id: str, group_name: str):
    """Calls the Flow Orchestrator to start a specific flow."""
    url = f"{ORCHESTRATOR_SERVICE_URL}/api/v1/orchestrator/run/flow"
    payload = {"flow_id": flow_id, "group_name": group_name}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, json=payload, timeout=30.0) # Adjust timeout
            response.raise_for_status()
            logger.info(f"Successfully triggered flow {group_name}/{flow_id}. Response: {response.json()}")
        except httpx.RequestError as exc:
            logger.error(f"HTTP error triggering flow {group_name}/{flow_id}: {exc}")
        except Exception as exc:
             logger.error(f"Error triggering flow {group_name}/{flow_id}: {exc}")


async def trigger_group(group_name: str, flow_ids: list[str]):
    """Fetches flows for a group and triggers them."""
    logger.info(f"CRON triggered for group: {group_name}. Flows: {flow_ids}")
    # In a real scenario, you might fetch flow IDs dynamically here if needed,
    # but the schedule config likely lists the group_name and maybe associated flows.
    # Assuming flow_ids are passed correctly from the schedule job definition.
    tasks = [trigger_flow(flow_id, group_name) for flow_id in flow_ids]
    await asyncio.gather(*tasks)


async def load_and_schedule_jobs():
    """Fetches schedules from Config Service and adds/updates jobs."""
    logger.info("Loading schedules from Configuration Service...")
    url = f"{CONFIG_SERVICE_URL}/api/v1/config/schedules"
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            schedules = response.json().get("schedules", [])
            logger.info(f"Found {len(schedules)} schedules.")

            # Simple approach: remove all jobs and add new ones
            # More robust: Update existing jobs based on ID
            scheduler.remove_all_jobs()

            for schedule in schedules:
                job_id = schedule.get("RowKey") # Unique ID for the schedule job
                group_name = schedule.get("GroupName")
                cron_expression = schedule.get("CronExpression")
                # Assuming schedule config contains list of flows directly or indirectly
                flow_ids_str = schedule.get("FlowIDs") # e.g., "flow1,flow2"
                is_enabled = schedule.get("IsEnabled", False)

                if job_id and group_name and cron_expression and flow_ids_str and is_enabled:
                    flow_ids = [f.strip() for f in flow_ids_str.split(',')]
                    try:
                        scheduler.add_job(
                            trigger_group,
                            CronTrigger.from_crontab(cron_expression),
                            args=[group_name, flow_ids],
                            id=job_id,
                            name=f"Group_{group_name}",
                            replace_existing=True,
                        )
                        logger.info(f"Scheduled job '{job_id}' for group '{group_name}' with CRON '{cron_expression}'")
                    except Exception as e:
                        logger.error(f"Failed to schedule job '{job_id}': {e}")
                else:
                    logger.warning(f"Skipping invalid schedule config: {schedule}")

        except httpx.RequestError as exc:
            logger.error(f"Could not fetch schedules from Config Service: {exc}")
        except Exception as e:
             logger.error(f"Error processing schedules: {e}")

async def start_scheduler():
    await load_and_schedule_jobs() # Initial load
    # TODO: Add logic to periodically reload schedules if needed
    scheduler.start()
    logger.info("Scheduler started.")
