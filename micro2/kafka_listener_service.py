# --- kafka_listener_service/config.py ---
import os
from dotenv import load_dotenv

load_dotenv()

# URL for the Configuration Service
CONFIG_SERVICE_URL = os.getenv("CONFIG_SERVICE_URL")
# URL for the Flow Orchestrator Service
ORCHESTRATOR_SERVICE_URL = os.getenv("ORCHESTRATOR_SERVICE_URL")

# Kafka Configuration (from environment variables)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_GROUP_ID_PREFIX = os.getenv("KAFKA_GROUP_ID_PREFIX", "etl-kafka-listener")
# Example using SASL/PLAIN auth common with Confluent Cloud - adjust as needed!
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL") # Or PLAINTEXT, SASL_PLAINTEXT etc.
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME") # Often the API Key
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD") # Often the API Secret

# How often to reload listener configurations (in minutes)
LISTENER_REFRESH_MINUTES = int(os.getenv("LISTENER_REFRESH_MINUTES", "30"))

# Basic validation
if not CONFIG_SERVICE_URL or not ORCHESTRATOR_SERVICE_URL:
    print("Warning: CONFIG_SERVICE_URL or ORCHESTRATOR_SERVICE_URL not set.")
if not KAFKA_BOOTSTRAP_SERVERS:
    print("Warning: KAFKA_BOOTSTRAP_SERVERS not set.")
if "SASL" in KAFKA_SECURITY_PROTOCOL and (not KAFKA_SASL_USERNAME or not KAFKA_SASL_PASSWORD):
     print(f"Warning: Kafka security protocol is {KAFKA_SECURITY_PROTOCOL} but SASL username/password might be missing.")


# --- kafka_listener_service/services/kafka_listener.py ---
from confluent_kafka import Consumer, KafkaError, KafkaException
import asyncio
import httpx
import logging
import signal
import socket # For unique consumer ID suffix
from config import (
    CONFIG_SERVICE_URL, ORCHESTRATOR_SERVICE_URL, KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_MECHANISM, KAFKA_SASL_USERNAME,
    KAFKA_SASL_PASSWORD, KAFKA_GROUP_ID_PREFIX, LISTENER_REFRESH_MINUTES
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global state to manage running consumers
consumer_tasks = {} # Dictionary to hold { "topic_group_key": asyncio.Task }
running = asyncio.Event() # Event to signal consumers to stop gracefully
running.set() # Start in running state

async def trigger_flow_from_kafka(http_client: httpx.AsyncClient, flow_id: str, group_name: str, message_data: str):
    """Calls the Flow Orchestrator service to process a Kafka message."""
    if not ORCHESTRATOR_SERVICE_URL:
        logger.error("Orchestrator service URL not configured. Cannot trigger flow.")
        return

    url = f"{ORCHESTRATOR_SERVICE_URL}/api/v1/orchestrator/run/kafka_flow"
    payload = {
        "flow_id": flow_id,
        "group_name": group_name,
        "message_data": message_data # Pass raw message data as string
    }
    try:
        # Use a longer timeout for orchestrator calls initiated by Kafka?
        response = await http_client.post(url, json=payload, timeout=60.0)
        response.raise_for_status()
        logger.info(f"Successfully triggered Kafka flow {group_name}/{flow_id}. Orchestrator response: {response.json()}")
    except httpx.RequestError as exc:
        logger.error(f"Request error calling orchestrator for Kafka flow {group_name}/{flow_id}: {exc}")
    except httpx.HTTPStatusError as exc:
        logger.error(f"Orchestrator returned error for Kafka flow {group_name}/{flow_id}: Status {exc.response.status_code}, Response: {exc.response.text}")
    except Exception as exc:
        logger.error(f"Unexpected error triggering Kafka flow {group_name}/{flow_id}: {exc}")


async def consume_topic(consumer_config: dict, topic: str, flow_id: str, group_name: str):
    """Task function to continuously consume messages from a specific topic."""
    consumer = Consumer(consumer_config)
    logger.info(f"Consumer task starting for Topic: '{topic}', Flow: '{group_name}/{flow_id}', Group ID: '{consumer_config.get('group.id')}'")
    try:
        consumer.subscribe([topic])
        logger.info(f"Subscribed to topic '{topic}'")

        while running.is_set():
            msg = consumer.poll(timeout=1.0) # Poll Kafka for messages

            if msg is None:
                await asyncio.sleep(0.1) # Avoid tight loop when no messages
                continue # Go back to polling

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event - not necessarily an error. Ignore.
                    # logger.debug(f"Reached end of partition for {topic} [{msg.partition()}]")
                    pass
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                     logger.error(f"Kafka Error on topic {topic}: {msg.error()}. Topic might not exist yet or ACLs wrong.")
                     await asyncio.sleep(30) # Wait before retrying subscription/poll
                else:
                    # Log other Kafka errors
                    logger.error(f"Kafka Error on topic {topic}: {msg.error()}")
                    # Consider adding a delay or specific handling for certain errors
                    await asyncio.sleep(5)
            else:
                # Message successfully consumed
                try:
                    # Process the message
                    message_content = msg.value().decode('utf-8') # Assuming UTF-8 encoded messages
                    logger.info(f"Received message from topic '{topic}' (Flow: {group_name}/{flow_id})")
                    # Trigger the orchestrator asynchronously, don't block consumer
                    async with httpx.AsyncClient() as client:
                        asyncio.create_task(trigger_flow_from_kafka(client, flow_id, group_name, message_content))

                    # Optional: Manual commit if 'enable.auto.commit' is False
                    # try:
                    #     consumer.commit(asynchronous=False) # Or True
                    # except Exception as commit_err:
                    #     logger.error(f"Failed to commit offset for topic {topic}: {commit_err}")

                except UnicodeDecodeError:
                    logger.error(f"Failed to decode message value from topic '{topic}' as UTF-8. Message offset: {msg.offset()}")
                    # Decide how to handle non-UTF8 messages (e.g., skip, move to dead-letter queue)
                except Exception as e:
                    logger.error(f"Error processing message from topic '{topic}': {e}")
                    # Decide error handling (e.g., retry? dead-letter queue?)

            # Give other asyncio tasks a chance to run
            await asyncio.sleep(0.01)

    except KafkaException as e:
        logger.error(f"KafkaException in consumer for topic {topic}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in consumer task for topic {topic}: {e}")
    finally:
        logger.warning(f"Closing Kafka consumer for topic '{topic}'")
        consumer.close()
        logger.warning(f"Consumer task finished for topic '{topic}'")


async def load_and_manage_listeners():
    """Fetches listener configurations and starts/stops consumer tasks."""
    logger.info(f"Attempting to load Kafka listeners from {CONFIG_SERVICE_URL}...")
    if not CONFIG_SERVICE_URL or not KAFKA_BOOTSTRAP_SERVERS:
        logger.error("Config Service URL or Kafka Bootstrap Servers not set. Cannot manage listeners.")
        return

    listeners_config = []
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{CONFIG_SERVICE_URL}/api/v1/config/kafka_listeners?enabled_only=true", timeout=20.0)
            response.raise_for_status()
            listeners_config = response.json()
            logger.info(f"Successfully fetched {len(listeners_config)} enabled Kafka listeners.")
        except Exception as e:
            logger.error(f"Could not fetch Kafka listeners from Config Service: {e}")
            # Optionally: Keep existing listeners running if config fails? Or stop them?
            # For simplicity, we'll just log the error and proceed (listeners won't be updated).
            return

    fetched_listener_keys = set()
    hostname_suffix = socket.gethostname() # Add hostname for potentially more unique group.instance.id if needed

    for listener_conf in listeners_config:
        topic = listener_conf.get("TopicName")
        flow_id = listener_conf.get("FlowID")
        group_name = listener_conf.get("GroupName")

        if not all([topic, flow_id, group_name]):
            logger.warning(f"Skipping Kafka listener config with missing fields: {listener_conf.get('RowKey')}")
            continue

        # Create a unique key for managing this listener task
        listener_key = f"{topic}_{group_name}_{flow_id}"
        fetched_listener_keys.add(listener_key)

        if listener_key not in consumer_tasks or consumer_tasks[listener_key].done():
            if listener_key in consumer_tasks:
                logger.info(f"Restarting finished consumer task for: {listener_key}")
                del consumer_tasks[listener_key] # Remove finished task entry

            # Define Kafka Consumer Configuration
            consumer_group_id = f"{KAFKA_GROUP_ID_PREFIX}-{group_name}-{flow_id}"
            conf = {
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'group.id': consumer_group_id,
                # 'group.instance.id': f"{consumer_group_id}-{hostname_suffix}-{uuid.uuid4()}", # For static membership
                'auto.offset.reset': 'earliest', # Or 'latest'
                'enable.auto.commit': True, # Simpler, but potential for message loss/duplicates on failure
                # --- Security Settings ---
                'security.protocol': KAFKA_SECURITY_PROTOCOL,
                'sasl.mechanisms': KAFKA_SASL_MECHANISM,
                'sasl.username': KAFKA_SASL_USERNAME,
                'sasl.password': KAFKA_SASL_PASSWORD,
                # Add other necessary Kafka properties e.g. ssl.ca.location if using custom CA
            }
            # Remove None values from security config if not SASL
            if "SASL" not in KAFKA_SECURITY_PROTOCOL:
                conf.pop('sasl.mechanisms', None)
                conf.pop('sasl.username', None)
                conf.pop('sasl.password', None)
            if not KAFKA_SASL_USERNAME and 'sasl.username' in conf:
                 conf.pop('sasl.username')
                 conf.pop('sasl.password')
                 conf.pop('sasl.mechanisms')

            logger.info(f"Creating consumer task for: {listener_key}")
            task = asyncio.create_task(consume_topic(conf, topic, flow_id, group_name))
            consumer_tasks[listener_key] = task
        # else: logger.debug(f"Consumer task already running for: {listener_key}")


    # Stop and remove consumers for configurations that no longer exist or are disabled
    current_listener_keys = set(consumer_tasks.keys())
    listeners_to_stop = current_listener_keys - fetched_listener_keys
    for listener_key in listeners_to_stop:
        logger.warning(f"Listener configuration removed or disabled for: {listener_key}. Stopping consumer.")
        task = consumer_tasks.pop(listener_key) # Remove from tracking dict
        if task and not task.done():
            task.cancel() # Request cancellation
            # Don't await cancellation here to avoid blocking reload loop
            # Let the main shutdown handle final cleanup await

    logger.info("Listener management cycle complete.")

async def listener_reloader():
    """Periodically reloads listener configurations."""
    while running.is_set():
        await load_and_manage_listeners()
        try:
            # Wait for the refresh interval, but check the running flag frequently
            await asyncio.wait_for(running.wait(), timeout=LISTENER_REFRESH_MINUTES * 60)
            # If running.wait() completes, it means running was cleared (shutdown requested)
            break # Exit the loop if shutdown is signalled
        except asyncio.TimeoutError:
            # Timeout occurred, means refresh interval passed, continue loop
            pass
        except Exception as e:
             logger.error(f"Error in listener reloader loop: {e}")
             await asyncio.sleep(60) # Wait a bit before retrying on error

async def start_kafka_listeners():
    """Loads initial listeners and starts the periodic reloader task."""
    if not running.is_set():
         logger.warning("Startup called but service is already shutting down.")
         return
    logger.info("Starting Kafka Listener Service...")
    await load_and_manage_listeners() # Initial load
    asyncio.create_task(listener_reloader())
    logger.info(f"Kafka listener reload scheduled every {LISTENER_REFRESH_MINUTES} minutes.")

async def shutdown_kafka_listeners(signal_name=""):
    """Signals all consumer tasks to stop and waits for them to finish."""
    if not running.is_set():
         logger.info("Shutdown already in progress.")
         return
    logger.warning(f"Shutdown signal received ({signal_name}). Stopping Kafka consumers...")
    running.clear() # Signal all loops to stop

    tasks_to_wait = []
    for listener_key, task in consumer_tasks.items():
        if task and not task.done():
            logger.info(f"Requesting cancellation for task: {listener_key}")
            task.cancel()
            tasks_to_wait.append(task)

    if tasks_to_wait:
        logger.info(f"Waiting for {len(tasks_to_wait)} consumer tasks to finish...")
        # Wait for tasks to complete or cancel
        await asyncio.gather(*tasks_to_wait, return_exceptions=True)
        logger.info("All active consumer tasks have been processed.")
    else:
        logger.info("No active consumer tasks to wait for.")

    consumer_tasks.clear()
    logger.warning("Kafka Listener Service shutdown complete.")

# --- kafka_listener_service/main.py ---
from fastapi import FastAPI
import asyncio
import logging
import signal
from services.kafka_listener import start_kafka_listeners, shutdown_kafka_listeners, consumer_tasks, running

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="ETL Kafka Listener Service",
    description="Listens to Kafka topics and triggers ETL flows.",
    version="1.0.0"
)

# Handle OS signals for graceful shutdown
def handle_signal(sig, frame):
    logger.warning(f"Received signal {sig}. Initiating graceful shutdown.")
    # Run shutdown in a way that doesn't block the signal handler
    asyncio.create_task(shutdown_kafka_listeners(signal.Signals(sig).name))

@app.on_event("startup")
async def startup_event():
    logger.info("Kafka Listener Service starting up...")
    # Set signal handlers
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal, sig, None)

    # Start the main listener logic
    asyncio.create_task(start_kafka_listeners())

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("FastAPI shutdown event triggered.")
    # Ensure graceful shutdown is called if not already triggered by signal
    await shutdown_kafka_listeners("FastAPI Shutdown")

@app.get("/health", tags=["Health"])
def read_health():
    """Health check endpoint."""
    # Healthy if the main running flag is set
    if running.is_set():
         # Could add more checks, like verifying config service reachability
        return {"status": "healthy", "active_listeners": len(consumer_tasks)}
    else:
        return {"status": "unhealthy", "reason": "Service is shutting down or failed"}

@app.post("/reload_listeners", status_code=202, tags=["Actions"])
async def reload_listeners_endpoint():
    """Manually triggers a reload of listener configurations."""
    if not running.is_set():
         raise HTTPException(status_code=503, detail="Service is shutting down.")
    logger.info("Manual listener reload requested via API.")
    # Run reload in background
    from services.kafka_listener import load_and_manage_listeners
    asyncio.create_task(load_and_manage_listeners())
    return {"message": "Listener reload initiated."}

@app.get("/listeners", tags=["Debug"])
def get_active_listeners():
    """Lists the currently active listener tasks (for debugging)."""
    active = {}
    for key, task in consumer_tasks.items():
        active[key] = {"done": task.done(), "cancelled": task.cancelled()}
    return {"active_listeners": active, "running_state": running.is_set()}


@app.get("/", tags=["Root"])
def read_root():
    """Root endpoint providing basic service info."""
    return {"message": "Welcome to the ETL Kafka Listener Service"}

if __name__ == "__main__":
    import uvicorn
    # Run directly for local testing
    # Note: Graceful shutdown handling might be less reliable with --reload
    uvicorn.run(app, host="0.0.0.0", port=8002)


# --- kafka_listener_service/requirements.txt ---
fastapi
uvicorn[standard]
python-dotenv
httpx # For API calls to orchestrator
confluent-kafka # Kafka client library

# --- kafka_listener_service/Dockerfile ---
# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables for Python
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Install dependencies
# librdkafka-dev is needed for confluent-kafka C extensions
RUN apt-get update && apt-get install -y --no-install-recommends librdkafka-dev gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the port the app runs on
EXPOSE 80

# Command to run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
