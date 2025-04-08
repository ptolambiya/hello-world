from confluent_kafka import Consumer, KafkaError, KafkaException
import asyncio
import httpx
import json
import logging
from config import (
    CONFIG_SERVICE_URL, ORCHESTRATOR_SERVICE_URL, KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD, KAFKA_GROUP_ID_PREFIX
) # Adapt auth as needed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variable to hold consumer tasks, allows stopping them
consumer_tasks = {}
running = True

async def trigger_flow_from_kafka(flow_id: str, group_name: str, message_data: str):
    """Calls the Flow Orchestrator for a Kafka message."""
    url = f"{ORCHESTRATOR_SERVICE_URL}/api/v1/orchestrator/run/kafka_flow"
    payload = {
        "flow_id": flow_id,
        "group_name": group_name, # Need group_name associated with the flow
        "message_data": message_data
    }
    async with httpx.AsyncClient() as client:
        try:
            logger.debug(f"Triggering Kafka flow {group_name}/{flow_id}")
            response = await client.post(url, json=payload, timeout=60.0) # Longer timeout?
            response.raise_for_status()
            logger.info(f"Successfully triggered Kafka flow {group_name}/{flow_id}. Response: {response.json()}")
        except httpx.RequestError as exc:
            logger.error(f"HTTP error triggering Kafka flow {group_name}/{flow_id}: {exc}")
        except Exception as exc:
             logger.error(f"Error triggering Kafka flow {group_name}/{flow_id}: {exc}")


async def consume_topic(consumer_conf, topic: str, flow_id: str, group_name: str):
    """Consumes messages from a specific topic."""
    consumer = Consumer(consumer_conf)
    try:
        consumer.subscribe([topic])
        logger.info(f"Subscribed to topic '{topic}' for flow '{group_name}/{flow_id}'")
        while running:
            msg = consumer.poll(timeout=1.0) # Poll for messages
            if msg is None:
                await asyncio.sleep(0.1) # Avoid busy-waiting
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Reached end of partition for {topic}")
                elif msg.error():
                    logger.error(f"Kafka error on topic {topic}: {msg.error()}")
                    # Consider adding a delay or break mechanism on persistent errors
                    await asyncio.sleep(5)
            else:
                try:
                    message_content = msg.value().decode('utf-8')
                    logger.info(f"Received message on {topic} for flow {group_name}/{flow_id}")
                    # Run trigger in background, don't block consumer loop
                    asyncio.create_task(trigger_flow_from_kafka(flow_id, group_name, message_content))
                    # Manual commit needed if enable.auto.commit=False
                    # consumer.commit(asynchronous=True)
                except Exception as e:
                    logger.error(f"Error processing message from {topic}: {e}")
                    # Decide how to handle poison pills/bad messages

            await asyncio.sleep(0.01) # Yield control briefly

    except KafkaException as e:
         logger.error(f"Kafka configuration/subscription error for topic {topic}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in consumer for topic {topic}: {e}")
    finally:
        logger.info(f"Closing consumer for topic {topic}")
        consumer.close()


async def load_and_start_listeners():
    """Fetches Kafka listener configs and starts consumers."""
    global running
    running = True
    logger.info("Loading Kafka listeners from Configuration Service...")
    url = f"{CONFIG_SERVICE_URL}/api/v1/config/kafka_listeners"
    listeners = []
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            listeners = response.json().get("listeners", [])
            logger.info(f"Found {len(listeners)} Kafka listeners.")
    except Exception as e:
        logger.error(f"Could not fetch Kafka listeners: {e}")
        return # Cannot proceed without config

    # Stop existing consumers before starting new ones
    await stop_listeners()

    for listener_conf in listeners:
        topic = listener_conf.get("TopicName")
        flow_id = listener_conf.get("FlowID")
        group_name = listener_conf.get("GroupName") # Ensure GroupName is in config table
        is_enabled = listener_conf.get("IsEnabled", False)
        # Unique group.id per listener instance/deployment potentially
        consumer_group_id = f"{KAFKA_GROUP_ID_PREFIX}-{group_name}-{flow_id}"

        if topic and flow_id and group_name and is_enabled:
             # Basic Kafka Consumer Config (Adapt security protocol as needed)
             conf = {
                 'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                 'group.id': consumer_group_id,
                 'security.protocol': 'SASL_SSL', # Example, adjust based on Confluent Cloud setup
                 'sasl.mechanisms': 'PLAIN',
                 'sasl.username': KAFKA_SASL_USERNAME,
                 'sasl.password': KAFKA_SASL_PASSWORD,
                 'auto.offset.reset': 'earliest', # Or 'latest'
                 'enable.auto.commit': True # Set to False for manual commits
                 # Add other relevant Kafka consumer properties
             }
             logger.info(f"Starting consumer task for topic '{topic}', flow '{group_name}/{flow_id}'")
             task = asyncio.create_task(consume_topic(conf, topic, flow_id, group_name))
             consumer_tasks[f"{group_name}/{flow_id}"] = task
        else:
            logger.warning(f"Skipping invalid Kafka listener config: {listener_conf}")

async def stop_listeners():
    """Stops all running consumer tasks."""
    global running
    running = False
    logger.info(f"Stopping {len(consumer_tasks)} Kafka consumers...")
    for task_id, task in consumer_tasks.items():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            logger.info(f"Consumer task {task_id} cancelled.")
        except Exception as e:
            logger.error(f"Error stopping task {task_id}: {e}")
    consumer_tasks.clear()
    logger.info("All Kafka consumers stopped.")
