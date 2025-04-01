from kafka import KafkaConsumer
import json
import logging

# Configure logging to see Kafka client debug info (optional)
logging.basicConfig(level=logging.INFO)

def consume_messages():
    # Configuration
    KAFKA_BROKERS = 'localhost:9092'  # Comma-separated for multiple brokers
    TOPIC_NAME = 'etltest1'
    GROUP_ID = 'my-consumer-group'
    
    try:
        # Create consumer instance
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BROKERS,
            auto_offset_reset='earliest',  # Start from beginning if no offset
            group_id=GROUP_ID,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=True,  # Periodically commit offsets
            security_protocol='PLAINTEXT'  # Use 'SSL' or 'SASL_SSL' for secure clusters
        )

        print(f"Subscribed to topic '{TOPIC_NAME}'. Waiting for messages...")
        
        # Poll for messages indefinitely
        for message in consumer:
            print("\n--- New Message ---")
            print(f"Topic: {message.topic}")
            print(f"Partition: {message.partition}")
            print(f"Offset: {message.offset}")
            print(f"Key: {message.key}")  # Message key (if provided)
            print(f"Value: {message.value}")  # Deserialized message body
            
    except KeyboardInterrupt:
        print("\nConsumer stopped by user")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if 'consumer' in locals():
            consumer.close()
            print("Consumer closed gracefully")

if __name__ == "__main__":
    consume_messages()