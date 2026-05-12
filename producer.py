"""
Wikimedia Recent Changes → Kafka Producer
------------------------------------------
Connects to the Wikimedia SSE stream and publishes
every edit event to a Kafka topic as JSON.
"""

import json
import logging
import sseclient
import requests
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("wikimedia-producer")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "wikimedia-recentchange"
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


def build_producer():
    return KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        api_version=(2, 6),
    )


def stream_to_kafka(producer):
    logger.info("Connecting to Wikimedia stream...")
    
    headers = {
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
        "User-Agent": "wikimedia-kafka-producer/1.0"
    }
    
    response = requests.get(STREAM_URL, stream=True, headers=headers, timeout=None)
    response.raise_for_status()

    count = 0
    for raw_line in response.iter_lines(decode_unicode=True):
        if not raw_line or not raw_line.startswith("data:"):
            continue
        try:
            data = json.loads(raw_line[5:].strip())
            
            if count == 0:
                logger.info("First event: %s", str(data)[:200])

            producer.send(
                KAFKA_TOPIC,
                key=str(data.get("id", "")),
                value=data,
            )
            count += 1
            if count % 100 == 0:
                producer.flush()
                logger.info("Published %d events.", count)

        except Exception as e:
            logger.warning("Skipping: %s", e)

if __name__ == "__main__":
    producer = build_producer()
    logger.info("Kafka producer ready.")
    try:
        stream_to_kafka(producer)
    except KeyboardInterrupt:
        logger.info("Shutting down — flushing producer.")
        producer.flush()
        producer.close()