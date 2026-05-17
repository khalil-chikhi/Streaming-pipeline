import json
import logging
import requests
from kafka import KafkaProducer
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("wikimedia-producer")

KAFKA_BOOTSTRAP_SERVERS = "wikimedia-kafka-khalilchikhi018-9584.c.aivencloud.com:13171"
KAFKA_TOPIC = "wikimedia-recentchange"
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

SSL_CAFILE = "ca.pem"
SSL_CERTFILE = "service.cert"
SSL_KEYFILE = "service.key"

def build_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        security_protocol="SSL",
        ssl_cafile=SSL_CAFILE,
        ssl_certfile=SSL_CERTFILE,
        ssl_keyfile=SSL_KEYFILE,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        api_version=(2, 6),
    )


def stream_to_kafka(producer):
    while True:
        try:
            logger.info("Connecting to Wikimedia stream...")
            response = requests.get(STREAM_URL, stream=True, headers={
                "Accept": "text/event-stream",
                "Cache-Control": "no-cache",
                "User-Agent": "wikimedia-kafka-producer/1.0"
            },timeout=None)
            
            response.raise_for_status()

            count = 0
            for raw_line in response.iter_lines(decode_unicode=True):
                if not raw_line or not raw_line.startswith("data:"):
                    continue
                try:
                    data = json.loads(raw_line[5:].strip())
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
        except Exception as e:
            logger.warning("Stream disconnected: %s. Reconnecting...", e)
            time.sleep(5)

if __name__ == "__main__":
    producer = build_producer()
    logger.info("Kafka producer ready.")
    try:
        stream_to_kafka(producer)
    except KeyboardInterrupt:
        logger.info("Shutting down — flushing producer.")
        producer.flush()
        producer.close()