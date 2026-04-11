import azure.functions as func
import logging
import os
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

app = func.FunctionApp()

messages = []

# ── Kafka consumer config ─────────────────────────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":   "PLAIN",
    "sasl.username":     os.environ["KAFKA_API_KEY"],
    "sasl.password":     os.environ["KAFKA_API_SECRET"],
    "group.id":          os.getenv("KAFKA_CONSUMER_GROUP", "azure-function-group_1"),
    "auto.offset.reset": "earliest",
}

# ── Schema Registry config ────────────────────────────────────────────────────
SR_CONFIG = {
    "url": os.environ["SCHEMA_REGISTRY_URL"],
    "basic.auth.user.info": (
        f"{os.environ['SR_API_KEY']}:{os.environ['SR_API_SECRET']}"
    ),
}

TOPIC = os.getenv("KAFKA_TOPIC", "testorders")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "azure-function-group_1")
USERNAME = os.getenv("KAFKA_API_KEY")
PASSWORD = os.getenv("KAFKA_API_SECRET")
PROTOCOL = os.getenv("KAFKA_PROTOCOL", "SASL_SSL")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
SR_API_KEY = os.getenv("SR_API_KEY")
SR_API_SECRET = os.getenv("SR_API_SECRET")


def _build_deserializer() -> JSONDeserializer:
    """
    Creates a JSONDeserializer that fetches the schema automatically
    from Schema Registry using the schema ID in each message header.
    No hardcoded schema string needed.
    """
    sr_client = SchemaRegistryClient(SR_CONFIG)
    return JSONDeserializer(
        schema_str=None,
        schema_registry_client=sr_client,
        from_dict=lambda data, _ctx: data,
    )


@app.kafka_trigger(
    arg_name="msg",
    topic=TOPIC,
    broker_list=BOOTSTRAP,
    consumer_group=CONSUMER_GROUP,
    username=USERNAME,
    password=PASSWORD,
    protocol=PROTOCOL,
    data_type="binary",
    schema_registry_url=SCHEMA_REGISTRY_URL,
    schema_registry_username=SR_API_KEY,
    schema_registry_password=SR_API_SECRET,
)
def kafka_handler(msg: func.KafkaEvent) -> None:
    """Kafka-triggered function that processes Kafka messages with Schema Registry."""
    logging.info(
        "Kafka event received: topic=%s partition=%s offset=%s key=%s",
        msg.topic,
        msg.partition,
        msg.offset,
        msg.key,
    )

    try:
        deserializer = _build_deserializer()
        record = deserializer(
            msg.get_body(),
            SerializationContext(TOPIC, MessageField.VALUE),
        )

        if record is None:
            logging.warning("Deserializer returned None — skipping")
            return

        if "payload" in record:
            record = record["payload"]

        if not isinstance(record, dict):
            logging.warning("Kafka payload is not a JSON object; skipping: %s", record)
            return

        messages.append({
            "id":         record.get("id"),
            "customer":   record.get("customer"),
            "amount":     record.get("amount"),
            "updated_at": record.get("updated_at"),
        })

        if len(messages) > 100:
            messages[:] = messages[-100:]

        logging.info("Consumed Kafka record: %s", record)

    except Exception as e:
        logging.error("Failed to deserialize Kafka message: %s", e, exc_info=True)


@app.route(route="orders", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def show_orders(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP endpoint — returns an HTML page showing consumed messages."""
    rows = ""
    for m in reversed(messages):
        rows += f"""
        <tr>
            <td>{m.get('id', '')}</td>
            <td>{m.get('customer', '')}</td>
            <td>{m.get('amount', '')}</td>
            <td>{m.get('updated_at', '')}</td>
        </tr>"""

    if not rows:
        rows = '<tr><td colspan="4" style="text-align:center;color:#888">No messages yet — waiting for Kafka events...</td></tr>'

    html = f"""<!DOCTYPE html>
<html>
<head>
  <title>Kafka Orders</title>
  <meta http-equiv="refresh" content="10">
  <style>
    body {{ font-family: sans-serif; padding: 2rem; }}
    h1   {{ font-size: 1.4rem; margin-bottom: 1rem; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px 12px; text-align: left; }}
    th {{ background: #f0f0f0; font-weight: 500; }}
    tr:nth-child(even) {{ background: #fafafa; }}
    .meta {{ font-size: 0.8rem; color: #888; margin-top: 1rem; }}
  </style>
</head>
<body>
  <h1>Kafka topic: {TOPIC}</h1>
  <table>
    <thead>
      <tr><th>ID</th><th>Customer</th><th>Amount</th><th>Updated at</th></tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>
  <p class="meta">Showing {len(messages)} message(s). Page auto-refreshes every 10 seconds.</p>
</body>
</html>"""

    return func.HttpResponse(html, mimetype="text/html")
