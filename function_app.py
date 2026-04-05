import azure.functions as func
import logging
import os
from confluent_kafka import Consumer, KafkaError
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
    "group.id":          "azure-function-group",
    "auto.offset.reset": "earliest",
}

# ── Schema Registry config ────────────────────────────────────────────────────
SR_CONFIG = {
    "url": os.environ["SCHEMA_REGISTRY_URL"],
    "basic.auth.user.info": (
        f"{os.environ['SR_API_KEY']}:{os.environ['SR_API_SECRET']}"
    ),
}

TOPIC = "testorders"


def _build_deserializer() -> JSONDeserializer:
    """
    Creates a JSONDeserializer that fetches the schema automatically
    from Schema Registry using the schema ID in each message header.
    No hardcoded schema string needed.
    """
    sr_client = SchemaRegistryClient(SR_CONFIG)
    return JSONDeserializer(
        schema_str=None,                      # fetch from registry automatically
        schema_registry_client=sr_client,
        from_dict=lambda data, _ctx: data,    # return raw dict as-is
    )


@app.timer_trigger(
    schedule="*/10 * * * * *",
    arg_name="timer",
    run_on_startup=True,
)
def poll_kafka(timer: func.TimerRequest) -> None:
    """Runs every 10 seconds, polls Kafka, schema fetched from registry."""
    logging.info("Polling Kafka topic (JSON_SR — schema from registry)...")

    deserializer = _build_deserializer()
    consumer     = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])

    try:
        for _ in range(20):
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logging.error(f"Kafka error: {msg.error()}")
                continue

            record = deserializer(
                msg.value(),
                SerializationContext(TOPIC, MessageField.VALUE),
            )

            if record is None:
                logging.warning("Deserialiser returned None — skipping")
                continue

            # Unwrap JDBC envelope if present
            if "payload" in record:
                record = record["payload"]

            messages.append({
                "id":         record.get("id"),
                "customer":   record.get("customer"),
                "amount":     record.get("amount"),
                "updated_at": record.get("updated_at"),
            })

            logging.info(f"Consumed: {record}")

    except Exception as e:
        logging.error(f"Error during Kafka poll: {e}", exc_info=True)

    finally:
        consumer.close()

    if len(messages) > 100:
        messages[:] = messages[-100:]

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
        rows = '<tr><td colspan="4" style="text-align:center;color:#888">No messages yet — waiting for Kafka poll...</td></tr>'

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