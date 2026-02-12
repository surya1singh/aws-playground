import os, json, random, uuid, time
from datetime import datetime, timedelta, timezone
from faker import Faker
from confluent_kafka import Producer

fake = Faker()
random.seed(7)

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "order_events")

RATE_PER_SEC = float(os.environ.get("RATE_PER_SEC", "20"))
DUP_PROB = float(os.environ.get("DUP_PROB", "0.01"))
LATE_PROB = float(os.environ.get("LATE_PROB", "0.02"))
LATE_MAX_MIN = int(os.environ.get("LATE_MAX_MIN", "30"))

p = Producer({"bootstrap.servers": BOOTSTRAP})
recent = []  # for exact duplicates

def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def maybe_late(ts: datetime) -> datetime:
    if random.random() < LATE_PROB:
        return ts - timedelta(minutes=random.randint(1, LATE_MAX_MIN))
    return ts

def build_event():
    now = datetime.now(timezone.utc)
    now = maybe_late(now)
    order_id = f"o{random.randint(1, 5_000_000)}"
    et = random.choice(["OrderPlaced", "OrderPaid", "OrderShipped", "OrderDelivered", "OrderCancelled"])
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": et,
        "event_ts": iso(now),
        "order_id": order_id,
        "user_id": f"u{random.randint(1, 200_000)}",
        "amount": round(random.uniform(5, 800), 2),
        "payment_method": random.choice(["card", "upi", "paypal"]),
        "status": et.replace("Order", "").upper()
    }

def maybe_dup(payload: str) -> str:
    if random.random() < DUP_PROB and recent:
        return random.choice(recent)
    recent.append(payload)
    if len(recent) > 10000:
        del recent[:5000]
    return payload

def dr(err, msg):
    if err:
        print("Delivery failed:", err)

def main():
    print("Producing to:", BOOTSTRAP, "topic:", TOPIC)
    while True:
        e = build_event()
        payload = json.dumps(e)
        payload = maybe_dup(payload)
        key = e.get("event_id", str(uuid.uuid4()))
        p.produce(TOPIC, key=key.encode(), value=payload.encode(), callback=dr)
        p.poll(0)
        time.sleep(1.0 / RATE_PER_SEC)

if __name__ == "__main__":
    main()
