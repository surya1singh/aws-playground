import os, random, uuid
from datetime import datetime, timedelta, timezone
from faker import Faker
from sqlalchemy import create_engine, text

fake = Faker()
random.seed(7)

DDL = """
CREATE TABLE IF NOT EXISTS customers (
  customer_id TEXT PRIMARY KEY,
  email TEXT,
  phone TEXT,
  full_name TEXT,
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS addresses (
  address_id TEXT PRIMARY KEY,
  customer_id TEXT,
  line1 TEXT, city TEXT, state TEXT, zip TEXT, country TEXT,
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS products (
  product_id TEXT PRIMARY KEY,
  sku TEXT,
  category TEXT,
  price NUMERIC(10,2),
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS merchants (
  merchant_id TEXT PRIMARY KEY,
  name TEXT,
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS orders (
  order_id TEXT PRIMARY KEY,
  customer_id TEXT,
  merchant_id TEXT,
  order_status TEXT,
  order_ts TIMESTAMPTZ,
  updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS order_items (
  order_id TEXT,
  line_no INT,
  product_id TEXT,
  qty INT,
  unit_price NUMERIC(10,2),
  PRIMARY KEY(order_id, line_no)
);

CREATE TABLE IF NOT EXISTS payments (
  payment_id TEXT PRIMARY KEY,
  order_id TEXT,
  payment_method TEXT,
  amount NUMERIC(10,2),
  payment_status TEXT,
  payment_ts TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS shipments (
  shipment_id TEXT PRIMARY KEY,
  order_id TEXT,
  carrier TEXT,
  ship_ts TIMESTAMPTZ,
  delivery_ts TIMESTAMPTZ,
  status TEXT
);

CREATE TABLE IF NOT EXISTS returns (
  return_id TEXT PRIMARY KEY,
  order_id TEXT,
  reason TEXT,
  refund_amount NUMERIC(10,2),
  return_ts TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS inventory (
  product_id TEXT,
  warehouse_id TEXT,
  on_hand_qty INT,
  reserved_qty INT,
  updated_at TIMESTAMPTZ,
  PRIMARY KEY(product_id, warehouse_id)
);

CREATE TABLE IF NOT EXISTS promotions (
  promo_id TEXT PRIMARY KEY,
  code TEXT,
  discount_pct INT,
  starts_at TIMESTAMPTZ,
  ends_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS reviews (
  review_id TEXT PRIMARY KEY,
  order_id TEXT,
  rating INT,
  review_text TEXT,
  created_at TIMESTAMPTZ
);
"""

def utc_now():
    return datetime.now(timezone.utc)

def conn():
    # Example: postgresql+psycopg2://user:pass@host:5432/db
    return os.environ["PG_DSN"]

def upsert_inventory(engine, product_id, warehouse_id, on_hand, reserved, ts):
    engine.execute(text("""
        INSERT INTO inventory(product_id, warehouse_id, on_hand_qty, reserved_qty, updated_at)
        VALUES (:p, :w, :onh, :res, :ts)
        ON CONFLICT(product_id, warehouse_id)
        DO UPDATE SET on_hand_qty=EXCLUDED.on_hand_qty, reserved_qty=EXCLUDED.reserved_qty, updated_at=EXCLUDED.updated_at
    """), {"p": product_id, "w": warehouse_id, "onh": on_hand, "res": reserved, "ts": ts})

def seed(engine, n_customers=5000, n_products=2000, n_orders=15000):
    now = utc_now()

    # customers + addresses
    for i in range(n_customers):
        cid = f"c{uuid.uuid4().hex[:12]}"
        created = now - timedelta(days=random.randint(10, 500))
        updated = created + timedelta(days=random.randint(0, 20))
        engine.execute(text("""
            INSERT INTO customers(customer_id,email,phone,full_name,created_at,updated_at)
            VALUES (:cid,:email,:phone,:name,:c,:u)
            ON CONFLICT(customer_id) DO NOTHING
        """), {"cid": cid, "email": fake.email(), "phone": fake.msisdn()[:15], "name": fake.name(), "c": created, "u": updated})

        aid = f"a{uuid.uuid4().hex[:12]}"
        engine.execute(text("""
            INSERT INTO addresses(address_id,customer_id,line1,city,state,zip,country,created_at,updated_at)
            VALUES (:aid,:cid,:l1,:city,:st,:zip,:cty,:c,:u)
            ON CONFLICT(address_id) DO NOTHING
        """), {"aid": aid, "cid": cid, "l1": fake.street_address(), "city": fake.city(),
               "st": fake.state_abbr(), "zip": fake.postcode(), "cty": "US", "c": created, "u": updated})

    # merchants
    merchant_ids = []
    for _ in range(100):
        mid = f"m{uuid.uuid4().hex[:10]}"
        merchant_ids.append(mid)
        created = now - timedelta(days=random.randint(30, 900))
        engine.execute(text("""
            INSERT INTO merchants(merchant_id,name,created_at,updated_at)
            VALUES (:mid,:name,:c,:u) ON CONFLICT(merchant_id) DO NOTHING
        """), {"mid": mid, "name": fake.company(), "c": created, "u": created})

    # products
    product_ids = []
    categories = ["electronics", "fashion", "home", "sports", "beauty", "grocery"]
    for _ in range(n_products):
        pid = f"p{uuid.uuid4().hex[:12]}"
        product_ids.append(pid)
        created = now - timedelta(days=random.randint(10, 900))
        engine.execute(text("""
            INSERT INTO products(product_id,sku,category,price,created_at,updated_at)
            VALUES (:pid,:sku,:cat,:price,:c,:u) ON CONFLICT(product_id) DO NOTHING
        """), {"pid": pid, "sku": fake.bothify("SKU-#######"), "cat": random.choice(categories),
               "price": round(random.uniform(3, 800), 2), "c": created, "u": created})

    # inventory (stateful)
    warehouses = ["w1", "w2", "w3", "w4"]
    for pid in random.sample(product_ids, k=min(len(product_ids), 1200)):
        for w in random.sample(warehouses, k=random.randint(1, len(warehouses))):
            upsert_inventory(engine, pid, w, random.randint(0, 500), random.randint(0, 50), now)

    # orders + items + payments + shipments + returns + reviews
    customer_ids = [r[0] for r in engine.execute(text("SELECT customer_id FROM customers LIMIT 200000")).fetchall()]

    for _ in range(n_orders):
        oid = f"o{uuid.uuid4().hex[:14]}"
        cid = random.choice(customer_ids)
        mid = random.choice(merchant_ids)
        order_ts = now - timedelta(days=random.randint(0, 90), minutes=random.randint(0, 1440))
        status = random.choice(["PLACED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"])
        engine.execute(text("""
            INSERT INTO orders(order_id,customer_id,merchant_id,order_status,order_ts,updated_at)
            VALUES (:oid,:cid,:mid,:st,:ts,:u) ON CONFLICT(order_id) DO NOTHING
        """), {"oid": oid, "cid": cid, "mid": mid, "st": status, "ts": order_ts, "u": order_ts})

        n_lines = random.randint(1, 5)
        for ln in range(1, n_lines + 1):
            pid = random.choice(product_ids)
            qty = random.randint(1, 3)
            price = float(engine.execute(text("SELECT price FROM products WHERE product_id=:p"), {"p": pid}).scalar())
            engine.execute(text("""
                INSERT INTO order_items(order_id,line_no,product_id,qty,unit_price)
                VALUES (:oid,:ln,:pid,:qty,:up) ON CONFLICT(order_id,line_no) DO NOTHING
            """), {"oid": oid, "ln": ln, "pid": pid, "qty": qty, "up": price})

        if status in ["PAID", "SHIPPED", "DELIVERED"]:
            pay_id = f"pay{uuid.uuid4().hex[:12]}"
            amount = round(random.uniform(10, 500), 2)
            engine.execute(text("""
                INSERT INTO payments(payment_id,order_id,payment_method,amount,payment_status,payment_ts)
                VALUES (:pid,:oid,:pm,:amt,:ps,:ts) ON CONFLICT(payment_id) DO NOTHING
            """), {"pid": pay_id, "oid": oid, "pm": random.choice(["card", "upi", "paypal"]),
                   "amt": amount, "ps": "SUCCESS", "ts": order_ts + timedelta(minutes=random.randint(1, 30))})

        if status in ["SHIPPED", "DELIVERED"]:
            sid = f"shp{uuid.uuid4().hex[:12]}"
            ship_ts = order_ts + timedelta(hours=random.randint(2, 48))
            del_ts = ship_ts + timedelta(hours=random.randint(12, 96)) if status == "DELIVERED" else None
            engine.execute(text("""
                INSERT INTO shipments(shipment_id,order_id,carrier,ship_ts,delivery_ts,status)
                VALUES (:sid,:oid,:car,:st,:dt,:ss) ON CONFLICT(shipment_id) DO NOTHING
            """), {"sid": sid, "oid": oid, "car": random.choice(["ups", "fedex", "usps"]),
                   "st": ship_ts, "dt": del_ts, "ss": "IN_TRANSIT" if del_ts is None else "DELIVERED"})

        if status == "DELIVERED" and random.random() < 0.08:
            rid = f"ret{uuid.uuid4().hex[:12]}"
            engine.execute(text("""
                INSERT INTO returns(return_id,order_id,reason,refund_amount,return_ts)
                VALUES (:rid,:oid,:rs,:amt,:ts) ON CONFLICT(return_id) DO NOTHING
            """), {"rid": rid, "oid": oid, "rs": random.choice(["damaged", "wrong_item", "late"]),
                   "amt": round(random.uniform(5, 200), 2), "ts": order_ts + timedelta(days=random.randint(1, 10))})

        if status == "DELIVERED" and random.random() < 0.20:
            rev = f"rev{uuid.uuid4().hex[:12]}"
            engine.execute(text("""
                INSERT INTO reviews(review_id,order_id,rating,review_text,created_at)
                VALUES (:rid,:oid,:rt,:txt,:ts) ON CONFLICT(review_id) DO NOTHING
            """), {"rid": rev, "oid": oid, "rt": random.randint(1, 5), "txt": fake.sentence(),
                   "ts": order_ts + timedelta(days=random.randint(0, 7))})

    print("Seed complete.")

if __name__ == "__main__":
    eng = create_engine(conn(), future=True)
    with eng.begin() as cxn:
        cxn.execute(text(DDL))
        seed(cxn,
             n_customers=int(os.environ.get("N_CUSTOMERS", "5000")),
             n_products=int(os.environ.get("N_PRODUCTS", "2000")),
             n_orders=int(os.environ.get("N_ORDERS", "15000")))
