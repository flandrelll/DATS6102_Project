#%%
from faker import Faker
from pymongo import MongoClient
import random
from datetime import datetime, timedelta

# ----------------- settings -----------------
N_CUSTOMERS = 1000
N_PRODUCTS  = 500
N_ORDERS    = 2000

SEED = 42  # set for reproducibility
IMPERFECT_FRACTION = 0.01  # 1% imperfect records
# --------------------------------------------------

random.seed(SEED)
fake = Faker()

CATEGORIES = ["Electronics", "Accessories", "Clothing", "Home", "Beauty"]
PAYMENT_STATUSES = ["pending", "paid", "refunded"]

client = MongoClient("mongodb://localhost:27017")
db = client["shopdb"]

customers_coll = db["customers"]
products_coll  = db["products"]
orders_coll    = db["orders"]

# Optional: drop old data during testing
customers_coll.drop()
products_coll.drop()
orders_coll.drop()

# ---------------------- Customers ----------------------
customers = []
for cid in range(1, N_CUSTOMERS + 1):
    # intentionally missing email for a small fraction
    if random.random() < IMPERFECT_FRACTION:
        email = None
    else:
        # force gmail/yahoo for later tasks
        domain = random.choice(["gmail.com", "yahoo.com", "example.com"])
        email = f"{fake.user_name()}@{domain}"

    customer = {
        "_id": cid,
        "name": fake.name(),
        "age": random.randint(18, 70),
        "email": email,  # may be None on purpose
        "is_premium": random.choice([True, False]),
        "address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "zip": fake.postcode()
        }
    }
    customers.append(customer)

customers_coll.insert_many(customers, ordered=False)
print("Inserted customers:", customers_coll.count_documents({}))

# ---------------------- Products -----------------------
products = []
valid_products_for_orders = []  # only products with a category

for pid in range(1, N_PRODUCTS + 1):
    # small fraction missing category (for prep task) – NOT be used in orders
    if random.random() < IMPERFECT_FRACTION:
        category = None
        can_use_in_orders = False
    else:
        category = random.choice(CATEGORIES)
        can_use_in_orders = True

    product = {
        "_id": pid,
        "name": fake.word().title(),
        "category": category,
        "price": round(random.uniform(5.0, 500.0), 2),
        "stock": random.randint(0, 1000)
    }
    products.append(product)

    if can_use_in_orders:
        valid_products_for_orders.append(product)

products_coll.insert_many(products, ordered=False)
print("Inserted products:", products_coll.count_documents({}))

# ---------------------- Orders -------------------------
customer_ids = list(range(1, N_CUSTOMERS + 1))

orders = []

def random_date_between(start, end):
    delta = end - start
    offset = random.randrange(delta.days * 24 * 3600)
    return start + timedelta(seconds=offset)

# define a date range that covers at least 2025-04-02 to 2025-05-01
start_date = datetime(2024, 1, 1)
end_date   = datetime(2026, 12, 31)

for oid in range(1, N_ORDERS + 1):
    customer_id = random.choice(customer_ids)

    # small fraction of orders with empty items (for prep task)
    if random.random() < IMPERFECT_FRACTION:
        items = []
    else:
        n_items = random.randint(1, 5)
        items = []
        for _ in range(n_items):
            product = random.choice(valid_products_for_orders)
            quantity = random.randint(1, 5)
            item = {
                "product_id": product["_id"],
                "product_name": product["name"],
                "category": product["category"],
                "quantity": quantity,
                "price": product["price"]
            }
            items.append(item)

    # compute total_amount from items
    total_amount = sum(it["quantity"] * it["price"] for it in items)

    # mostly valid payment_status, some invalid for prep task
    if random.random() < IMPERFECT_FRACTION:
        payment_status = "unknown_status"  # invalid on purpose
    else:
        payment_status = random.choice(PAYMENT_STATUSES)

    # order_date, with some guaranteed refunded orders in a specific range
    if oid <= 100:  # make first ~100 special
        # ensure some refunded in 2025-04-02 to 2025-05-01
        special_start = datetime(2025, 4, 2)
        special_end   = datetime(2025, 5, 1)
        order_date = random_date_between(special_start, special_end)
        # bias some of these to refunded
        payment_status = random.choice(["paid", "refunded"])
    else:
        order_date = random_date_between(start_date, end_date)

    order = {
        "_id": oid,
        "customer_id": customer_id,
        "order_date": order_date.strftime("%Y-%m-%d"),
        "items": items,
        "total_amount": round(total_amount, 2),
        "payment_status": payment_status
    }
    orders.append(order)

# insert in batches to avoid huge payload
batch_size = 5000
for i in range(0, len(orders), batch_size):
    orders_coll.insert_many(orders[i:i+batch_size], ordered=False)

print("Inserted orders:", orders_coll.count_documents({}))

client.close()
print("Done generating shop data.")


#%%
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["shopdb"]

customers = db["customers"]
products  = db["products"]
orders    = db["orders"]

# 1) Count documents
print("customers:", customers.count_documents({}))
print("products :", products.count_documents({}))
print("orders   :", orders.count_documents({}))

# 2) Fix missing email -> "unknown@example.com"
customers.update_many(
    {"$or": [{"email": None}, {"email": {"$exists": False}}]},
    {"$set": {"email": "unknown@example.com"}}
)

# 3) Fix missing category -> "Misc"
products.update_many(
    {"$or": [{"category": None}, {"category": {"$exists": False}}]},
    {"$set": {"category": "Misc"}}
)

# 4) Remove empty orders: items: []
orders.delete_many({"items": {"$size": 0}})

# 5) Normalize invalid payment_status -> "pending"
orders.update_many(
    {"payment_status": {"$nin": ["pending", "paid", "refunded"]}},
    {"$set": {"payment_status": "pending"}}
)

client.close()

#%%