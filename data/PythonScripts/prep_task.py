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

