#%%
from pymongo import MongoClient
import time

client = MongoClient("mongodb://localhost:27017")
db = client["shopdb"]

customers = db["customers"]
products  = db["products"]
orders    = db["orders"]

#%%
# ----- Task 6 -----
times = []
for i in range(10):
    # reset
    customers.update_many({}, {"$set": {"is_premium": False}})
    start = time.perf_counter()
    prem = list(orders.aggregate([
        {"$group": {
            "_id": "$customer_id",
            "total_orders": {"$sum": 1},
            "total_spending": {"$sum": "$total_amount"}
        }},
        {"$match": {
            "$or": [
                {"total_orders": {"$gt": 5}},
                {"total_spending": {"$gt": 1000}}]}}]))
    ids = [d["_id"] for d in prem]
    customers.update_many(
        {"_id": {"$in": ids}},
        {"$set": {"is_premium": True}}
    )
    end = time.perf_counter()
    run_time = end - start
    times.append(run_time)
    print(f"Run {i+1}: {run_time:.6f} seconds")

print("Task 6 Avg(seconds):", sum(times)/len(times))

#%%
# ----- Task 7 -----
times = []
for i in range(10):
    start = time.perf_counter()
    list(products.find({"price":{"$gt": 200}},{"name":1, "category":1,"price":1,"_id":0})
)
    end = time.perf_counter()
    run_time = end - start
    times.append(run_time)
    print(f"Run {i+1}: {run_time:.6f} seconds")
print("Task 7 Avg(seconds):", sum(times)/len(times))

#%%
# ----- Task 8 -----
times = []
for i in range(10):
    start = time.perf_counter()
    list(orders.aggregate([
        {"$match": {"items.product_id": 1}},
        {"$project": {
            "_id": 1,
            "customer_id": 1,
            "items": {
                "$filter": {
                    "input": "$items",
                    "as": "item",
                    "cond": {"$eq": ["$$item.product_id", 1]}}}}}]))
    end = time.perf_counter()
    run_time = end - start
    times.append(run_time)
    print(f"Run {i+1}: {run_time:.6f} seconds")
print("Task 8 Avg(seconds):", sum(times)/len(times))

#%%
# ----- Task 9 -----
backup = list(customers.find({}))
times = []
for i in range(10):
    # reset
    customers.delete_many({})
    customers.insert_many(backup)
    start = time.perf_counter()
    ids = orders.distinct("customer_id")
    customers.delete_many({"_id": {"$nin": ids}})
    end = time.perf_counter()
    run_time = end - start
    times.append(run_time)
    print(f"Run {i+1}: {run_time:.6f} seconds")
print("Task 9 Avg(seconds):", sum(times)/len(times))

#%%
# ----- Task 10 -----
times = []
for i in range(10):
    start = time.perf_counter()
    young = list(customers.find({"age": {"$lt": 25}}))
    young_ids = [d["_id"] for d in young]
    list(orders.aggregate([
        {"$match": {"customer_id": {"$in": young_ids}}},
        {"$unwind": "$items"},
        {"$group": {"_id": "$items.category"}}]))
    end = time.perf_counter()
    run_time = end - start
    times.append(run_time)
    print(f"Run {i+1}: {run_time:.6f} seconds")
print("Task 10 Avg(seconds):", sum(times)/len(times))
#%%
times = []
for i in range(10):
    start = time.perf_counter()
    list(customers.find({
        "email": {"$regex": "@(gmail|yahoo)\\.com$"}
    }))
    end = time.perf_counter()
    run_time = end - start
    times.append(run_time)
    print(f"Run {i+1}: {run_time:.6f} seconds")
print("Task 11 Avg(seconds):", sum(times)/len(times))

#%%