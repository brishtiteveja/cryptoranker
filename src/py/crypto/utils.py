from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017')
db = client.crypto_db

# 1. Check total documents
total_docs = db.historical_data.count_documents({})
print(f"Total documents in collection: {total_docs}")

# 2. Check unique timestamps
timestamps = db.historical_data.distinct('timestamp')
print(f"Number of unique timestamps: {len(timestamps)}")
print("Latest timestamp:", max(timestamps))

# 3. Check unique coins
unique_coins = db.historical_data.distinct('coin_id')
print(f"Number of unique coins: {len(unique_coins)}")

# 4. Check document count for latest timestamp
latest_ts = max(timestamps)
docs_for_latest = db.historical_data.count_documents({'timestamp': latest_ts})
print(f"Documents for latest timestamp: {docs_for_latest}")

# 5. Sample a few documents
sample_docs = list(db.historical_data.find({'timestamp': latest_ts}).limit(3))
for doc in sample_docs:
    print("\nSample document:")
    print(f"Coin ID: {doc['coin_id']}")
    print(f"Timestamp: {doc['timestamp']}")
    print(f"Metrics: {doc['metrics']}")