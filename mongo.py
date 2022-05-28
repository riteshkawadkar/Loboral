from pymongo import MongoClient

client = MongoClient("mongodb+srv://mongoadmin:mongoadmin@cluster0.keqe2go.mongodb.net/?retryWrites=true&w=majority")

db = client.get_database("scrapers_db")

records = db.folios

docu_count = records.count_documents({})

print(docu_count)



