from pymongo import MongoClient

client = MongoClient("mongodb+srv://mongoadmin:mongoadmin@cluster0.keqe2go.mongodb.net/?retryWrites=true&w=majority")

db = client.get_database("scrapers_db")

records = db.index

docu_count = records.count_documents({})

# print(docu_count)


# offsetno = records.find_one({'collection':'Listamonclova'})
# # print(list(records.find()))
# print(offsetno['offset'])

result = records.update_one({'total_count':1}, {'$set': {'total_count':5}}, upsert=True)
print(result.matched_count)




