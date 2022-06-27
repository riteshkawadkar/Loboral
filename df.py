import pymongo

client = pymongo.MongoClient("mongodb://Pad18:ER8Bsy2zIFpAFZiV@104.225.140.236:27017/Crudo")

#use database "organisation"
db = client.get_database("Crudo")
records = db.Laboral_Coahuila
docu_count = records.count_documents({})

print(docu_count)