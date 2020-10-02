import os
import pymongo
from pymongo.errors import DuplicateKeyError, BulkWriteError

from unisa_scraper import UnisaScraper
from models import Qualification, Module

print("Connecting to remote db...")
username = os.environ.get("username")
password = os.environ.get("password")
client = pymongo.MongoClient(f"mongodb+srv://{username}:{password}@unisadb.mctod.mongodb.net/unisaDb?retryWrites=true&w=majority")
print("Connected!")
db = client.unisa_database

module_collection = db.modules
module_collection.create_index([('url', pymongo.ASCENDING)], unique=True)
qualification_collection = db.qualifications
# qualification_collection.create_index([('url', pymongo.ASCENDING)], unique=True)

print("Scraping Unisa website ...")
scraper = UnisaScraper(headless=True)
qualifications: [Qualification] = scraper.get_qualifications()

for qualification in qualifications:
    try:
        qualification_collection.replace_one({"url": qualification.url}, qualification.to_dict(), upsert=True)
        modules = list(map(Module.to_dict, qualification.modules))
        module_collection.insert_many(modules)
    except DuplicateKeyError as e:
        print(e)
    except BulkWriteError as e:
        for module in qualification.modules:
            module_collection.replace_one({"url": module.url}, module.to_dict(), upsert=True)

print("Done!")