import time
import pymongo
from pymongo.errors import DuplicateKeyError, BulkWriteError

from unisa_scraper import UnisaScraper
from models import Qualification, Module

# constants
all_qual_link = "/sites/corporate/default/Register-to-study-through-Unisa/Undergraduate-&-honours-qualifications/Find-your-qualification-&-choose-your-modules/All-qualifications/"

print("Connecting to remote db...")
username = "python_unisa_db"
password = "q15CPX9mSKW6c3Qk"
client = pymongo.MongoClient(f"mongodb+srv://{username}:{password}@unisadb.mctod.mongodb.net/unisaDb?retryWrites=true&w=majority")
print("Connected!")
db = client.unisa_database

module_collection = db.modules
module_collection.create_index([('url', pymongo.ASCENDING)], unique=True)
qualification_collection = db.qualifications
# qualification_collection.create_index([('url', pymongo.ASCENDING)], unique=True)

print("Scraping Unisa website ...")
scraper = UnisaScraper()
start = time.time()
qualifications: [Qualification] = scraper.get_qualifications(all_qual_link)
end = time.time()
print("Duration:", end - start, "sec")

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