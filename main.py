import time
import pickle
import os
from typing import Union
import pprint

import pymongo
from pymongo.cursor import Cursor
from pymongo.errors import DuplicateKeyError, BulkWriteError
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.results import InsertOneResult, InsertManyResult

from unisa_scraper import UnisaScraperV2
from models import Qualification, Module


def debug_dump(qs: [Qualification]):
    with open("debug.pkl", "wb") as f:
        pickle.dump(qs, f)


def debug_load() -> [Qualification]:
    if os.path.isfile("debug.pkl"):
        with open("debug.pkl", "rb") as f:
            return pickle.load(f)


def scrape_data() -> [Qualification]:
    print("Scraping Unisa website ...")
    if (cached := debug_load()) is not None:
        q = cached
    else:
        scraper = UnisaScraperV2()
        start = time.time()
        q = scraper.get_qualifications()
        end = time.time()
        debug_dump(q)
        print("Duration:", end - start, "sec")

    headings = UnisaScraperV2.get_headings(q)
    open('headings.txt', 'w').close()
    with open("headings.txt", "a") as file_object:
        print(len(headings))
        for heading in headings:
            file_object.write(f"{heading}\n")
    return q


qualifications = scrape_data()


def get_mongodb() -> Database:
    print("Connecting to local db...")
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017")
    print("Connected!")
    return client.unisa_database


def pretty(text: any):
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(text)


def backup_data():
    if db is None:
        exit(1)
    # set references
    qualification_collection: Collection = db.qualifications

    # # clear existing data
    # qualification_collection.drop()

    # create index
    qualification_collection.drop_indexes()
    qualification_collection.create_index([('url', pymongo.ASCENDING)], unique=True)
    qualification_collection.create_index([('code', pymongo.ASCENDING), ('name', pymongo.ASCENDING)], unique=True)
    qualification_collection.create_index([("$**", pymongo.TEXT)])

    print("Documents before:", qualification_collection.count_documents({}))

    for qualification in qualifications:
        doc = qualification.to_dict()
        before = qualification_collection.find_one_and_replace({"url": qualification.url}, doc, upsert=True)
        after = qualification_collection.find_one({"url": qualification.url})
        # before is none if the doc didn't exist (hence upsert)
        assert before is None or (before["_id"] == after["_id"])

    print("Documents after :", qualification_collection.count_documents({}))


def find_q_with_module_code(code: str) -> [Qualification]:
    qualification_collection: Collection = db.qualifications
    s = time.time()
    cursor: Cursor = qualification_collection.find({"module_levels.module_groups.modules.code": code})
    # cursor: Cursor = qualification_collection.find()
    e = time.time()
    results: [Qualification] = []
    for doc in cursor:
        results.append(doc)
    print(f"Found {len(results)} results in :", round((e - s) * 1000, 2), "ms")
    return results


print("Adding data to mongo")
start = time.time()
db = get_mongodb()
backup_data()
end = time.time()
print("Duration:", end - start, "sec")
res: [Qualification] = find_q_with_module_code("COS1511")
# for q in res:
#     print(q["name"])
