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


def scrape_data() -> ([Qualification], [Module]):
    print("Scraping Unisa website ...")
    scraper = UnisaScraperV2()
    start = time.time()
    q = scraper.get_qualifications()
    end = time.time()
    print("Duration:", end - start, "sec")
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

    # clear existing data
    qualification_collection.drop()

    # create index
    qualification_collection.drop_indexes()
    qualification_collection.create_index([('url', pymongo.ASCENDING)], unique=True)
    qualification_collection.create_index([('code', pymongo.ASCENDING)], unique=True)
    qualification_collection.create_index([('module_levels.module_groups.modules.url', pymongo.ASCENDING)])
    qualification_collection.create_index([("$**", pymongo.TEXT)])

    docs = map(Qualification.to_dict, qualifications)
    qualification_collection.insert_many(docs)


def find_q_with_module_code(code: str) -> [Qualification]:
    qualification_collection: Collection = db.qualifications
    s = time.time()
    # cursor: Cursor = qualification_collection.find({"module_levels.module_groups.modules.code": code})
    cursor: Cursor = qualification_collection.find()
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
