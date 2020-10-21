import time
import pickle
import os
from typing import Union

import pymongo
from pymongo.errors import DuplicateKeyError, BulkWriteError
from pymongo.database import Database
from pymongo.collection import Collection

from unisa_scraper import UnisaScraper
from models import Qualification, Module


def get_qualifications() -> [Qualification]:
    # constants
    all_qual_link = "/sites/corporate/default/Register-to-study-through-Unisa/Undergraduate-&-honours-qualifications/Find-your-qualification-&-choose-your-modules/All-qualifications/"
    qualification_pickle_filename = "qualifications.pkl"

    if os.path.isfile(f"./{qualification_pickle_filename}"):
        with open(qualification_pickle_filename, 'rb') as f:
            return pickle.load(f)
    else:
        print("Scraping Unisa website ...")
        scraper = UnisaScraper()
        start = time.time()
        q = scraper.get_qualifications(all_qual_link)
        end = time.time()
        print("Duration:", end - start, "sec")

        try:
            print("Dumping list to pickle file...")
            with open(qualification_pickle_filename, 'wb') as f:
                pickle.dump(q, f)
        except Exception as e:
            print(e)

        return q


def get_modules(q_list: [Qualification]) -> [Module]:
    m = {}
    for qualification in q_list:
        if qualification.modules is None or len(qualification.modules) == 0:
            continue
        for module in qualification.modules:
            m[module.url] = module

    return m.values()


def get_mongodb() -> Database:
    print("Connecting to local db...")
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb")
    return client.unisa_database;

    # print("Connecting to remote db...")
    # username = "python_unisa_db"
    # password = "q15CPX9mSKW6c3Qk"
    # client = pymongo.MongoClient(
    #     f"mongodb+srv://{username}:{password}@unisadb.mctod.mongodb.net/unisaDb?retryWrites=true&w=majority")
    # print("Connected!")
    # return client.unisa_database


# code
qualifications = get_qualifications()
modules = get_modules(qualifications)


def backup_data():
    db = get_mongodb()

    qualification_collection: Collection = db.qualifications
    module_collection: Collection = db.modules
    module_collection.create_index([('url', pymongo.ASCENDING)], unique=True)
    qualification_collection.create_index([('url', pymongo.ASCENDING)], unique=True)

    def insert_list(collection: Collection, lst: [Union[Qualification, Module]]):
        docs: [dict] = []
        for item in lst:
            docs.append(item.to_dict())
        collection.insert_many(docs)

    if qualification_collection.count_documents({}) != len(qualifications):
        qualification_collection.drop()
        insert_list(qualification_collection, qualifications)

    if module_collection.count_documents({}) != len(modules):
        module_collection.drop()
        insert_list(module_collection, modules)


# backup_data()
query = ["child", "psy"]

results = {}

for qualification in qualifications:
    if len(qualification.modules) == 0:
        continue

    score: int = 0
    for module in qualification.modules:
        if all(module.matches(term) for term in query):
            score += 1
            print(f"{qualification.code} : {module.url}")

    match: float = float(score) / float(len(qualification.modules))
    if match > 0.0:
        results[qualification.url] = (round(match * 100.0, 1), qualification)


def good_score(s: int) -> bool:
    return s > 10


for score, q in results.values():
    print(f"{score}% = {q.name}")

print("Found:", len(results.values()))
