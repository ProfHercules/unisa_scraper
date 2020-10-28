import time
import pickle
import os
from typing import Union

import pymongo
from pymongo.errors import DuplicateKeyError, BulkWriteError
from pymongo.database import Database
from pymongo.collection import Collection

from unisa_scraper import UnisaScraperV2
from models import Qualification, Module


def get_qualifications() -> [Qualification]:
    # constants
    qualification_pickle_filename = "qualifications.pkl"

    if os.path.isfile(f"./{qualification_pickle_filename}"):
        with open(qualification_pickle_filename, 'rb') as f:
            return pickle.load(f)
    else:
        print("Scraping Unisa website ...")
        scraper = UnisaScraperV2()
        start = time.time()
        q = scraper.get_qualifications()
        end = time.time()
        print("Duration:", end - start, "sec")

        print(scraper.get_headings())

        try:
            print("Dumping list to pickle file...")
            with open(qualification_pickle_filename, 'wb') as f:
                pickle.dump(q, f)
            scraper.dump_module_list()
        except Exception as e:
            print(e)

        return q


data = get_qualifications()


def get_mongodb() -> Database:
    print("Connecting to local db...")
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?compressors=snappy&gssapiServiceName=mongodb")
    print("Connected!")
    return client.unisa_database

#
# def backup_data():
#     db = get_mongodb()
#
#     qualification_collection: Collection = db.qualifications
#     module_collection: Collection = db.modules
#     module_collection.create_index([('url', pymongo.ASCENDING)], unique=True)
#     qualification_collection.create_index([('url', pymongo.ASCENDING)], unique=True)
#
#     def insert_list(collection: Collection, lst: [Union[Qualification, Module]]):
#         docs: [dict] = []
#         for item in lst:
#             docs.append(item.to_dict())
#         collection.insert_many(docs)
#
#     if qualification_collection.count_documents({}) != len(qualifications):
#         qualification_collection.drop()
#         insert_list(qualification_collection, qualifications)
#
#     if module_collection.count_documents({}) != len(modules):
#         module_collection.drop()
#         insert_list(module_collection, modules)


# print("Adding data to mongo")
# # backup_data()
# print("Done")
