import sys
from typing import List, Iterable
import re

import redis
import pyodbc
from pymongo import MongoClient
from neo4j import GraphDatabase

import numpy as np
import pandas as pd

class RedisHandler:
    def __init__(self, 
                 ip_address: str = 'localhost', 
                 port: int = 6379,
                 decode_responses: bool = True):
        self.r = redis.StrictRedis(
            ip_address,
            port,
            decode_responses
        )
        self.pipe = self.r.pipeline()
    def info(self):
        print(self.get_nodes())
    def get_value(self, key: str):
        key = str(key)
        try:
            self.value = self.r.get(key)
        except:
            sys.exit("Error in execution..")
    def set_value(self, key: str, value: str):
        key, value = str(key), str(value)
        try:
            self.r.set(key, value)
        except Exception as e:
            print(e)
    def set_dict(self, hash_name: str, kv_map: dict):
        assert isinstance(kv_map, dict)
        hash_name = str(hash_name)
        self.r.hset(hash_name,
                    kv_map)
    def get_dict(self, hash_name: str):
        hash_name = str(hash_name)
        self.data = self.r.getall(hash_name)
    def insert_from_file(self, 
                         delimitter: str,
                         filepath: str):
        try:
            with open(filepath, 'r') as f:
                text = f.read().split("\n")
        except:
            raise FileNotFoundError(filepath)
        for line in text:
            key, value = line.split(delimitter)
            self.pipe.set(key, value)
        self.pipe.execute()

class MSSQL_Handler:
    def __init__(self,
                 server: str,
                 database: str):
        self.server = server
        self.database = database
        self.conn_str = (
            r'DRIVER={ODBC Driver 17 for SQL Server};'
            fr'SERVER={self.server};'
            fr'DATABASE={self.database};'
            r'Trusted_Connection=yes;'
        )
        self.cnxn = pyodbc.connect(self.conn_str)
    def read_query(self, sql_filepath: str):
        file = open(sql_filepath, 'r')
        self.query = file.read()
        file.close()
    def replace_placeholders(self,
                             placeholder_pattern: str,
                             replacement: str):
        assert self.query is not None, "Execute read_query(filepath) first"
        assert placeholder_pattern is not None and replacement is not None, "Both input fields cannot be none"
        self.query = re.sub(f"{placeholder_pattern}",
                            replacement,
                            self.query)
    def query_db(self):
        assert self.query is not None, "Execute read_query(filepath) first"
        self._df = pd.read_sql_query(self.query,
                                     self.cnxn)
    @property
    def df(self):
        return self._df

class MongoHandler:
    """Usage:
    handler = MongoHandler(*args)
    with handler() as h:
        res = h.select(**kwargs)    
    """
    def __init__(self,
                 url: str,
                 db_name: str,
                 collection_name: str, 
                 username: str,
                 password: str,
                 authMechanism: str = "SCRAM-SHA-1"):
        self.mongo_url = f"mongodb://{username}:{password}@{url}/{db_name}"
        self.client = MongoClient(self.mongo_url,
                                  authSource = db_name,
                                  authMechanism = authMechanism)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
    def close(self):
        self.client.close()
    def delete(self, filename: str, primary_keys: list):
        reference = {primary_keys[0]: filename}
        if self.collection.find_one(reference):
            self.collection.delete_one(reference)
    def select(self, keys_of_interest: List[str], **kwargs):
        self.default_res = dict(zip(keys_of_interest, [None]))
        reference = kwargs
        res = self.collection.find(reference, {"_id": False})
        if res is not None:
            return [r for r in res]
        else:
            reference.update(self.default_res)
            return reference
    def select_many(self, **kwargs):
        d = dict()
        for k, v in kwargs.items():
            if type(v) is dict:
                d.update({k: v})
            elif type(v) is not list:
                v = [v]
                d.update({k: {"$in": v}})
            else:
                d.update({k: {"$in": v}})
        res = self.collection.find(d, {"_id": False})
        return [r for r in res]
    def upsert(self, filename: str, **kwargs):
        reference = {self.primary_keys[0]: filename}
        if not self.collection.find_one(reference):
            self.insert(reference, **kwargs)
        else:
            self.update(reference, **kwargs)
    def update(self, kv_pairs: dict, **kwargs):
        self.collection.update_one(kv_pairs,
                                   {"$set": kwargs})
    def __enter__(self):
        return self
    def __exit__(self, type, value, tb):
        self.close()
    @property
    def Collection(self):
        return self.collection

class Neo4j_Handler:
    def __init__(self,
                 url: str, 
                 username: str,
                 password: str):
        self.url = url
        self.user = username
        self.pwd = password
        self.driver = GraphDatabase.driver(self.url,
                                           auth = (self.user,self.pwd)
                                           )
    def query(self, 
              query: str, 
              parameters: Iterable = None, 
              db: str = None):
        assert self.driver is not None, "Driver not initialized. Check login credentials"
        session = self.driver.session(database=db) if db is not None else self.driver.session() 
        return list(session.run(query, parameters))
    def __enter__(self):
        return self
    def __exit__(self):
        self.close()