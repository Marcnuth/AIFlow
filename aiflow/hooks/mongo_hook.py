"""
This file is forked from: https://github.com/airflow-plugins/mongo_plugin/blob/master/hooks/mongo_hook.py
"""

from ssl import CERT_NONE
from airflow.hooks.base_hook import BaseHook
from pymongo import MongoClient
import logging
from urllib.parse import quote_plus


logger = logging.getLogger(__name__)


class MongoHook(BaseHook):
    """
    PyMongo Wrapper to Interact With Mongo Database
    Mongo Connection Documentation
    https://docs.mongodb.com/manual/reference/connection-string/index.html
    You can specify connection string options in extra field of your connection
    https://docs.mongodb.com/manual/reference/connection-string/index.html#connection-string-options
    ex.
        {replicaSet: test, ssl: True, connectTimeoutMS: 30000}
    """
    conn_type = 'MongoDb'

    def __init__(self, conn_id='mongo_default', *args, **kwargs):
        super().__init__(source='mongo')

        self.mongo_conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson

    def get_conn(self):
        """
        Fetches PyMongo Client
        """
        conn = self.connection

        uri = 'mongodb://{creds}{host}{port}/{database}'.format(
            creds='{}:{}@'.format(
                quote_plus(conn.login), quote_plus(conn.password)
            ) if conn.login is not None else '',

            host=conn.host,
            port='' if conn.port is None else ':{}'.format(conn.port),
            database='' if conn.schema is None else conn.schema
        )

        logger.debug(f'the mongodb connection uri is {uri}')

        # Mongo Connection Options dict that is unpacked when passed to MongoClient
        options = self.extras

        # If we are using SSL disable requiring certs from specific hostname
        if options.get('ssl', False):
            options.update({'ssl_cert_reqs': CERT_NONE})

        logger.debug(f'the mongodb connection options are {options}')
        return MongoClient(uri, **options)

    def get_collection(self, mongo_collection, mongo_db=None):
        """
        Fetches a mongo collection object for querying.
        Uses connection schema as DB unless specified
        """
        mongo_db = mongo_db if mongo_db is not None else self.connection.schema
        mongo_conn = self.get_conn()

        return mongo_conn.get_database(mongo_db).get_collection(mongo_collection)

    def aggregate(self, mongo_collection, aggregate_query, mongo_db=None, **kwargs):
        """
        Runs and aggregation pipeline and returns the results
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.aggregate
        http://api.mongodb.com/python/current/examples/aggregation.html
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.aggregate(aggregate_query, **kwargs)

    def find(self, mongo_collection, query, find_one=False, mongo_db=None, **kwargs):
        """
        Runs a mongo find query and returns the results
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.find
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        if find_one:
            return collection.find_one(query, **kwargs)
        else:
            return collection.find(query, **kwargs)

    def insert_one(self, mongo_collection, doc, mongo_db=None, **kwargs):
        """
        Inserts a single document into a mongo collection
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.insert_one
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.insert_one(doc, **kwargs)

    def insert_many(self, mongo_collection, docs, mongo_db=None, **kwargs):
        """
        Inserts many docs into a mongo collection.
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.insert_many
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.insert_many(docs, **kwargs)

    def replace_one(self, mongo_collection, replacement_filter, doc, mongo_db=None, **kwargs):
        """
        Replaces a single document that matches a filter in a mongo collection.
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.replace_one
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.replace_one(replacement_filter, doc, **kwargs)

    def update_one(self, mongo_collection, update_filter, update, mongo_db=None, **kwargs):
        """
        Updates a single document that matches a filter in a mongo collection.
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.update_one
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.update_one(update_filter, update, **kwargs)

    def update_many(self, mongo_collection, update_filter, update, mongo_db=None, **kwargs):
        """
        Updates many docs that matches a filter in a mongo collection.
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.update_many
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.update_many(update_filter, update, **kwargs)

    def bulk_write(self, mongo_collection, requests, mongo_db=None, **kwargs):
        """
        Submits a bulk write job mongo based on the specified requests.
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.bulk_write
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.bulk_write(requests, **kwargs)
