from airflow.models import BaseOperator
from pathlib import Path
import json
from bson import json_util
from aiflow.hooks.mongo_hook import MongoHook
import logging
from jsonpath import jsonpath


logger = logging.getLogger(__name__)


class MongoToCSVOperator(BaseOperator):
    """
    This operator allows you to read data from mongo, and write to a file
    """

    def __init__(self, mongo_conn_id, mongo_collection, mongo_database, mongo_query, output_fields, output_file, *args, **kwargs):
        super(MongoToCSVOperator, self).__init__(*args, **kwargs)

        self.mongo_conn_id = mongo_conn_id
        self.mongo_db = mongo_database
        self.mongo_collection = mongo_collection
        self.mongo_query = mongo_query
        self.is_pipeline = True if isinstance(self.mongo_query, list) else False
        self.output_fields = output_fields
        self.output_file = Path(output_file)
        self.limit = kwargs.get('limit', None)

    def execute(self, context):
        logger.debug('start MongoToFileOperator ...')

        self.output_file.parent.mkdir(exist_ok=True, parents=True)
        if self.output_file.exists() and self.output_file.is_file:
            self.output_file.unlink()

        mongo_conn = MongoHook(self.mongo_conn_id).get_conn()

        collection = mongo_conn.get_database(self.mongo_db).get_collection(self.mongo_collection)
        results = collection.aggregate(self.mongo_query) if self.is_pipeline else collection.find(self.mongo_query)

        with open(self.output_file.absolute().as_posix(), 'w+') as f:
            f.write(','.join(self.output_fields) + '\n')
            for i, doc in enumerate(results):
                if self.limit and i >= self.limit:
                    break

                # todo: escape the string in streaming writing way
                data = [self._value_of(doc, field, default='') for field in self.output_fields]
                data = list(map(lambda x: x if isinstance(x, str) else json.dumps(x), data))

                f.write(','.join(data) + '\n')
                f.flush()

                if i % 10000 == 0:
                    logger.debug(f'already wrote {i} lines data, limit: {self.limit}.')

    def _value_of(self, doc, field, default=None):
        try:
            found = jsonpath(doc, field)
            return found[0] if found else default
        except:
            return default
