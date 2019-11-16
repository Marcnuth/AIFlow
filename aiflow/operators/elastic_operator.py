from airflow.models import BaseOperator
from pathlib import Path
import json
from bson import json_util
from aiflow.hooks.elastic_hook import ElasticHook
import logging
from jsonpath import jsonpath


logger = logging.getLogger(__name__)


class Elastic2CSVOperator(BaseOperator):
    """
    This operator allows you to read data from mongo, and write to a file
    """

    def __init__(self, elastic_conn_id, elastic_index, output_fields, output_file, *args, **kwargs):
        super(Elastic2CSVOperator, self).__init__(*args, **kwargs)

        self.elastic_conn_id = elastic_conn_id
        self.elastic_index = elastic_index
        self.output_fields = output_fields
        self.output_file = Path(output_file)

        self.elastic_search = kwargs.get('elastic_search', None)
        self.limit = kwargs.get('limit', None)

    def execute(self, context):
        logger.debug('start Elastic2CSVOperator ...')

        self.output_file.parent.mkdir(exist_ok=True, parents=True)
        if self.output_file.exists() and self.output_file.is_file:
            self.output_file.unlink()

        es = ElasticHook(self.elastic_conn_id).client()

        res = es.search(index=self.elastic_index, q=self.elastic_search)
        hits = res['hits']
        if hits['total'] <= 0:
            logger.info(f'no hits found, no file will be exported. es result: {res}')
            return

        for h in hits['hits']:
            logger.info(h['_source'])
