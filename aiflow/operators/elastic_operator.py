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

    def __init__(self, elastic_conn_id, elastic_function, elastic_func_kwargs, output_fields, output_file, *args, **kwargs):
        super(Elastic2CSVOperator, self).__init__(*args, **kwargs)

        self.elastic_conn_id = elastic_conn_id
        self.elastic_function = elastic_function
        self.elastic_func_kwargs = elastic_func_kwargs
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

        res = getattr(es, self.elastic_function)(**self.elastic_func_kwargs)
        hits = res['hits']
        if hits['total'] <= 0:
            logger.info(f'no hits found, no file will be exported. es result: {res}')
            return

        with open(self.output_file.absolute().as_posix(), 'w+') as f:
            f.write(','.join(self.output_fields) + '\n')
            for i, doc in enumerate(hits['hits']):
                if self.limit and i >= self.limit:
                    break

                # todo: escape the string in streaming writing way
                data = [self._value_of(doc['_source'], field, default='') for field in self.output_fields]
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
