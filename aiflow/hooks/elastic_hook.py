from ssl import CERT_NONE
from airflow.hooks.base_hook import BaseHook
import logging
from urllib.parse import quote_plus
from elasticsearch import Elasticsearch


logger = logging.getLogger(__name__)


class ElasticHook(BaseHook):

    conn_type = 'Elasticsearch'

    def __init__(self, conn_id='elastic_default', *args, **kwargs):
        super().__init__(source='elasticsearch')

        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson

    def client(self):

        uri = '{host}:{port}'.format(
            host=self.connection.host,
            port='' if self.connection.port is None else self.connection.port,
        )

        return Elasticsearch([uri])
