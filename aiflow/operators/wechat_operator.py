from airflow.models import BaseOperator
from pathlib import Path
import json
import logging
import requests


logger = logging.getLogger(__name__)


class WeChatWorkRobotOperator(BaseOperator):
    """
    This operator will send message to wechat work via wechat work robot
    """

    def __init__(self, wechat_webhook_url, message, *args, **kwargs):
        super(WeChatWorkRobotOperator, self).__init__(*args, **kwargs)

        self.wechat_webhook_url = wechat_webhook_url
        self.message = message

    def execute(self, context):
        logger.debug('start WeChatWorkRobotOperator ...')

        r = requests.post(self.wechat_webhook_url, json=message)
        assert r.status_code == 200, f'error happens when sending message, status_code={r.status_code}, response={r.contnet}'
        assert r.json()['errcode'] == 0, f'error happens when sending message, response={r.content}'
