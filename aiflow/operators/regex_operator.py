from airflow.models import BaseOperator
from pathlib import Path
import json
from bson import json_util
import logging
from collections import namedtuple
import pandas as pd
import re


logger = logging.getLogger(__name__)


class RegExLabellingOperator(BaseOperator):
    """
    This operator allows you to label data based on regex rules
    """

    class RegExRules:
        Rule = namedtuple('Ruled', ['regex', 'label'])

        def __init__(self, rules, unmatch_label, re_flags):
            self._unmatch_label = unmatch_label
            self._rules = [self.Rule(regex=r[0], label=r[1]) for r in rules]
            self._re_flags = re_flags

        def label(self, text):
            if not text or not isinstance(text, str):
                return self._unmatch_label

            return next((rule.label for rule in self._rules if re.search(rule.regex, text, self._re_flags)), self._unmatch_label)

    def __init__(self, input_file, data_column, regex_rules, label_column, output_file, *args, **kwargs):
        super(RegExLabellingOperator, self).__init__(*args, **kwargs)

        self.input_file = Path(input_file)
        self.data_column = data_column
        self.regex_rules = regex_rules
        self.label_column = label_column
        self.output_file = Path(output_file)

    def execute(self, context):
        logger.debug('start RegExLabellingOperator ...')

        assert self.input_file.name.lower().endswith('csv'), f'"csv" input file required'
        assert self.input_file.exists() and self.input_file.is_file(), f'invalid input_file: {input_file}'
        assert isinstance(self.regex_rules, self.RegExRules), f'regex_rules should be instance of RegExRules'

        self.output_file.parent.mkdir(exist_ok=True, parents=True)
        if self.output_file.exists() and self.output_file.is_file:
            self.output_file.unlink()

        df = pd.read_csv(self.input_file)
        df[self.label_column] = df[self.data_column].map(self.regex_rules.label)

        df.to_csv(self.output_file, index=False, header=True)
