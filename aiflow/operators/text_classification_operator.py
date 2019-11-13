from airflow.models import BaseOperator
from pathlib import Path
import json
from bson import json_util
from aiflow.hooks.mongo_hook import MongoHook
from aiflow.units.doc2vec_unit import Doc2VecUnit
import logging
from dictor import dictor
import pandas as pd
import numpy as np


logger = logging.getLogger(__name__)


class TextClassificationDataBuildOperator(BaseOperator):
    """
    This operator allows you to build dataset for text classification
    """

    def __init__(self, input_file, data_column, label_column, output_data_file, output_dummies_file, word2vec_file, *args, **kwargs):
        super(TextClassificationDataBuildOperator, self).__init__(*args, **kwargs)

        self.input_file = Path(input_file)
        self.data_column = data_column
        self.label_column = label_column
        self.output_data_file = Path(output_data_file)
        self.output_dummies_file = Path(output_dummies_file)

        assert self.input_file.name.lower().endswith('csv'), f'"csv" input file required'
        assert self.input_file.exists() and self.input_file.is_file, f'invalid input_file: {input_file}'

        self.doc2vec = Doc2VecUnit(word2vec_file, *args, **kwargs)

    def execute(self, context):
        logger.debug('start TextClassificationDataBuildOperator ...')

        self._prepare_output_dir(self.output_data_file)._prepare_output_dir(self.output_dummies_file)
        df = pd.read_csv(self.input_file)

        n_vector_dim = 300
        X = np.zeros((df.shape[0], n_vector_dim))
        for i, row in df.iterrows():
            X[i, ] = self.doc2vec.execute(sentence=row[self.data_column])

        dummies = pd.get_dummies(df[self.label_column])
        Y = dummies.to_numpy()
        lookup = dummies.idxmax(axis=1)

        np.savez(self.output_data_file.absolute().as_posix(), x=X, y=Y)
        lookup.to_json(self.output_dummies_file)

    def _prepare_output_dir(self, output_file):
        output_file.parent.mkdir(exist_ok=True, parents=True)
        if output_file.exists() and output_file.is_file:
            output_file.unlink()
        return self
