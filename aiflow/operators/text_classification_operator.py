from airflow.models import BaseOperator
from pathlib import Path
import json
from bson import json_util
from aiflow.units.doc2mat_unit import Doc2MatUnit
import logging
import pandas as pd
import numpy as np
import shutil


logger = logging.getLogger(__name__)


class TextClassificationDataBuildOperator(BaseOperator):
    """
    This operator allows you to build dataset for text classification
    """

    def __init__(self, input_file, data_column, label_column, output_data_dir, output_extra_file, word2vec_file, *args, **kwargs):
        super(TextClassificationDataBuildOperator, self).__init__(*args, **kwargs)

        self.input_file = Path(input_file)
        self.data_column = data_column
        self.label_column = label_column
        self.output_data_dir = Path(output_data_dir)
        self.output_extra_file = Path(output_extra_file)

        n_max_words_count = kwargs.get('n_max_words_count', 5)
        self.doc2mat = Doc2MatUnit(n_max_words_count, word2vec_file, *args, **kwargs)

    def execute(self, context):
        logger.debug('start TextClassificationDataBuildOperator ...')

        assert self.input_file.name.lower().endswith('csv'), f'input file must be CSV.'
        assert self.input_file.exists() and self.input_file.is_file, f'invalid input_file: {input_file}'

        # prepare output env
        shutil.rmtree(self.output_data_dir, ignore_errors=True)
        self.output_data_dir.mkdir(exist_ok=True, parents=True)
        self.output_extra_file.parent.mkdir(exist_ok=True, parents=True)
        if self.output_extra_file.exists() and self.output_extra_file.is_file:
            self.output_extra_file.unlink()

        df = pd.read_csv(self.input_file)

        n_vector_dim = 300

        ohlabels = pd.get_dummies(df[self.label_column])
        with open(self.output_extra_file.absolute().as_posix(), 'w+') as f:
            f.write(json.dumps(dict(
                classes=list(ohlabels.columns)
            )))

        for i, row in df.iterrows():
            val = row[self.data_column]
            x = self.doc2mat.execute(sentence='' if pd.isna(val) else val)
            y = ohlabels.iloc[i].to_numpy()
            np.savez((self.output_data_dir / f'{i}.npz').absolute().as_posix(), x=x, y=y)
            if i % 10000 == 0:
                logger.debug(f'finish embedding X for {i} rows')

    def _prepare_output_dir(self, output_file):
        output_file.parent.mkdir(exist_ok=True, parents=True)
        if output_file.exists() and output_file.is_file:
            output_file.unlink()
        return self
