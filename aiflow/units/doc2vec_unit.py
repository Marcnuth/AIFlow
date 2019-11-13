from . import Unit
import logging
import string
import re
from itertools import filterfalse
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from gensim.models import KeyedVectors
from pathlib import Path
import numpy as np


logger = logging.getLogger(__name__)


class Doc2VecUnit(Unit):

    def __init__(self, word2vec_model_url, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.lower = kwargs.get('lower', False)
        self.hyphen_replacement = kwargs.get('hyphen_replacement', None)
        self.remove_stopwords = kwargs.get('remove_stopwords', True)
        self.remove_numeric = kwargs.get('remove_numeric', True)
        self.lemmatize = kwargs.get('lemmatize', False)

        # todo: support load from website
        self.word2vec = KeyedVectors.load_word2vec_format(Path(word2vec_model_url).absolute().as_posix())

        self.n_ouput_dim = kwargs.get('n_ouput_dim', 300)

    def execute(self, **kwargs):

        words = self._sentence_to_words(kwargs.get('sentence', '').strip())
        vectors = list(filter(bool, map(self._safe_word_to_vector, words)))
        if not vectors:
            return np.zeros(self.n_ouput_dim)

        return np.concatenate(vectors).reshape(len(vectors), -1).mean(axis=0)

    def _sentence_to_words(self, sentence):
        if not sentence or not sentence.strip():
            return sentence

        s = sentence.lower() if self.ower else sentence
        s = s.strip().replace('-', self.hyphen_replacement)

        words = re.split(r'[\s{}]'.format(string.punctuation), s)
        words = list(filterfalse(str.isnumeric, words)) if self.remove_numeric else words
        words = list(filter(lambda x: x not in stopwords.words('english'), words)) if self.remove_stopwords else words

        lemmatizer = WordNetLemmatizer()
        words = list(map(lemmatizer.lemmatize, words)) if self.lemmatize else words

        return list(filter(bool, words))

    def _safe_word_to_vector(self, word):
        if word in self.word2vec:
            return self.word2vec[word]
        else:
            return None
