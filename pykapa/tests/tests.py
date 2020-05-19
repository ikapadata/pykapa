from pprint import pprint
from unittest import skip

import logging
from datetime import date, datetime, timedelta
from unittest import TestCase
ae = TestCase().assertEqual
from pykapa.gen_funcs import user_inputs, load_config_file, save_config_to_file
from pykapa.config import logger
import pykapa
import os


class TestGenFuncs(TestCase):

    def test_yaml_files(self):
        file = load_config_file('test')
        path = os.path.dirname(pykapa.__file__)
        if not file:
            d = {'data': {
                'something': True,
                'somepath': 'some/long/path',
            }}
            save_config_to_file(d, "test")
        logger.info(file)
        assert False



