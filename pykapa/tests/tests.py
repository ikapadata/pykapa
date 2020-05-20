import os

import pykapa
from pykapa.gen_funcs import load_config_file, save_config_to_file
from pykapa.models import User
from pykapa.tests.base import PykapaTestCase
import pandas as pd


class TestGenFuncs(PykapaTestCase):

    def test_yaml_files(self):
        file = load_config_file('test')
        path = os.path.dirname(pykapa.__file__)
        if not file:
            d = {'data': {
                'something': True,
                'somepath': 'some/long/path',
            }}
            save_config_to_file(d, "test")

    def test_db_sessions(self):
        ed = User(name='ed', fullname='Ed Jones', nickname='edsnickname')
        self.session.add(ed)
        self.session.commit()
        people = self.session.query(User).all()
        assert len(people) > 0
        assert len(people) < 2

    def test_dataframe_to_sql(self):

        df = pd.DataFrame({'name' : ['User 1', 'User 2', 'User 3']})
        df.to_sql('testpd', con=self.test_engine, if_exists='append')
        users = self.test_engine.execute("SELECT * FROM testpd").fetchall()
        self.test_engine.execute("Drop table testpd")

