import os

import pykapa
from pykapa.settings.config import load_config_file, save_config_to_file
from pykapa.models import User
from pykapa.controllers.files import load_file_to_dataframe, save_dataframe_to_file, save_dict_to_file, load_file_to_dict
from pykapa.controllers.survey_cto import SurveyCTOContoller
from pykapa.settings.config import get_test_path_for_filename
from pykapa.qcMessenger import run_quality_control
from pykapa.tests.base import PykapaTestCase
from datetime import datetime
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

    def test_survey_rows(self):
        server = "https://ikapadata.surveycto.com"
        username = "bruce@ikapadata.com"
        password = "5vy_2019_c70_B0Y"
        form_id = "audit_quality"

        scto = SurveyCTOContoller(server, form_id, username, password, self.session)
        # jsond = scto.get_latest_survey_results()
        # save_dict_to_file(jsond, get_test_path_for_filename('t1.json'))
        uuid_test = "uuid:74f612f4-da82-48b6-8551-4d8ae672b46c"

        data = load_file_to_dict(get_test_path_for_filename('t1.json'))
        scto.create_database_rows(data)
        scto.create_database_rows(data)
        scto.create_database_rows(data)
        k = scto.get_row_by_uuid(uuid_test)
        assert k.uuid_scto
        all_dates = scto.get_completion_ordered_rows()
        unprocessed = scto.get_unprocessed_rows()
        assert scto.get_unprocessed_rows() != []
        for row in unprocessed:
            row.processed_on = datetime.now()
            self.session.add(row)
        self.session.commit()
        scto.create_database_rows(data)
        assert scto.get_unprocessed_rows() == []

    def test_qc(self):
        run_quality_control()



