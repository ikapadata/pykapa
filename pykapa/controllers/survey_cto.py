from requests.auth import HTTPBasicAuth

from sqlalchemy.orm.session import Session
from pykapa.settings.logging import logger
from pykapa.models import SurveyForm, SFormRow
from datetime import datetime
from pykapa.incentives_functions import *


class SurveyCTOContoller():
    """
    SurveyCTO Controller

    """

    def __init__(self, domain, form_id, username, password, session, date_format="%b %d, %Y %I:%M:%S %p"):
        self.domain = domain
        self.form_id = form_id
        self.session: Session = session
        self.base_url = self.domain + '/api/v1/forms/data/wide/json/' + form_id
        self.username = username
        self.password = password
        self.date_format = date_format
        logger.info("Configured SCTO Controller for form %s to: %s", form_id, self.base_url)

    def get_latest_survey_results(self):
        resp = requests.get(self.base_url, auth=HTTPBasicAuth(self.username, self.password))
        assert resp.status_code == 200
        json_data = resp.json()
        return json_data

    def get_this_survey(self):
        survey = self.session.query(SurveyForm).filter(SurveyForm.form_id==self.form_id).one_or_none()
        if not survey:
            survey = SurveyForm(form_id=self.form_id)
            self.session.add(survey)
            self.session.commit()
        return survey

    def get_clean_uuid(self, dirty_uuid):
        return dirty_uuid[dirty_uuid.find("uuid:")+5:]

    def get_row_by_uuid(self, uuid):
        clean_uuid = self.get_clean_uuid(uuid)
        return self.session.query(SFormRow).filter(SFormRow.uuid_scto == clean_uuid).one()

    def get_completion_ordered_rows(self):
        return self.session.query(SFormRow).order_by(SFormRow.completed).all()

    def parse_date(self, date_string):
        # example: "Mar 17, 2020 4:41:14 PM"
        dt = datetime.strptime(date_string, self.date_format)
        return dt

    def create_database_rows(self, json_data):
        '''
        Not handling upserts, just expecting pure data by uuid
        :param json_data:
        :return:
        '''
        survey = self.get_this_survey()
        for row in json_data:

            uuid = self.get_clean_uuid(row.get('KEY'))
            completed_dt = self.parse_date(row.get("CompletionDate"))
            sform_row = self.session.query(SFormRow).filter(SFormRow.uuid_scto == uuid).one_or_none()
            if not sform_row:
                sform_row = SFormRow(uuid_scto=uuid, completed=completed_dt, blob=json.dumps(row))
                self.session.add(sform_row)
        self.session.commit()

    def get_unprocessed_rows(self):
        return self.session.query(SFormRow).filter(SFormRow.processed_on == None).order_by(SFormRow.completed).all()
