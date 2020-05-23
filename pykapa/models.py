from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from pykapa.settings.db import Base, engine, Session


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    fullname = Column(String)
    nickname = Column(String)

    def __repr__(self):
       return "<User(name='%s', fullname='%s', nickname='%s')>" % (
                            self.name, self.fullname, self.nickname)


class SurveyForm(Base):
    __tablename__ = 'survey_form'

    id = Column(Integer, primary_key=True)
    # name / form_id
    form_id = Column(String)
    rows = relationship("SFormRow")


class SFormRow(Base):
    __tablename__ = 'survey_row'

    id = Column(Integer, primary_key=True)
    survey = Column(Integer, ForeignKey("survey_form.id"))

    completed = Column(DateTime, nullable=False, index=True)
    uuid_scto = Column(String, index=True, unique=True)
    processed_on = Column(DateTime, nullable=True, index=True)

    blob = Column(String)



Base.metadata.create_all(engine)
session = Session()
