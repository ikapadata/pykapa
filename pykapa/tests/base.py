from unittest import TestCase
from pykapa.models import Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class PykapaTestCase(TestCase):

    def setUp(self) -> None:
        self.test_engine = create_engine('sqlite:///test.db', echo=False)
        Session = sessionmaker(bind=self.test_engine)
        self.session = Session()
        Base.metadata.create_all(self.test_engine)

    def tearDown(self) -> None:
        Base.metadata.drop_all(self.test_engine)


