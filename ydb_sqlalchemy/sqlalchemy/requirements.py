from sqlalchemy.testing import exclusions
from sqlalchemy.testing.requirements import SuiteRequirements


class Requirements(SuiteRequirements):
    @property
    def array_type(self):
        return exclusions.closed()

    @property
    def uuid_data_type(self):
        return exclusions.open()
