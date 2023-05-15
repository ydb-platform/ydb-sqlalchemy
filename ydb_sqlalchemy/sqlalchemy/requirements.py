from sqlalchemy.testing import exclusions
from sqlalchemy.testing.requirements import SuiteRequirements


class Requirements(SuiteRequirements):
    @property
    def array_type(self):
        return exclusions.closed()

    @property
    def uuid_data_type(self):
        return exclusions.open()

    @property
    def nullable_booleans(self):
        return exclusions.closed()

    @property
    def foreign_keys(self):
        # foreign keys unsupported
        return exclusions.closed()

    @property
    def self_referential_foreign_keys(self):
        return exclusions.closed()

    @property
    def foreign_key_ddl(self):
        return exclusions.closed()

    @property
    def foreign_key_constraint_reflection(self):
        return exclusions.closed()

    @property
    def temp_table_reflection(self):
        return exclusions.closed()

    @property
    def temporary_tables(self):
        return exclusions.closed()

    @property
    def temporary_views(self):
        return exclusions.closed()

    @property
    def index_reflection(self):
        return exclusions.closed()

    @property
    def view_reflection(self):
        return exclusions.closed()

    @property
    def unique_constraint_reflection(self):
        return exclusions.closed()

    @property
    def insert_returning(self):
        return exclusions.closed()

    @property
    def autoincrement_insert(self):
        # YDB doesn't support autoincrement
        return exclusions.closed()

    @property
    def autoincrement_without_sequence(self):
        # YDB doesn't support autoincrement
        return exclusions.closed()

    @property
    def duplicate_names_in_cursor_description(self):
        return exclusions.closed()

    @property
    def regexp_match(self):
        return exclusions.open()
