from types import MethodType

import pytest
import sqlalchemy as sa
from sqlalchemy import Column, Integer, Unicode
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.testing.fixtures import TablesTest, config


class TestDirectories(TablesTest):
    __backend__ = True

    def prepare_table(self, engine):
        base = declarative_base()

        class Table(base):
            __tablename__ = "dir/test"
            id = Column(Integer, primary_key=True)
            text = Column(Unicode)

        base.metadata.create_all(engine)
        session = sessionmaker(bind=engine)()
        session.add(Table(id=2, text="foo"))
        session.commit()
        return base, Table, session

    def try_update(self, session, Table):
        row = session.query(Table).first()
        row.text = "bar"
        session.commit()
        return row

    def drop_table(self, base, engine):
        base.metadata.drop_all(engine)

    def bind_old_method_to_dialect(self, dialect):
        def _handle_column_name(self, variable):
            return variable

        dialect._handle_column_name = MethodType(_handle_column_name, dialect)

    def test_directories(self):
        engine_good = sa.create_engine(config.db_url)
        base, Table, session = self.prepare_table(engine_good)
        row = self.try_update(session, Table)
        assert row.id == 2
        assert row.text == "bar"
        self.drop_table(base, engine_good)

        engine_bad = sa.create_engine(config.db_url)
        self.bind_old_method_to_dialect(engine_bad.dialect)
        base, Table, session = self.prepare_table(engine_bad)
        with pytest.raises(Exception) as excinfo:
            self.try_update(session, Table)
        assert "Unknown name: $dir" in str(excinfo.value)
        self.drop_table(base, engine_bad)
