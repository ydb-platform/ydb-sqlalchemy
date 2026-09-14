import subprocess
import sys


def test_module_import_does_not_require_alembic():
    script = """
import sys


class BlockAlembicImport:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "alembic" or fullname.startswith("alembic."):
            raise ModuleNotFoundError("No module named 'alembic'", name="alembic")
        return None


sys.meta_path.insert(0, BlockAlembicImport())

from ydb_sqlalchemy.alembic import YDBImpl

try:
    YDBImpl()
except ModuleNotFoundError as error:
    assert error.name == "alembic"
    assert "Alembic 1.14 or later is required" in str(error)
else:
    raise AssertionError("YDBImpl must not be usable without Alembic")
"""

    subprocess.run([sys.executable, "-c", script], check=True)
