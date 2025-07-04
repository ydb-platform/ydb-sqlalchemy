import sqlalchemy as sa

sa_version = sa.__version__

if sa_version.startswith("2."):
    from .sa20 import (
        YqlCompiler,
        YqlDDLCompiler,
        YqlIdentifierPreparer,
        YqlTypeCompiler,
    )
elif sa_version.startswith("1.4."):
    from .sa14 import (
        YqlCompiler,
        YqlDDLCompiler,
        YqlIdentifierPreparer,
        YqlTypeCompiler,
    )
else:
    raise RuntimeError("Unsupported SQLAlchemy version.")
