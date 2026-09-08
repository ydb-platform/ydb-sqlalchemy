# Working in this repository

## CHANGELOG

`CHANGELOG.md` has no "unreleased" section. The pending release notes are simply the bullets above the first `## ` line; that first `## ` line is the latest *released* version.

- Add a single `*` bullet to the very top of `CHANGELOG.md` **only** when a change is meaningful to the end user — new or changed behavior, a bug fix, a performance or compatibility improvement.
- Do **not** add entries for internal-only work such as tests, coverage, CI, refactors, or documentation.
- **Never add a heading of any kind.** Not `## Unreleased`, not `## [Unreleased]`, not `## Next`, not a date, not `## X.Y.Z ##`, and no `### Added` / `### Fixed` subsections. Release automation (`.github/scripts/increment_version.py`) inserts the version header above the pending bullets at release time.
- Do not restyle the file to Keep a Changelog format, and do not reorder or reword existing entries.

This is not a style preference — a heading at the top **breaks the release**. `.github/workflows/python-publish.yml` reads the release notes as everything above the first `## ` line (`sed -e '/^## .*$/,$d'`), so a leading heading makes them empty and the publish job fails with `CHANGELOG empty`. `add_changelog_version` in `increment_version.py` then also skips inserting the real version header, because it returns early when the file already starts with `##`.

Correct — bullets first, no heading of your own:

```md
* Support YDB view reflection
* Fix dialect import failure on SQLAlchemy 1.4

## 0.1.22 ##
* Ignore table schema in reflection and SQL compilation
```

Wrong:

```md
## Unreleased

### Added
* Support YDB view reflection

## 0.1.22 ##
```

## Tests

`tox -e test-unit` runs the unit tests; `tox -e test-dialect` brings up YDB in Docker and runs the dialect, Alembic and compliance suites against it. `tox -e style` and `tox -e black` are what CI checks for formatting.

Integration tests share one database, so give any table a name unique to the test that creates it rather than assuming an empty schema.
