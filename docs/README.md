# YDB SQLAlchemy Documentation

This directory contains the documentation for YDB SQLAlchemy dialect.

## Building Documentation

### Prerequisites

1. Install Sphinx and required extensions:
```bash
pip install sphinx sphinx-rtd-theme sphinx-copybutton
```

### Building HTML Documentation

1. Navigate to the docs directory:
```bash
cd docs
```

2. Build the documentation:
```bash
make html
```

3. Open the documentation in your browser:
```bash
open .build/html/index.html
```

### Building Other Formats

- **PDF**: `make latexpdf` (requires LaTeX)
- **EPUB**: `make epub`
- **Man pages**: `make man`

### Development

When adding new documentation:

1. Create `.rst` files in the appropriate directory
2. Add them to the `toctree` in `index.rst`
3. Rebuild with `make html`
4. Check for warnings and fix them

### Structure

- `index.rst` - Main documentation page
- `installation.rst` - Installation guide
- `quickstart.rst` - Quick start guide
- `connection.rst` - Connection configuration
- `types.rst` - Data types documentation
- `migrations.rst` - Alembic migrations guide
- `api/` - API reference documentation
- `conf.py` - Sphinx configuration
- `_static/` - Static files (images, CSS, etc.)

### Configuration

The documentation is configured in `conf.py`. Key settings:

- **Theme**: `sphinx_rtd_theme` (Read the Docs theme)
- **Extensions**: autodoc, napoleon, intersphinx, copybutton
- **Intersphinx**: Links to Python, SQLAlchemy, and Alembic docs

### Troubleshooting

**Sphinx not found**: Make sure Sphinx is installed in your virtual environment

**Import errors**: Ensure the YDB SQLAlchemy package is installed in the same environment

**Theme issues**: Install `sphinx-rtd-theme` if you get theme-related errors
