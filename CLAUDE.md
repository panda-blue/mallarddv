# MallardDV Coding Guidelines

## Build & Setup
- Install dependencies: `pip install -r requirements.txt`
- Install dev dependencies: `pip install -r dev-requirements.txt`

## Testing Commands
- Run all tests: `coverage run -m pytest -xvs`
- Run single test: `coverage run -m pytest -xvs test_mallardv.py::TestMallardDataVault::test_name`
- Generate coverage report: `coverage html`

## Code Style
- Formatting: Black (v22.10.0)
- Imports: Standard library first, third-party second, local modules last
- Types: Use type hints (Optional, List, Dict, Any)
- Naming: snake_case for variables/functions, PascalCase for classes
- Error handling: Use try/except with structured error collection
- Docstrings: Google-style with Args/Returns sections
- Pre-commit hooks ensure all code meets quality standards

## Additional Notes
- DuckDB is the underlying database engine
- Data Vault methodology implementation
- Verify test coverage before submitting changes