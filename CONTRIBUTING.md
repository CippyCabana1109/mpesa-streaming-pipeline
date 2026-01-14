# Contributing to M-Pesa Streaming Pipeline

Thank you for your interest in contributing to the M-Pesa Streaming Pipeline project! This document provides guidelines and instructions for contributing.

## Code of Conduct

- Be respectful and inclusive
- Welcome newcomers and help them learn
- Focus on constructive feedback
- Respect different viewpoints and experiences

## How to Contribute

### Reporting Bugs

1. Check if the bug has already been reported in [Issues](https://github.com/CippyCabana1109/mpesa-streaming-pipeline/issues)
2. If not, create a new issue with:
   - Clear title and description
   - Steps to reproduce
   - Expected vs actual behavior
   - Environment details (OS, Python version, etc.)
   - Relevant logs or error messages

### Suggesting Features

1. Check existing feature requests in [Issues](https://github.com/CippyCabana1109/mpesa-streaming-pipeline/issues)
2. Create a new issue with:
   - Clear description of the feature
   - Use case and motivation
   - Proposed implementation approach (if applicable)

### Pull Requests

1. **Fork the repository** and clone your fork
2. **Create a feature branch**: `git checkout -b feature/your-feature-name`
3. **Make your changes**:
   - Follow the code style (see below)
   - Add tests for new functionality
   - Update documentation as needed
   - Ensure all tests pass
4. **Commit your changes**: `git commit -m "Add: descriptive commit message"`
5. **Push to your fork**: `git push origin feature/your-feature-name`
6. **Create a Pull Request** with:
   - Clear title and description
   - Reference related issues
   - Screenshots/demos if applicable

## Development Setup

### Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Git

### Setup Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/CippyCabana1109/mpesa-streaming-pipeline.git
   cd mpesa-streaming-pipeline
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt  # If available
   ```

4. **Start infrastructure**
   ```bash
   docker-compose up -d
   ```

5. **Run tests**
   ```bash
   pytest tests/ -v
   ```

## Code Style

### Python Style Guide

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/)
- Use type hints where possible
- Maximum line length: 120 characters
- Use descriptive variable and function names
- Add docstrings to all public functions and classes

### Formatting

We use `black` for code formatting and `isort` for import sorting:

```bash
# Format code
black src/ tests/

# Sort imports
isort src/ tests/

# Check formatting
black --check src/ tests/
isort --check-only src/ tests/
```

### Linting

We use `flake8` for linting:

```bash
flake8 src/ tests/ --max-line-length=120 --max-complexity=10
```

### Type Checking

We use `mypy` for type checking:

```bash
mypy src/ --ignore-missing-imports
```

## Testing

### Writing Tests

- Write tests for all new functionality
- Use `pytest` for testing
- Place tests in the `tests/` directory
- Follow the naming convention: `test_*.py`
- Aim for >80% code coverage

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run specific test file
pytest tests/test_pipeline.py -v

# Run integration tests
pytest tests/ --run-integration -v
```

## Documentation

### Code Documentation

- Add docstrings to all public functions, classes, and methods
- Use Google-style docstrings:
  ```python
  def function_name(param1: str, param2: int) -> bool:
      """Brief description.
      
      Longer description if needed.
      
      Args:
          param1: Description of param1
          param2: Description of param2
      
      Returns:
          Description of return value
      
      Raises:
          ValueError: When something goes wrong
      """
  ```

### README Updates

- Update README.md when adding new features
- Include examples and usage instructions
- Update architecture diagrams if needed

## Commit Messages

Follow conventional commit format:

- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `style:` Code style changes (formatting, etc.)
- `refactor:` Code refactoring
- `test:` Adding or updating tests
- `chore:` Maintenance tasks

Example:
```
feat: Add ML-based anomaly detection

- Implement Isolation Forest model
- Add feature engineering pipeline
- Integrate with stream processor
- Add model training script
```

## Review Process

1. All PRs require at least one review
2. Address review comments promptly
3. Keep PRs focused and reasonably sized
4. Ensure CI/CD checks pass
5. Update documentation as needed

## Questions?

- Open an issue for questions or discussions
- Check existing issues and discussions
- Review the README for project overview

Thank you for contributing! 🚀

