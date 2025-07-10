# Contributing to The Nine Cycle Project

Thank you for your interest in contributing to The Nine Cycle Project! This document provides guidelines and information for contributors.

## 🤝 Code of Conduct

We are committed to providing a welcoming and inspiring community for all. Please be respectful and constructive in all interactions.

## 🚀 Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/your-username/theninecycle.git
   cd theninecycle
   ```
3. **Set up the development environment** following the README instructions
4. **Create a branch** for your feature or fix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

## 📝 Development Guidelines

### Code Style

- Follow **PEP 8** Python style guidelines
- Use **Black** for code formatting: `black src/`
- Use **type hints** for all function parameters and return values
- Write **docstrings** for all classes and functions
- Keep line length to **88 characters** (Black default)

### Code Quality

```bash
# Format code
black src/ scripts/ tests/

# Lint code
flake8 src/ scripts/ tests/

# Type checking
mypy src/

# Run tests
pytest tests/ -v --cov=src
```

### Commit Messages

Use clear, descriptive commit messages following this format:

```
type(scope): brief description

Detailed explanation if needed

- Additional details
- Breaking changes
```

**Types:**
- `feat`: New features
- `fix`: Bug fixes
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or modifying tests
- `chore`: Maintenance tasks

**Examples:**
```
feat(collectors): add support for economic data collection
fix(database): resolve connection timeout issues
docs(readme): update installation instructions
```

## 🧪 Testing

- Write tests for all new functionality
- Ensure all tests pass before submitting PR
- Aim for >90% code coverage
- Test both success and error scenarios

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run specific test file
pytest tests/test_collectors.py -v
```

## 📊 Contributing Areas

### High Priority
- **Data Collection**: New data sources and collectors
- **Pattern Analysis**: Cycle detection algorithms
- **Data Validation**: Quality assurance improvements
- **Performance**: Optimization and scalability

### Documentation
- Code documentation and examples
- API documentation
- User guides and tutorials
- Architecture documentation

### Testing
- Unit tests for all modules
- Integration tests
- Performance benchmarks
- Error handling tests

## 🔧 Project Structure

```
src/
├── collectors/          # Data collection modules
├── utils/              # Utility functions
├── analysis/           # Data analysis (future)
└── api/               # API endpoints (future)

scripts/               # Utility scripts
tests/                # Test suite
docs/                 # Documentation
data/                 # Data storage
```

## 📋 Pull Request Process

1. **Ensure your code follows** the style guidelines
2. **Add or update tests** for your changes
3. **Update documentation** if needed
4. **Run the full test suite** and ensure it passes
5. **Create a pull request** with:
   - Clear title and description
   - Reference to related issues
   - List of changes made
   - Screenshots if applicable

### PR Checklist

- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Tests added/updated and passing
- [ ] Documentation updated
- [ ] No breaking changes (or clearly documented)
- [ ] Commit messages are clear and descriptive

## 🐛 Reporting Issues

When reporting issues, please include:

- **Clear description** of the problem
- **Steps to reproduce** the issue
- **Expected vs actual behavior**
- **Environment details** (OS, Python version, etc.)
- **Error messages** and stack traces
- **Minimal code example** if applicable

## 💡 Feature Requests

For feature requests, please provide:

- **Clear description** of the proposed feature
- **Use case** and motivation
- **Possible implementation** approach
- **Impact** on existing functionality

## 📞 Getting Help

- **GitHub Issues**: For bugs and feature requests
- **Discussions**: For questions and general discussion
- **Documentation**: Check existing docs first

## 🏆 Recognition

Contributors will be acknowledged in:
- README contributors section
- Release notes for significant contributions
- Special recognition for major contributions

Thank you for contributing to The Nine Cycle Project!
