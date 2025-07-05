---
description: RRQ Development Guidelines
globs: ["**/*", "!tests/**/*"]
alwaysApply: true
---
# RRQ Development Guidelines

## Project Overview

**Important**: Also refer to:
- `tests/CLAUDE.md` - Testing guidelines
- `README.md` - Project setup and overview

## Quick Start Commands
```bash
# Backend
uv run pytest             # Running tests
ruff check                # Lint backend code
ruff format               # Format backend code

# Package Management
uv add package_name        # Add dependency
uv sync --extra dev        # Sync dependencies
```

## Code References Format
**CRITICAL**: Always use VS Code clickable format for code references:
- `app/api/users.py:45` - Single line
- `app/models/job.py:120-135` - Line range
- Always use paths relative to project root

### Python Code Style
- Python 3.10+ with comprehensive type hints
- Double quotes for strings
- Max line length: 88 characters
- PEP 8 naming: `snake_case` functions, `PascalCase` classes
- Pydantic V2 for data validation
- Import order: stdlib → third-party → local
- Docstrings for public interfaces
- Type annotations for all function signatures

### Code Quality Practices
- Early returns over nested conditions
- Small, focused functions (single responsibility)
- Descriptive variable names
- Comprehensive error handling with specific exceptions
- Consistent async/await patterns
- Use `match/case` for complex conditionals

## Self-Review Checklist for Large Changes

Before submitting significant backend changes:

### Code Quality
- [ ] All functions have type hints
- [ ] Complex logic is well-commented
- [ ] No debug prints or commented code
- [ ] Follows existing patterns in codebase
- [ ] Proper error handling throughout
- [ ] Idiomatic code using Python, Asyncio, and Pydantic best practices

### Testing
- [ ] Unit tests for new functionality
- [ ] Integration tests for API changes
- [ ] All tests pass with `--warnings-as-errors`
- [ ] Edge cases covered
- [ ] Mocked external dependencies

### Performance
- [ ] Database queries are optimized (N+1 prevention)
- [ ] Async operations are properly awaited
- [ ] No blocking I/O in async contexts
- [ ] Background jobs for heavy operations

### Security
- [ ] Input validation on all endpoints
- [ ] No sensitive data in logs
- [ ] SQL injection prevention

### Documentation
- [ ] API endpoints documented
- [ ] Complex functions have docstrings
- [ ] Schema changes documented
- [ ] Migration files created if needed

## Linting and Pre-commit

**Always run before committing:**
```bash
# Format and lint
ruff format 
ruff check  --fix

```

## Important Development Rules

1. **Never commit broken tests** - Fix root causes
2. **Ask before large changes** - Especially cross-domain
3. **Follow existing patterns** - Check similar code first
4. **Quality over speed** - Do it right the first time
5. **Security first** - Never expose sensitive data

## Performance Considerations
- Profile before optimizing
- Use async correctly (no sync in async)
- Cache expensive operations
- Paginate large result sets
- Monitor query performance

## Debugging Tips
- Use `uv run pytest --maxfail=1` for failing tests
- Use debugger with `import pdb; pdb.set_trace()`

Remember: If unsure about implementation, check existing code patterns first!
