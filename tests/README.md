# SMUS CI/CD Tests

> **[Preview]** Amazon SageMaker Unified Studio CI/CD CLI is currently in preview and is subject to change. Commands, configuration formats, and APIs may evolve based on customer feedback. We recommend evaluating this tool in non-production environments during preview. For feedback and bug reports, please open an issue https://github.com/aws/CICD-for-SageMakerUnifiedStudio/issues

See [developer-guide.md](../developer-guide.md) for comprehensive testing documentation.

## Quick Start

```bash
# Run all tests
python tests/run_tests.py --type all

# Run integration tests in parallel
python tests/run_tests.py --type integration --parallel

# Run specific test
pytest tests/integration/examples-analytics-workflows/dashboard-glue-quick/ -v -s
```

## Structure

- `unit/` - Unit tests (fast, no AWS)
- `integration/` - Integration tests (real AWS resources)
- `run_tests.py` - Test runner with parallel support
- `scripts/` - Setup and utility scripts
