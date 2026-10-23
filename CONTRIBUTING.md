# Contributing to Lakehouse Engine

Thank you for your interest in contributing to **Lakehouse Engine**! We welcome contributions to declarative ETL, autonomous data self-healing, data quality expectations, and AI agentic metadata architectures.

---

## 🛠️ Development Setup

### 1. Prerequisites
* Python 3.10 or 3.11
* [Task](https://taskfile.dev/) runner (`brew install go-task`)
* Git

### 2. Clone and Setup Environment
```bash
git clone https://github.com/anilkulkarni87/databricks-lakehouse-boilerplate.git
cd databricks-lakehouse-boilerplate

python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 3. Verify Local Unit Tests
All unit tests are 100% self-contained and run in under 0.1 seconds without requiring an external Spark cluster or cloud connection:
```bash
task test
```

---

## 📐 Key Development Workflows

### Exporting & Validating JSON Schema
When adding new fields to Pydantic configuration models (`src/lakehouse_engine/models/config.py`), update and export the JSON Schema:
```bash
task schema:export
```

### Scaffolding New Entities
To test schema inference and scaffolding:
```bash
task scaffold -- -n orders -t raw_landing.orders_events -s configs/examples/sample_dlq_records.json
```

### Autonomous DLQ Triage
To test autonomous DLQ diagnosis and self-healing patches:
```bash
task triage -- -c configs/examples/subscriptions.yml -s configs/examples/sample_dlq_records.json
```

---

## 📋 Pull Request Guidelines

1. **Create a Feature Branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. **Ensure All Tests Pass**:
   Run `task test`. All existing and new tests must pass cleanly.
3. **Keep Commits Clean & Conventional**:
   Use conventional commits (e.g., `feat:`, `fix:`, `docs:`, `test:`).
4. **No Breaking Schema Changes**:
   Ensure existing YAML templates in `configs/` continue to validate.
5. **Open a Pull Request**:
   GitHub Actions CI will automatically test and validate your changes against Python 3.10 and 3.11.
