
A data processing pipeline for analyzing client data by age and location, with support for both CSV and Parquet output formats and comprehensive testing.
## 🚀 Overview

This project processes client data to analyze demographics by age and location. It takes client information and city/zip code data as input, filters clients based on age criteria, enriches the data with department information, and outputs the results in both CSV and Parquet formats.
## ✨ Features
![img.png](img.png)
- **Data Processing**:
  - Filter clients by age threshold
  - Merge client data with city/zip code information
  - Add department information based on zip codes
  - Handle missing or invalid data gracefully

- **Input/Output**:
  - Load data from CSV files
  - Export results to both CSV and Parquet formats
  - Automatic directory creation for output files
  - Configurable file paths and output formats

- **Testing & Quality**:
  - Comprehensive unit tests with pytest
  - Type hints for better code quality
  - GitHub Actions CI/CD pipeline
  - Code coverage reporting

## ️ Project Structure

```
.
├── .github/
│   └── workflows/
│       └── build.yml          # GitHub Actions CI/CD configuration
├── resources/
│   ├── city_zipcode.csv       # City and zip code reference data
│   └── clients_bdd.csv        # Client database
├── src/
│   └── country_age_analyses/
│       ├── scripts/
│       │   ├── data_processing.py  # Core data transformation logic
│       │   ├── io_utils.py         # File I/O operations
│       │   ├── pipeline.py         # Main processing pipeline
│       │   └── utils.py            # Utility functions
│       └── __init__.py
├── tests/
│   ├── resources/
│   │   └── input/             # Test input data
│   ├── test_data_processing.py # Tests for data processing
│   ├── test_export_data.py     # Tests for export functionality
│   └── test_run_pipeline.py    # Integration tests
├── .gitignore
├── pyproject.toml             # Project configuration and dependencies
├── README.md                  # This file
└── requirements.txt           # Project dependencies
```

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- pip (Python package manager)
- Git

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/djeFerroudja/customer-data-pipeline.git
   cd customer-data-pipeline
   ```

2. Create and activate a virtual environment (recommended):
   ```bash
   python -m venv .venv
   .\.venv\Scripts\activate
   ```


## 🏃‍♂️ Usage

### Running the Pipeline

To process the client data:
```bash
python -m country_age_analyses.scripts.pipeline
```

### Running Tests

Run all tests:
```bash
pytest
```

Run tests with coverage report:
```bash
pytest --cov=src
```

### Development

Before committing code, run the following to ensure code quality:

```bash
# Run linter
pylint src tests

# Run formatter
black src tests

# Run type checking
mypy src
```

## 🔧 Configuration

The main configuration can be found in the pipeline script. You can modify:
- Input file paths
- Output directories
- Age threshold for filtering
- Output formats (CSV/Parquet)
## 🧪 Testing

Run the test suite:
```bash
python -m pytest tests/
```

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request



## 📧 Contact

[Ferroudja DJELLALI] - [ferroudja.djellali@gmail.com]" > README.md