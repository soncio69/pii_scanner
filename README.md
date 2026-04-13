# PII Scanner for Oracle Databases

A tool that scans multiple Oracle database schemas to identify columns containing Personally Identifiable Information (PII), with a focus on Italian banking contexts.

## Features

- **Hybrid Detection**: Combines three detection methods for comprehensive PII identification
- **Smart Filtering**: Automatically excludes numeric PK/FK ID columns from PII detection
- **Italian Banking Focus**: Pre-configured patterns for Italian PII types (codice fiscale, NDG, IBAN, etc.)
- **Excel Output**: Results exported to Excel for easy review and reporting
- **Flexible**: Enable/disable LLM and pattern detection independently

## Installation

```bash
pip install -r requirements.txt
```

### Requirements

- `oracledb` - Oracle database connection
- `pandas` - Data handling and Excel export
- `openpyxl` - Excel file format support
- `pyyaml` - Configuration file parsing
- `requests` - HTTP library for LLM communication
- `tqdm` - Progress bar display

## Usage

### Basic Scan

```bash
python main.py --excel credenziali.xlsx --host dbserver --port 1521 --output risultati.xlsx
```

### Skip LLM Detection (Faster)

```bash
python main.py --excel credenziali.xlsx --host dbserver --port 1521 --no-llm
```

### Skip Pattern Detection

```bash
python main.py --excel credenziali.xlsx --host dbserver --port 1521 --no-pattern
```

### Specify Oracle Service Name

```bash
python main.py --excel credenziali.xlsx --host dbserver --port 1521 --service ORCL
```

This overrides the SERVICE_NAME column in the Excel file. Useful when all schemas use the same service.

### Enable Debug Logging

```bash
python main.py --excel credenziali.xlsx --host dbserver --port 1521 --debug
```

Logs are written to `pii_scanner.log` by default. Use `--log-file` to specify a different location.

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--excel` | Excel file with credentials (required) | - |
| `--host` | Oracle server hostname (required, or use host:port:service format) | - |
| `--port` | Oracle port (default: 1521) | 1521 |
| `--service` | Oracle service name (overrides SERVICE_NAME in Excel) | - |
| `--no-llm` | Skip LLM-based detection | false |
| `--no-pattern` | Skip pattern-based detection | false |
| `--output` | Output Excel file | pii_report.xlsx |
| `--log-file` | Log file path | pii_scanner.log |
| `--debug` | Enable debug-level logging | false |

## Credentials File Format

The Excel credentials file should contain the following columns:

| Column | Description |
|--------|-------------|
| USER | Database username |
| PASSWORD | Database password (semicolon-separated if multiple) |

## Architecture

The scanner uses a **hybrid detection approach** with three detectors:

```
main.py → Scanner → HybridDetector → [NameDetector, PatternDetector, OllamaDetector]
```

### 1. NameDetector
Fast column name matching against patterns defined in `config/column_mappings.yaml`. Checks column names against Italian and English PII patterns.

### 2. PatternDetector
Regex matching on sampled row data for format validation. Validates that actual data matches expected PII formats (e.g., codice fiscale, IBAN).

### 3. OllamaDetector
Local LLM analysis using Ollama for ambiguous columns. Requires Ollama running on port 11434. Uses AI to classify columns where name and pattern detection are inconclusive.

### Performance

| Detection Enabled | Estimated Time (50 schemas) |
|-------------------|---------------------------|
| Name only | ~5 minutes |
| Name + Pattern | ~15 minutes |
| Name + Pattern + LLM | ~2-3 hours |

## Configuration

### Column Mappings

Edit `config/column_mappings.yaml` to customize PII detection patterns. The file contains categories for:

- **Personal Identity**: codice_fiscale, partita_iva, nome, cognome
- **Contact**: indirizzo, telefono, email
- **Demographics**: data_nascita, luogo_nascita, sesso
- **Documents**: documento_identità
- **Banking IDs**: NDG, RAPPORTO, ABI, CAB
- **Account Numbers**: IBAN, conto_corrente
- **Cards**: carta_di_credito, bancomat
- **Financial**: BIC/SWIFT
- **Contract/Policy**: numero_polizza, numero_contratto

## Output

The scanner generates an Excel report with columns:

| Column | Description |
|--------|-------------|
| SCHEMA | Database schema name |
| TABLE | Table name |
| COLUMN | Column name |
| DATA_TYPE | Oracle data type (e.g., VARCHAR2, NUMBER, DATE) |
| IS_PII | Y/N indicating PII detection |

### Smart Filtering

The scanner automatically excludes numeric ID columns that are primary or foreign keys from PII detection. This prevents false positives on system-generated identifiers like `ID`, `ID_CLIENTE`, or `ID_CONTRATTO` that serve as database keys rather than actual PII.

## Requirements

- Python 3.8+
- Oracle Database
- Optional: [Ollama](https://ollama.com/) for LLM-based detection (port 11434)