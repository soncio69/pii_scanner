# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PII Scanner for Oracle Databases - scans multiple Oracle schemas to identify columns containing Personally Identifiable Information (PII), particularly focused on Italian banking contexts.

## Running the Scanner

```bash
# Install dependencies
pip install -r requirements.txt

# Run scan
python main.py --excel credenziali.xlsx --host dbserver --port 1521 --output risultati.xlsx

# Skip LLM detection (faster)
python main.py --excel credenziali.xlsx --host dbserver --port 1521 --no-llm

# Skip pattern detection
python main.py --excel credenziali.xlsx --host dbserver --port 1521 --no-pattern
```

Expected Excel credentials format: columns "USER", "PASSWORD" (semicolon-separated in the original file).

## Architecture

The scanner uses a **hybrid detection approach** with three detectors:

1. **NameDetector** - Fast column name matching against `config/column_mappings.yaml` (Italian/English patterns)
2. **PatternDetector** - Regex matching on sampled row data for format validation
3. **OllamaDetector** - Local LLM analysis for ambiguous columns (requires Ollama running on port 11434)

Flow: `main.py` → `Scanner` → `HybridDetector` → detectors → Excel output

## Key Configuration

- `config/column_mappings.yaml` - Column name patterns for PII categories (codice_fiscale, NDG, rapporto, anagrafica, etc.)
- `src/detectors/pattern_detector.py` - Regex patterns for data validation
- `src/detectors/llm_detector.py` - System prompt for Ollama LLM classification

## Performance Notes

- Name detection: ~5 min for 50 schemas
- + Pattern detection: ~15 min
- + LLM detection: ~2-3 hours (use `--no-llm` for faster scans)