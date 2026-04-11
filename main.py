#!/usr/bin/env python3
"""
PII Scanner for Oracle Databases
Usage: python main.py --excel credentials.xlsx --host dbserver --port 1521

Optional:
  --no-llm        Skip LLM-based detection
  --no-pattern    Skip pattern-based detection
  --output        Output file (default: pii_report.xlsx)
"""

import argparse
import sys
from src.scanner import Scanner, ScanConfig, setup_logging


def main():
    parser = argparse.ArgumentParser(description="Scan Oracle schemas for PII columns")
    parser.add_argument("--excel", required=True, help="Excel file with credentials")
    parser.add_argument("--host", required=True, help="Oracle server hostname (or use host:port:service format)")
    parser.add_argument("--port", type=int, default=1521, help="Oracle port (default: 1521)")
    parser.add_argument("--service", help="Oracle service name (overrides SERVICE_NAME in Excel)")
    parser.add_argument("--no-llm", action="store_true", help="Skip LLM detection")
    parser.add_argument("--no-pattern", action="store_true", help="Skip pattern detection")
    parser.add_argument("--output", default="pii_report.xlsx", help="Output file (Excel)")
    parser.add_argument("--log-file", default="pii_scanner.log", help="Log file (default: pii_scanner.log)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    config = ScanConfig(
        host=args.host,
        port=args.port,
        service=args.service,
        excel_path=args.excel,
        use_llm=not args.no_llm,
        use_pattern=not args.no_pattern,
        log_file=args.log_file,
        debug=args.debug
    )

    print(f"""
PII Scanner Configuration:
  Excel:     {args.excel}
  Host:      {args.host}:{args.port}
  Service:   {args.service}
  LLM:       {'enabled' if config.use_llm else 'disabled'}
  Pattern:   {'enabled' if config.use_pattern else 'disabled'}
  Output:    {args.output}
  Log:       {args.log_file}
  Debug:     {'enabled' if config.debug else 'disabled'}
""")

    setup_logging(config.log_file, config.debug)
    scanner = Scanner(config)
    results = scanner.scan()

    if not results:
        print("\nNo results to save")
        return

    # Count summary
    import pandas as pd
    df = pd.DataFrame([
        {
            "SCHEMA": r.schema,
            "TABLE": r.table,
            "COLUMN": r.column,
            "DATA_TYPE": r.data_type,
            "IS_PII": "Y" if r.is_pii else "N"
        }
        for r in results
    ])

    total = len(df)
    pii_count = (df["IS_PII"] == "Y").sum()
    schema_count = df["SCHEMA"].nunique()
    table_count = df["TABLE"].nunique()

    print(f"\n=== Summary ===")
    print(f"Schemas scanned:     {schema_count}")
    print(f"Tables scanned:      {table_count}")
    print(f"Total columns:       {total}")
    print(f"Columns with PII:    {pii_count}")
    print(f"Columns without PII: {total - pii_count}")

    # PII by type (if we have this data)
    if pii_count > 0:
        print(f"\nPII columns found: {pii_count}")

    scanner.save_report(args.output)


if __name__ == "__main__":
    main()