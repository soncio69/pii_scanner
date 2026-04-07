import oracledb
from typing import List, Set
from dataclasses import dataclass
from tqdm import tqdm
import logging

from src.database.oracle_connector import OracleConnector, build_dsn
from src.database.metadata_fetcher import MetadataFetcher
from src.database.credentials import load_credentials
from src.detectors.hybrid_detector import HybridDetector, PiiFinding


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ScanConfig:
    host: str
    port: int
    service: str = None
    excel_path: str = None
    use_llm: bool = True
    use_pattern: bool = True
    sample_size: int = 20


@dataclass
class ColumnResult:
    schema: str
    table: str
    column: str
    is_pii: bool


class Scanner:
    """Main PII scanner for Oracle schemas"""

    def __init__(self, config: ScanConfig):
        self.config = config
        self.results: List[ColumnResult] = []
        self._pii_columns: Set[tuple] = set()  # (schema, table, column) -> is PII

    def scan(self) -> List[ColumnResult]:
        """Execute full scan across all schemas"""
        credentials = load_credentials(self.config.excel_path)
        logger.info(f"Loaded {len(credentials)} credentials from Excel")

        for cred in tqdm(credentials, desc="Scanning schemas"):
            # Use global service from config if provided, otherwise use per-credential service
            service_name = self.config.service or cred["service_name"]
            schema_results = self._scan_schema(
                host=self.config.host,
                port=self.config.port,
                user=cred["user"],
                password=cred["password"],
                service_name=service_name
            )
            self.results.extend(schema_results)

        return self.results

    def _scan_schema(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        service_name: str
    ) -> List[ColumnResult]:
        """Scan a single schema"""
        results = []
        pii_columns: Set[tuple] = set()

        dsn = build_dsn(host, port, service_name)
        connector = OracleConnector(user, password, dsn)

        try:
            with connector.get_connection() as conn:
                fetcher = MetadataFetcher(conn)
                detector = HybridDetector(
                    fetcher,
                    use_llm=self.config.use_llm,
                    use_pattern=self.config.use_pattern
                )

                # Get all tables with columns (efficient)
                tables = fetcher.get_all_tables_with_columns(owner=user.upper())

                logger.info(f"Schema {user}: {len(tables)} tables")

                # Scan each table
                for table in tables:
                    # First, mark all columns as NOT_PII
                    for col in table.columns:
                        results.append(ColumnResult(
                            schema=user.upper(),
                            table=table.name,
                            column=col.name,
                            is_pii=False
                        ))

                    # Then, detect PII and update results
                    table_findings = detector.detect(schema=user, table=table)

                    for finding in table_findings:
                        pii_columns.add((user.upper(), table.name, finding.column))

        except oracledb.Error as e:
            logger.error(f"Error scanning schema {user}: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error for {user}: {str(e)}")
        finally:
            connector.close()

        # Update is_pii for found columns
        for result in results:
            if (result.schema, result.table, result.column) in pii_columns:
                result.is_pii = True

        return results

    def save_report(self, output_path: str):
        """Save results to Excel file"""
        import pandas as pd

        if not self.results:
            print("No results to save")
            return

        df = pd.DataFrame([
            {
                "SCHEMA": r.schema,
                "TABLE": r.table,
                "COLUMN": r.column,
                "IS_PII": "Y" if r.is_pii else "N"
            }
            for r in self.results
        ])

        # Ensure correct column order
        df = df[["SCHEMA", "TABLE", "COLUMN", "IS_PII"]]

        # Save to Excel
        if output_path.endswith(".xlsx"):
            df.to_excel(output_path, index=False, engine="openpyxl")
        else:
            df.to_excel(output_path + ".xlsx", index=False, engine="openpyxl")

        # Summary
        total = len(df)
        pii_count = (df["IS_PII"] == "Y").sum()
        logger.info(f"Saved {total} columns ({pii_count} with PII) to {output_path}")


if __name__ == "__main__":
    print("Scanner module loaded")