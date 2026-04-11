import oracledb
from typing import List, Set
from dataclasses import dataclass
from tqdm import tqdm
import logging

from src.database.oracle_connector import OracleConnector, build_dsn
from src.database.metadata_fetcher import MetadataFetcher
from src.database.credentials import load_credentials
from src.detectors.hybrid_detector import HybridDetector, PiiFinding


logger = logging.getLogger(__name__)


def setup_logging(log_file: str = None, debug: bool = False):
    """Configure logging to console and optional file"""
    level = logging.DEBUG if debug else logging.INFO
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
        handlers=handlers
    )


@dataclass
class ScanConfig:
    host: str
    port: int
    service: str
    excel_path: str = None
    use_llm: bool = True
    use_pattern: bool = True
    sample_size: int = 20
    log_file: str = None
    debug: bool = False


@dataclass
class ColumnResult:
    schema: str
    table: str
    column: str
    data_type: str  # e.g., "VARCHAR2(100)", "NUMBER", "DATE"
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
            # Always use service name from command line
            schema_results = self._scan_schema(
                host=self.config.host,
                port=self.config.port,
                user=cred["user"],
                password=cred["password"],
                service_name=self.config.service
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
                        # Format data type with length for varchar2
                        if col.data_type.upper() in ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR'):
                            dtype = f"{col.data_type}({col.data_length})"
                        else:
                            dtype = col.data_type

                        results.append(ColumnResult(
                            schema=user.upper(),
                            table=table.name,
                            column=col.name,
                            data_type=dtype,
                            is_pii=False
                        ))

                    # Then, detect PII and update results
                    table_findings = detector.detect(schema=user, table=table)

                    for finding in table_findings:
                        pii_columns.add((user.upper(), table.name, finding.column))

        except oracledb.Error as e:
            logger.error(f"Error scanning schema {user}: {str(e)}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error for {user}: {str(e)}", exc_info=True)
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
                "DATA_TYPE": r.data_type,
                "IS_PII": "Y" if r.is_pii else "N"
            }
            for r in self.results
        ])

        # Ensure correct column order
        df = df[["SCHEMA", "TABLE", "COLUMN", "DATA_TYPE", "IS_PII"]]

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