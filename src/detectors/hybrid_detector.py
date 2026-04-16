from dataclasses import dataclass
from typing import List
from src.database.metadata_fetcher import MetadataFetcher, TableInfo, ColumnInfo
from src.detectors.name_detector import NameDetector, PiiMatch as NameMatch
from src.detectors.pattern_detector import PatternDetector, PiiMatch as PatternMatch
from src.detectors.llm_detector import OllamaDetector, OllamaConnectionError
import logging


logger = logging.getLogger(__name__)


# Numeric types in Oracle
# Used to identify numeric ID columns that should be excluded from PII
NUMERIC_TYPES = {'NUMBER', 'INTEGER', 'INT', 'SMALLINT', 'BINARY_FLOAT', 'BINARY_DOUBLE', 'FLOAT'}

# Date/Time types in Oracle - excluded from PII detection
DATE_TYPES = {'DATE', 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP WITH LOCAL TIME ZONE',
              'TIME', 'TIME WITH TIME ZONE', 'TIME WITH LOCAL TIME ZONE'}

# BLOB types in Oracle - excluded from PII detection
BLOB_TYPES = {'BLOB', 'CLOB', 'NCLOB', 'LONG RAW', 'RAW', 'RAW(16)', 'BINARY FILE'}

# Specific column names to exclude from PII detection
EXCLUDED_COLUMNS = {
    'EXIT_CODE', 'JOB_NAME', 'JOB_KEY', 'STEP_NAME', 'READ_COUNT', 'READ_SKIP_COUNT',
    'KEY', 'BARCODE', 'MANDATO', 'MANDATE', 'CREATION_DATE', 'CREATED_BY'
}


@dataclass
class PiiFinding:
    schema: str
    table: str
    column: str
    pii_type: str
    confidence: float
    source: str  # "name", "pattern", "llm"


class HybridDetector:
    """Combine name, pattern, and LLM detection"""

    def __init__(
        self,
        metadata_fetcher: MetadataFetcher,
        use_llm: bool = True,
        use_pattern: bool = True
    ):
        self.fetcher = metadata_fetcher
        self.name_detector = NameDetector()
        self.pattern_detector = PatternDetector(metadata_fetcher)
        self.ollama_detector = OllamaDetector(metadata_fetcher) if use_llm else None
        self.use_pattern = use_pattern

    def verify_ollama(self):
        """Verify Ollama connection when LLM is enabled"""
        if self.ollama_detector is not None:
            self.ollama_detector.is_available()

    def detect(self, schema: str, table: TableInfo) -> List[PiiFinding]:
        """Run all detectors and combine results"""
        findings = []

        # Filter columns before detection
        columns_to_scan = self._filter_columns_by_type(table.columns)

        # 1. Name-based detection (always run - fast)
        name_matches = self.name_detector.detect(columns_to_scan)

        for match in name_matches:
            findings.append(PiiFinding(
                schema=schema,
                table=table.name,
                column=match.column_name,
                pii_type=match.pii_type,
                confidence=match.confidence,
                source="name"
            ))

        # 2. Pattern-based detection (optional - needs data access)
        if self.use_pattern:
            pattern_matches = self.pattern_detector.detect(schema, table.name)

            for match in pattern_matches:
                # Check if already found by name detector
                existing = next(
                    (f for f in findings if f.column == match.column_name),
                    None
                )
                if existing:
                    # Boost confidence if both methods agree
                    if existing.pii_type == match.pii_type:
                        existing.confidence = max(existing.confidence, match.confidence)
                        existing.source += "+pattern"
                else:
                    findings.append(PiiFinding(
                        schema=schema,
                        table=table.name,
                        column=match.column_name,
                        pii_type=match.pii_type,
                        confidence=match.confidence,
                        source="pattern"
                    ))

        # 3. LLM-based detection (optional - slow but thorough)
        if self.ollama_detector and self.ollama_detector.is_available():
            llm_matches = self.ollama_detector.detect(schema, table.name)

            for match in llm_matches:
                # Column name includes table prefix, extract just column
                column = match.column_name.split(".")[-1] if "." in match.column_name else match.column_name

                # Check if already found
                existing = next(
                    (f for f in findings if f.column == column),
                    None
                )
                if existing:
                    # Boost confidence
                    if existing.pii_type == match.pii_type:
                        existing.confidence = max(existing.confidence, match.confidence)
                        existing.source += "+llm"
                else:
                    findings.append(PiiFinding(
                        schema=schema,
                        table=table.name,
                        column=column,
                        pii_type=match.pii_type,
                        confidence=match.confidence,
                        source="llm"
                    ))

        # Filter out ID columns that are numeric and PK/FK
        findings = self._filter_id_columns(findings, table)

        # Filter out specific column names that should be excluded
        findings = self._filter_excluded_columns(findings)

        return findings

    def _filter_columns_by_type(self, columns: List[ColumnInfo]) -> List[ColumnInfo]:
        """
        Filter out columns that cannot contain PII based on data type.

        Exclusion criteria:
        - DATE, TIMESTAMP, and other date/time types
        - BLOB, CLOB, and other binary types
        """
        filtered = []
        for col in columns:
            dtype_upper = col.data_type.upper()
            if dtype_upper in DATE_TYPES:
                logger.debug(f"Skipping date column {col.name}")
                continue
            if dtype_upper in BLOB_TYPES:
                logger.debug(f"Skipping blob column {col.name}")
                continue
            filtered.append(col)
        return filtered

    def _filter_id_columns(self, findings: List[PiiFinding], table: TableInfo) -> List[PiiFinding]:
        """
        Exclude columns that contain ID, are numeric, and are PK/FK
        OR end with _ID and are numeric.

        Business rule: Numeric ID columns that serve as primary or foreign keys
        should not be flagged as PII, as they are just technical identifiers.

        Exclusion criteria (ALL must match):
        - Column name contains "ID" (e.g., USER_ID, CUSTOMER_ID)
        - Column data type is numeric (NUMBER, INTEGER, etc.)
        - Column is part of a primary key or foreign key constraint
        OR
        - Column name ends with "_ID"
        - Column data type is numeric
        """
        pk_fk_columns = table.pk_fk_columns or set()

        # Get column info map for quick lookup
        col_info = {c.name: c for c in table.columns}

        filtered = []
        for f in findings:
            col_name = f.column.upper()
            col = col_info.get(f.column)

            # Check if should be excluded based on PK/FK criteria
            is_pk_fk_id = (
                "ID" in col_name and
                col is not None and
                col.data_type.upper() in NUMERIC_TYPES and
                col_name in pk_fk_columns
            )

            # Check if should be excluded based on ending with _ID and numeric
            ends_with_id = col_name.endswith("_ID") and col is not None and col.data_type.upper() in NUMERIC_TYPES

            should_exclude = is_pk_fk_id or ends_with_id

            if not should_exclude:
                filtered.append(f)

        return filtered

    def _filter_excluded_columns(self, findings: List[PiiFinding]) -> List[PiiFinding]:
        """
        Exclude specific column names that should never be flagged as PII.

        These are operational/metadata columns that don't contain personal data:
        EXIT_CODE, job_name, job_key, step_name, read_count, read_skip_count,
        key, barcode, mandato, mandate, creation_date, created_by
        """
        filtered = []
        for f in findings:
            col_name_upper = f.column.upper().strip()
            if col_name_upper in EXCLUDED_COLUMNS:
                logger.debug(f"Excluding column {f.column} (excluded name)")
                continue
            filtered.append(f)
        return filtered


if __name__ == "__main__":
    print("Hybrid detector loaded")