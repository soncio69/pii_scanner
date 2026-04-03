from dataclasses import dataclass
from typing import List
from src.database.metadata_fetcher import ColumnInfo
from src.config import get_column_mappings


@dataclass
class PiiMatch:
    column_name: str
    pii_type: str
    confidence: float  # 0.0 to 1.0
    matched_pattern: str


class NameDetector:
    """Detect PII based on column names (Italian + English)"""

    def __init__(self):
        self.mappings = get_column_mappings()
        self.pii_columns = self.mappings.get("pii_columns", {})

    def detect(self, columns: List[ColumnInfo]) -> List[PiiMatch]:
        """Detect PII columns by name matching"""
        matches = []

        for col in columns:
            col_name_lower = col.name.lower()

            # Check each PII category
            for pii_type, patterns in self.pii_columns.items():
                for pattern in patterns:
                    if pattern.lower() in col_name_lower:
                        matches.append(PiiMatch(
                            column_name=col.name,
                            pii_type=pii_type,
                            confidence=0.9,
                            matched_pattern=pattern
                        ))
                        break  # One match per column is enough

        return matches


if __name__ == "__main__":
    detector = NameDetector()
    test_cols = [
        ColumnInfo("CODICE_FISCALE", "VARCHAR2", 16, False),
        ColumnInfo("CAMPO1", "VARCHAR2", 100, True),
        ColumnInfo("NOME_CLIENTE", "VARCHAR2", 50, False),
    ]
    matches = detector.detect(test_cols)
    for m in matches:
        print(f"  {m.column_name} -> {m.pii_type} ({m.confidence})")