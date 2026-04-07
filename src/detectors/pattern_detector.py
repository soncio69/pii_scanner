import re
from dataclasses import dataclass
from typing import List, Optional
from src.database.metadata_fetcher import ColumnInfo, MetadataFetcher
from src.detectors.name_detector import PiiMatch


@dataclass
class PiiPattern:
    pii_type: str
    # Regex patterns (values, not column names)
    patterns: List[str]
    # Weight for confidence calculation
    weight: float = 0.8


# Italian banking PII patterns
PII_PATTERNS = [
    # ============ Codice Fiscale ============
    PiiPattern(
        "codice_fiscale",
        patterns=[
            r"^[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]$",  # CF format
            r"^[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]{1}\d{3}[A-Z]{1}$",  # CF with single letters
        ],
        weight=0.95
    ),

    # ============ Partita IVA ============
    PiiPattern(
        "partita_iva",
        patterns=[
            r"^\d{11}$",  # 11 digits
            r"^IT\d{11}$",  # IT prefix
        ],
        weight=0.9
    ),

    # ============ IBAN ============
    PiiPattern(
        "iban",
        patterns=[
            r"^IT\d{2}[A-Z]\d{5}\d{5}[A-Z]\d{5}[A-Z]\d{5}$",  # Italian IBAN
            r"^[A-Z]{2}\d{2}[A-Z0-9]{4,30}$",  # Generic IBAN
        ],
        weight=0.85
    ),

    # ============ ABI (5 digits) ============
    PiiPattern(
        "abi",
        patterns=[
            r"^\d{5}$",  # 5 digits
        ],
        weight=0.7
    ),

    # ============ CAB (5 digits) ============
    PiiPattern(
        "cab",
        patterns=[
            r"^\d{5}$",  # 5 digits
        ],
        weight=0.7
    ),

    # ============ NDG (various formats) ============
    # NDG can be numeric or alphanumeric depending on bank
    PiiPattern(
        "ndg",
        patterns=[
            r"^\d{6,10}$",  # Numeric NDG (6-10 digits)
            r"^[A-Z0-9]{6,15}$",  # Alphanumeric NDG
            r"^NDG\d+$",  # NDG prefix
            r"^[A-Z]{2}\d{6,10}$",  # Prefix + numbers
        ],
        weight=0.75
    ),

    # ============ RAPPORTO (account number) ============
    # Italian bank account references - varies by bank (usually 10-12 digits)
    PiiPattern(
        "rapporto",
        patterns=[
            r"^\d{10,12}$",  # Standard account number
            r"^[A-Z0-9]{10,17}$",  # Alphanumeric (some banks use this)
            r"^\d{4}/\d{5,6}$",  # Branch/account format
            r"^R\d{9,12}$",  # R prefix
        ],
        weight=0.75
    ),

    # ============ Credit Cards ============
    PiiPattern(
        "carta_di_credito",
        patterns=[
            r"^\d{13,19}$",  # Card number (13-19 digits)
            r"^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$",  # Formatted card
        ],
        weight=0.85
    ),

    # ============ Email ============
    PiiPattern(
        "email",
        patterns=[
            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            r".*@.*\..*",  # Basic email pattern
        ],
        weight=0.8
    ),

    # ============ Phone (Italian) ============
    PiiPattern(
        "telefono",
        patterns=[
            r"^\+?39\d{9,10}$",  # Italian phone
            r"^\d{10,15}$",  # Generic phone
            r"^3\d{9}$",  # Mobile starting with 3
        ],
        weight=0.7
    ),

    # ============ PEC (Certified Email) ============
    PiiPattern(
        "pec",
        patterns=[
            r".*@.*\.pec\.it$",  # Italian certified email
        ],
        weight=0.85
    ),

    # ============ SWIFT/BIC ============
    PiiPattern(
        "bic_swift",
        patterns=[
            r"^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$",  # 8 or 11 char SWIFT
        ],
        weight=0.8
    ),

    # ============ Contract/Policy Number ============
    PiiPattern(
        "numero_polizza",
        patterns=[
            r"^\d{8,15}$",  # Policy number
            r"^POL\d+$",  # POL prefix
            r"^[A-Z]{2}\d{8,12}$",  # Prefix + numbers
        ],
        weight=0.7
    ),

    # ============ Codice REA ============
    PiiPattern(
        "codice_rea",
        patterns=[
            r"^[A-Z]{2}\d{6}$",  # REA format (province + number)
            r"^\d{6}$",  # Just numbers
        ],
        weight=0.75
    ),

    # ============ Date of Birth ============
    PiiPattern(
        "data_nascita",
        patterns=[
            r"^\d{2}/\d{2}/\d{4}$",  # DD/MM/YYYY
            r"^\d{4}-\d{2}-\d{2}$",  # YYYY-MM-DD
            r"^\d{8}$",  # YYYYMMDD
        ],
        weight=0.6
    ),
]


class PatternDetector:
    """Detect PII based on data content patterns"""

    def __init__(self, metadata_fetcher: MetadataFetcher, sample_size: int = 20):
        self.fetcher = metadata_fetcher
        self.sample_size = sample_size

    def detect(self, owner: str, table_name: str) -> List[PiiMatch]:
        """Detect PII by sampling data and checking patterns"""
        matches = []

        # Sample rows from table
        samples = self.fetcher.sample_rows(owner, table_name, self.sample_size)
        if not samples:
            return matches

        # Build regex for each column
        for column in samples[0].keys():
            values = [row.get(column) for row in samples if row.get(column)]

            if not values:
                continue

            # Convert all to string for pattern matching
            def to_str(v):
                if v is None:
                    return ""
                # Some Oracle types return bytes from __str__
                try:
                    result = str(v)
                    if isinstance(result, bytes):
                        return result.decode('utf-8', errors='ignore')
                    return result
                except TypeError:
                    # Fallback for stubborn cases
                    return repr(v)

            str_values = [to_str(v).strip() for v in values if v]

            # Check against each PII pattern
            for pii_pattern in PII_PATTERNS:
                matches_count = 0
                for value in str_values:
                    for regex in pii_pattern.patterns:
                        if re.match(regex, value, re.IGNORECASE):
                            matches_count += 1
                            break

                # If >50% of samples match, consider it PII
                if matches_count >= len(str_values) * 0.5:
                    matches.append(PiiMatch(
                        column_name=column,
                        pii_type=pii_pattern.pii_type,
                        confidence=pii_pattern.weight * (matches_count / len(str_values)),
                        matched_pattern="data_pattern"
                    ))
                    break  # One match per column

        return matches


if __name__ == "__main__":
    print("Pattern detector loaded")