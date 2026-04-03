from dataclasses import dataclass
from typing import List
from src.database.metadata_fetcher import MetadataFetcher, TableInfo
from src.detectors.name_detector import NameDetector, PiiMatch as NameMatch
from src.detectors.pattern_detector import PatternDetector, PiiMatch as PatternMatch
from src.detectors.llm_detector import OllamaDetector


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

    def detect(self, schema: str, table: TableInfo) -> List[PiiFinding]:
        """Run all detectors and combine results"""
        findings = []

        # 1. Name-based detection (always run - fast)
        name_matches = self.name_detector.detect(table.columns)

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

        return findings


if __name__ == "__main__":
    print("Hybrid detector loaded")