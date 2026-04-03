import requests
import json
from dataclasses import dataclass
from typing import List, Optional
from src.database.metadata_fetcher import MetadataFetcher
from src.detectors.name_detector import PiiMatch


# System prompt for PII classification - Italian banking focused
SYSTEM_PROMPT = """You are a data classification expert for Italian banking databases.
Your task is to analyze table column names and sample values to identify Personally Identifiable Information (PII).

PII categories to look for (Italian banking context):
- codice_fiscale: Italian tax code (format: ABCDEF12G34H567I)
- partita_iva: VAT number (11 digits)
- nome: First name
- cognome: Last name
- nome_completo / denominazione: Full name / Company name
- anagrafica: Customer master data (often contains names/addresses)
- indirizzo: Address
- città / comune: City
- cap: Postal code
- telefono / cellulare: Phone / Mobile
- email / pec: Email / Certified email
- data_nascita: Date of birth
- luogo_nascita: Place of birth
- sesso / genere: Gender
- documento_identità / carta_identita / passaporto: ID document
- ndg: Customer ID (Numero Delivered Globally)
- rapporto: Bank account/relationship ID
- iban: IBAN
- abi: Bank code (5 digits)
- cab: Branch code (5 digits)
- bic_swift: SWIFT code
- carta_di_credito / bancomat: Card numbers
- numero_polizza / numero_contratto: Policy/Contract number
- codice_rea: Business registration number
- descrizione / note: Description fields (may contain PII)
- nominativo: Name on account
- id / id_cliente / codice_cliente: Various IDs

Respond ONLY with valid JSON array (no other text):
[
  {"column": "COLUMN_NAME", "pii_type": "category", "confidence": 0.85},
  ...
]

If no PII detected, return empty array []. Use lowercase for pii_type."""


SAMPLE_VALUES_PROMPT = """Analyze these columns and sample values from an Italian banking table.
Respond with JSON array of columns containing PII:

Columns and samples (values redacted to first 20 chars):
{column_samples}

Categories: codice_fiscale, partita_iva, nome, cognome, nome_completo, anagrafica, indirizzo, città, cap, telefono, email, pec, data_nascita, luogo_nascita, sesso, documento_identità, ndg, rapporto, iban, abi, cab, bic_swift, carta_di_credito, bancomat, numero_polizza, numero_contratto, codice_rea, descrizione, note, nominativo, id, id_cliente, codice_cliente

Respond ONLY with JSON array."""


class OllamaDetector:
    """Detect PII using local LLM via Ollama"""

    def __init__(
        self,
        metadata_fetcher: MetadataFetcher,
        ollama_url: str = "http://localhost:11434",
        model: str = "llama3.2",
        sample_size: int = 10
    ):
        self.fetcher = metadata_fetcher
        self.ollama_url = ollama_url
        self.model = model
        self.sample_size = sample_size
        self._available = None

    def is_available(self) -> bool:
        """Check if Ollama is running"""
        if self._available is not None:
            return self._available

        try:
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=5)
            self._available = response.status_code == 200
        except requests.RequestException:
            self._available = False

        return self._available

    def _redact_value(self, value, max_len: int = 20) -> str:
        """Redact sensitive values while preserving format"""
        if value is None:
            return "NULL"

        s = str(value).strip()
        if len(s) <= max_len:
            return s[:max_len]
        return s[:max_len] + "..."

    def detect(self, owner: str, table_name: str) -> List[PiiMatch]:
        """Detect PII using LLM analysis of sample data"""
        if not self.is_available():
            return []

        # Get samples
        samples = self.fetcher.sample_rows(owner, table_name, self.sample_size)
        if not samples:
            return []

        # Build column -> samples mapping
        columns = list(samples[0].keys())
        column_samples = {}

        for col in columns:
            values = [row.get(col) for row in samples if row.get(col)]
            if values:
                column_samples[col] = [self._redact_value(v) for v in values[:5]]

        # Build prompt
        col_samples_str = "\n".join(
            f"- {col}: {samples}" for col, samples in column_samples.items()
        )

        prompt = SAMPLE_VALUES_PROMPT.format(column_samples=col_samples_str)

        # Call Ollama
        try:
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "system": SYSTEM_PROMPT,
                    "stream": False,
                    "format": "json"
                },
                timeout=60
            )

            if response.status_code != 200:
                return []

            result = response.json()
            text = result.get("response", "")

            # Parse JSON response
            matches = self._parse_llm_response(text)

            # Add table context
            for match in matches:
                match.column_name = table_name + "." + match.column_name

            return matches

        except Exception as e:
            print(f"LLM detection error for {owner}.{table_name}: {e}")
            return []

    def _parse_llm_response(self, text: str) -> List[PiiMatch]:
        """Parse LLM JSON response into PiiMatch objects"""
        try:
            # Find JSON array in response
            start = text.find("[")
            end = text.rfind("]") + 1

            if start == -1 or end == 0:
                return []

            json_str = text[start:end]
            data = json.loads(json_str)

            matches = []
            for item in data:
                matches.append(PiiMatch(
                    column_name=item.get("column", ""),
                    pii_type=item.get("pii_type", "unknown"),
                    confidence=item.get("confidence", 0.5),
                    matched_pattern="llm_analysis"
                ))

            return matches

        except (json.JSONDecodeError, KeyError) as e:
            print(f"Failed to parse LLM response: {e}")
            return []


if __name__ == "__main__":
    print("LLM detector loaded")