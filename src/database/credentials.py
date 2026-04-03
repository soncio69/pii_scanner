import pandas as pd
from pathlib import Path

def load_credentials(excel_path: str) -> list[dict]:
    """Load Oracle credentials from Excel file.

    Expected format: columns named "USER" and "PASSWORD"
    Returns list of dicts with schema, user, password
    """
    df = pd.read_excel(excel_path, engine="openpyxl")

    # Normalize column names (remove quotes, whitespace)
    df.columns = df.columns.str.strip().str.replace('"', '')

    # Expecting columns: USER, PASSWORD (and optionally SCHEMA or SERVICE_NAME)
    credentials = []
    for _, row in df.iterrows():
        cred = {
            "user": str(row.get("USER", "")).strip(),
            "password": str(row.get("PASSWORD", "")).strip(),
            "service_name": str(row.get("SERVICE_NAME", "")).strip()
                         or str(row.get("SCHEMA", "")).strip()
        }
        # Skip empty rows
        if cred["user"] and cred["password"]:
            credentials.append(cred)

    return credentials


if __name__ == "__main__":
    # Test
    import sys
    if len(sys.argv) > 1:
        creds = load_credentials(sys.argv[1])
        print(f"Loaded {len(creds)} credentials")
        for c in creds[:3]:
            print(f"  {c['user']} @ {c['service_name']}")