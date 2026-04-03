import yaml
from pathlib import Path

CONFIG_DIR = Path(__file__).parent.parent / "config"

def load_yaml(filename):
    with open(CONFIG_DIR / filename, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_column_mappings():
    return load_yaml("column_mappings.yaml")

def get_pii_rules():
    return load_yaml("pii_rules.yaml")

def get_db_config():
    return load_yaml("db_config.yaml")