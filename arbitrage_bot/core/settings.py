# core/settings.py

import json
from pathlib import Path

SETTINGS_FILE = Path("data/settings.json")
SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)

def load_stake():
    if SETTINGS_FILE.exists():
        with open(SETTINGS_FILE) as f:
            return json.load(f).get("stake", 10000)
    return 10000

def set_stake(value):
    with open(SETTINGS_FILE, "w") as f:
        json.dump({"stake": value}, f, indent=2)
