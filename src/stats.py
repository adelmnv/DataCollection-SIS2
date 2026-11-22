import json
import os
from config import STATS_FILE


def load_stats():
    if os.path.exists(STATS_FILE):
        with open(STATS_FILE, "r") as file:
            return json.load(file)
    return {}


def save_stat(name, value):
    stats = load_stats()
    stats[name] = value
    with open(STATS_FILE, "w") as file:
        json.dump(stats, file, indent=2)


def reset_stats():
    with open(STATS_FILE, "w") as file:
        json.dump({}, file)