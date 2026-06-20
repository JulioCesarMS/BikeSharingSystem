import os

TRACK_FILE = "data/logs/processed_files.txt"

def load_processed():
    if not os.path.exists(TRACK_FILE):
        return set()

    with open(TRACK_FILE, "r") as f:
        return set(line.strip() for line in f)

def save_processed(filename):
    with open(TRACK_FILE, "a") as f:
        f.write(filename + "\n")