import os
from pathlib import Path

# Base directory (adjust if needed)
BASE_DIR = Path(r"C:\Tritone_Spotify\Data_analytics\tritone-assignment")

EXCLUDE_DIRS = {"__pycache__", ".venv", ".git", "node_modules"}

def print_tree(path: Path, prefix=""):
    """Recursively prints directory tree."""
    items = sorted(path.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
    for i, item in enumerate(items):
        if item.name in EXCLUDE_DIRS:
            continue
        connector = "└── " if i == len(items) - 1 else "├── "
        print(prefix + connector + item.name)
        if item.is_dir():
            extension = "    " if i == len(items) - 1 else "│   "
            print_tree(item, prefix + extension)

if __name__ == "__main__":
    print(f"Project structure for: {BASE_DIR}\n")
    print_tree(BASE_DIR)
