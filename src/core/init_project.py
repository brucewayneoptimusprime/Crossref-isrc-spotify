import json
import hashlib
from datetime import datetime
from pathlib import Path

# === Base project directory ===
base_dir = Path(r"C:\Tritone_Spotify\Data_analytics\tritone-assignment")

# === File paths ===
tsv_file = base_dir / "data" / "raw" / "unclaimedmusicalworkrightshares.tsv"
config_path = base_dir / "config" / "settings.toml"
manifest_path = base_dir / "reports" / "run_manifest.json"

# === Compute SHA256 checksum (lightweight incremental read) ===
def compute_checksum(file_path, chunk_size=8192):
    sha = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            sha.update(chunk)
    return sha.hexdigest()

print(f"Computing checksum for: {tsv_file}")
checksum = compute_checksum(tsv_file)

# === Prepare TOML config ===
toml_content = f"""
# === Project Configuration ===
[data]
raw_tsv = "data/raw/unclaimedmusicalworkrightshares.tsv"
db_engine = "duckdb"
db_path = "data/processed/unclaimed.duckdb"

[artist]
name = "YOUR_ARTIST_NAME_HERE"
spotify_id = ""  # optional, fill later

[export]
workbook_path = "reports/tritone_artist_catalog.xlsx"
sheet1 = "Artist Catalog"
sheet2 = "Matches (Unclaimed)"
sheet3 = "Notes"

[logs]
dir = "logs/"
"""

# === Prepare manifest data ===
manifest = {
    "timestamp": datetime.now().isoformat(timespec="seconds"),
    "tsv_file": str(tsv_file),
    "file_size_gb": round(tsv_file.stat().st_size / (1024**3), 2),
    "sha256": checksum,
    "db_engine": "DuckDB",
    "config_path": str(config_path),
    "notes": "Initial manifest with file metadata and configuration reference."
}

# === Write files ===
config_path.write_text(toml_content.strip(), encoding="utf-8")
manifest_path.write_text(json.dumps(manifest, indent=4), encoding="utf-8")

print("\nConfiguration and manifest created successfully.")
print(f"Config:   {config_path}")
print(f"Manifest: {manifest_path}")
print(f"SHA256 checksum: {checksum[:32]}... (truncated)")
