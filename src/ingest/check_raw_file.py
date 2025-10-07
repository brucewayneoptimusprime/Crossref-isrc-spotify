from pathlib import Path

# === Absolute path to your TSV file ===
tsv_path = Path(r"C:\Tritone_Spotify\Data_analytics\tritone-assignment\data\raw\unclaimedmusicalworkrightshares.tsv")

# === Basic existence and size checks ===
if not tsv_path.exists():
    raise FileNotFoundError(f"File not found: {tsv_path}")

size_gb = tsv_path.stat().st_size / (1024**3)
print(f"File found at: {tsv_path}")
print(f"File size: {size_gb:.2f} GB")

# === Read first few lines safely (no full load) ===
print("\nPreviewing first 5 lines:")
with open(tsv_path, "r", encoding="utf-8", errors="ignore") as f:
    for i in range(5):
        line = f.readline()
        if not line:
            break
        print(f"{i+1:>2}: {line.strip()[:300]}")  # print first 300 chars per line for safety

# === Quick delimiter sanity check ===
with open(tsv_path, "r", encoding="utf-8", errors="ignore") as f:
    header = f.readline()
    columns = header.strip().split("\t")
    print(f"\nDetected {len(columns)} columns in header.")
    print("Column sample:", columns[:10])
