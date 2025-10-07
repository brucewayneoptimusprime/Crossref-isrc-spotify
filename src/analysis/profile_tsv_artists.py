import duckdb
import pandas as pd
from pathlib import Path

# === Paths ===
base_dir = Path(r"C:\Tritone_Spotify\Data_analytics\tritone-assignment")
db_path = base_dir / "data" / "processed" / "unclaimed.duckdb"
output_csv = base_dir / "reports" / "tsv_artist_profile.csv"
log_path = base_dir / "logs" / "07A_artist_profile.log"

def log(msg):
    print(msg)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

log("Starting TSV artist profiling\n")

# === Connect ===
con = duckdb.connect(str(db_path))
log(f"Connected to DuckDB: {db_path}")

# === Query top DisplayArtistName values ===
query = """
SELECT
    TRIM(DisplayArtistName) AS artist_name,
    COUNT(*) AS record_count
FROM unclaimed_rights
WHERE DisplayArtistName IS NOT NULL
  AND TRIM(DisplayArtistName) <> ''
  AND UPPER(TRIM(DisplayArtistName)) NOT IN ('VARIOUS ARTISTS', 'VARIOUS', 'UNKNOWN')
GROUP BY artist_name
ORDER BY record_count DESC
LIMIT 50;
"""

df = con.execute(query).fetchdf()
con.close()

log(f"Unique artist names extracted: {len(df)}")
log("Top 10 artists in the unclaimed dataset:\n")
print(df.head(10).to_string(index=False))

# === Save full profile ===
output_csv.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(output_csv, index=False)
log(f"Full profile saved to: {output_csv}\n")

log("Artist profiling completed successfully.")
