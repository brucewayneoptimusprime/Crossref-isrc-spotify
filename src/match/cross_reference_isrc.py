import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime

# === Base paths ===
base_dir = Path(r"C:\Tritone_Spotify\Data_analytics\tritone-assignment")
db_path = base_dir / "data" / "processed" / "unclaimed.duckdb"
spotify_csv = base_dir / "data" / "interim" / "spotify_catalog.csv"
report_xlsx = base_dir / "reports" / "tritone_artist_catalog.xlsx"
log_path = base_dir / "logs" / "06_cross_reference.log"

def log(msg):
    print(msg)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

log("Starting ISRC cross-reference process\n")

# === Load Spotify catalog ===
df_spotify = pd.read_csv(spotify_csv)
log(f"Spotify tracks loaded: {len(df_spotify):,}")

# Normalize ISRC
df_spotify["isrc_norm"] = (
    df_spotify["isrc"].astype(str).str.upper().str.replace("-", "").str.strip()
)
df_spotify.dropna(subset=["isrc_norm"], inplace=True)

# === Connect to DuckDB ===
con = duckdb.connect(str(db_path))
log(f"Connected to DuckDB: {db_path}")

# === Fetch matching records ===
query = """
SELECT
    u.ISRC,
    u.ResourceTitle,
    u.DisplayArtistName,
    u.UnclaimedRightSharePercentage,
    u.PercentileForPrioritisation
FROM unclaimed_rights u
WHERE REPLACE(UPPER(u.ISRC), '-', '') IN (
    SELECT DISTINCT isrc_norm FROM df_spotify
)
"""
con.register("df_spotify", df_spotify)
matches_df = con.execute(query).fetchdf()
con.unregister("df_spotify")

log(f"Matches found: {len(matches_df):,}")

# === Merge details for reporting ===
merged = pd.merge(
    df_spotify,
    matches_df,
    left_on="isrc_norm",
    right_on="ISRC",
    how="left",
    indicator=True
)

found = merged[merged["_merge"] == "both"]
coverage = len(found) / len(df_spotify) * 100

log(f"Matched tracks: {len(found):,} ({coverage:.2f}% coverage)")

# === Prepare Notes sheet ===
notes_text = [
    ["Artist", "The Beatles"],
    ["Total Spotify tracks", len(df_spotify)],
    ["Matched (found in unclaimed dataset)", len(found)],
    ["Coverage %", f"{coverage:.2f}%"],
    ["Database path", str(db_path)],
    ["Generated on", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
]

notes_df = pd.DataFrame(notes_text, columns=["Metric", "Value"])

# === Write to Excel ===
with pd.ExcelWriter(report_xlsx, engine="openpyxl") as writer:
    df_spotify.to_excel(writer, sheet_name="Artist Catalog", index=False)
    found.to_excel(writer, sheet_name="Matches (Unclaimed)", index=False)
    notes_df.to_excel(writer, sheet_name="Notes", index=False)

log(f"Excel report saved to: {report_xlsx}")
con.close()
log("Connection closed. Process complete.\n")
