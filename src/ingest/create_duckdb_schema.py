import duckdb
from pathlib import Path

# === Base project directory ===
base_dir = Path(r"C:\Tritone_Spotify\Data_analytics\tritone-assignment")

# === Database path ===
db_path = base_dir / "data" / "processed" / "unclaimed.duckdb"

# === Connect (creates file if not exists) ===
con = duckdb.connect(str(db_path))
print(f"Connected to DuckDB database at:\n{db_path}\n")

# === Define schema based on TSV columns ===
# We mirror the header line detected earlier (13 columns)
create_table_sql = """
CREATE TABLE IF NOT EXISTS unclaimed_rights (
    UnclaimedMusicalWorkRightShareRecordId BIGINT,
    ResourceRecordId TEXT,
    MusicalWorkRecordId TEXT,
    ISRC TEXT,
    DspResourceId TEXT,
    ResourceTitle TEXT,
    ResourceSubTitle TEXT,
    AlternativeResourceTitle TEXT,
    DisplayArtistName TEXT,
    DisplayArtistISNI TEXT,
    Duration INT,
    UnclaimedRightSharePercentage DOUBLE,
    PercentileForPrioritisation DOUBLE
);
"""

# === Execute schema creation ===
con.execute(create_table_sql)
print("Table 'unclaimed_rights' created (if not already exists).")

# === Verify structure ===
result = con.execute("PRAGMA table_info('unclaimed_rights');").fetchall()
print("\nTable Schema:")
for col in result:
    print(f" - {col[1]} ({col[2]})")

# === Row count check (should be zero now) ===
count = con.execute("SELECT COUNT(*) FROM unclaimed_rights;").fetchone()[0]
print(f"\nCurrent row count: {count}")

# === Close connection ===
con.close()
print("\nSchema setup completed successfully and connection closed.")
