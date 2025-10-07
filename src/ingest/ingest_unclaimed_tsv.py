import duckdb
import pandas as pd
from tqdm import tqdm
from pathlib import Path
import sys
import traceback

# === Base paths ===
base_dir = Path(r"C:\Tritone_Spotify\Data_analytics\tritone-assignment")
tsv_file = base_dir / "data" / "raw" / "unclaimedmusicalworkrightshares.tsv"
db_path = base_dir / "data" / "processed" / "unclaimed.duckdb"
log_path = base_dir / "logs" / "04B_ingestion.log"

# === Parameters ===
chunksize = 100_000      # rows per batch (~manageable on 8–16 GB RAM)
max_preview = 5           # print sample after each commit

# === Logging utility ===
def log(msg):
    print(msg)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# === Begin process ===
try:
    log("Starting TSV → DuckDB ingestion\n")
    log(f"Input file: {tsv_file}")
    log(f"Output DB : {db_path}\n")

    # --- Connect to DuckDB ---
    con = duckdb.connect(str(db_path))
    con.execute("PRAGMA threads=4;")  # allow parallel ops

    total_rows = 0
    error_batches = 0

    # --- Stream read in chunks ---
    for chunk_no, df in enumerate(pd.read_csv(tsv_file, sep="\t", chunksize=chunksize, low_memory=False), start=1):
        try:
            # Normalize column names (strip spaces, rename if header mismatch)
            df.columns = [c.strip().replace("#", "") for c in df.columns]

            # Insert chunk into DuckDB
            con.register("chunk_df", df)
            con.execute("""
                INSERT INTO unclaimed_rights
                SELECT
                    CAST(UnclaimedMusicalWorkRightShareRecordId AS BIGINT),
                    ResourceRecordId,
                    MusicalWorkRecordId,
                    ISRC,
                    DspResourceId,
                    ResourceTitle,
                    ResourceSubTitle,
                    AlternativeResourceTitle,
                    DisplayArtistName,
                    DisplayArtistISNI,
                    TRY_CAST(Duration AS INTEGER),
                    TRY_CAST(UnclaimedRightSharePercentage AS DOUBLE),
                    TRY_CAST(PercentileForPrioritisation AS DOUBLE)
                FROM chunk_df;
            """)
            con.unregister("chunk_df")

            total_rows += len(df)
            log(f"Batch {chunk_no:05d} | Rows: {len(df):>6} | Total inserted: {total_rows:,}")

            if chunk_no % 10 == 0:  # show small sample every 10th batch
                preview = con.execute("SELECT * FROM unclaimed_rights LIMIT ?;", [max_preview]).fetchdf()
                log(f"Sample rows after batch {chunk_no}:\n{preview}\n")

        except Exception as e:
            error_batches += 1
            log(f"Error in batch {chunk_no}: {e}")
            traceback.print_exc(file=sys.stdout)

    # --- Final count ---
    final_count = con.execute("SELECT COUNT(*) FROM unclaimed_rights;").fetchone()[0]
    log(f"\nIngestion completed. Final row count: {final_count:,}")
    log(f"Total error batches: {error_batches}")

    # --- Create an index on ISRC for fast lookups ---
    con.execute("CREATE INDEX IF NOT EXISTS idx_isrc ON unclaimed_rights(ISRC);")
    log("ISRC index created successfully.\n")

    con.close()
    log("Connection closed.\n")

except Exception as main_err:
    log(f"Fatal error: {main_err}")
    traceback.print_exc(file=sys.stdout)
    sys.exit(1)
