import os, time
import duckdb, pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth

# === Paths ===
base_dir = Path(r"C:\Tritone_Spotify\Data_analytics\tritone-assignment")
db_path = base_dir / "data" / "processed" / "unclaimed.duckdb"
log_path = base_dir / "logs" / "07B_pretest_isrc_overlap_v2.log"

def log(msg):
    print(msg)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# === Load environment ===
load_dotenv(base_dir / ".env")

# === Auth (cached) ===
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=os.getenv("SPOTIFY_CLIENT_ID"),
    client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
    redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
    scope="user-read-private",
    cache_path=base_dir / "config" / ".spotify_token_cache",
    open_browser=False
))
log("Authenticated using cached Spotify token.\n")

# === Connect to DuckDB ===
con = duckdb.connect(str(db_path))
log(f"Connected to DuckDB at: {db_path}\n")

# === Candidate artists ===
artists = [
    "Armin van Buuren",
    "Sizzla",
    "The Hit Crew",
    "Martin Jacoby",
    "Tele Music"
]

results = []

def fetch_isrcs_via_search(artist_name, limit=30):
    """Use search endpoint to fetch full track objects (with ISRCs)."""
    tracks = []
    offset = 0
    while len(tracks) < limit and offset < 100:
        data = sp.search(q=f"artist:{artist_name}", type="track", limit=50, offset=offset)
        items = data["tracks"]["items"]
        if not items:
            break
        for t in items:
            isrc = t.get("external_ids", {}).get("isrc")
            if isrc:
                tracks.append(isrc.upper().replace("-", "").strip())
            if len(tracks) >= limit:
                break
        offset += 50
        time.sleep(0.2)
    return list(set(tracks))

# === Iterate ===
for name in artists:
    log(f"Sampling artist: {name}")
    sample_isrcs = fetch_isrcs_via_search(name, limit=30)
    if not sample_isrcs:
        log(f"No ISRCs found for {name}\n")
        continue

    df_sample = pd.DataFrame(sample_isrcs, columns=["isrc_norm"])
    con.register("sample_isrcs", df_sample)
    q = """
    SELECT COUNT(*) FROM unclaimed_rights
    WHERE REPLACE(UPPER(ISRC), '-', '') IN (SELECT isrc_norm FROM sample_isrcs)
    """
    match_count = con.execute(q).fetchone()[0]
    total = len(sample_isrcs)
    pct = (match_count / total) * 100
    results.append((name, total, match_count, round(pct, 2)))
    con.unregister("sample_isrcs")
    log(f"{name}: {match_count}/{total} matched ({pct:.2f}%)\n")

con.close()
log("Connection closed.\n")

# === Summary ===
df = pd.DataFrame(results, columns=["Artist", "Sampled_ISRCs", "Matches", "Coverage_%"])
summary_path = base_dir / "reports" / "pretest_overlap_summary_v2.csv"
df.to_csv(summary_path, index=False)
log(f"Summary saved to: {summary_path}\n")

print("\n===== QUICK SUMMARY (via Track Search) =====")
print(df.to_string(index=False))
print("============================================\n")
