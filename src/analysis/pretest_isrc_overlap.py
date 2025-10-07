import os, time
import duckdb, pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth

# === Base paths ===
base_dir = Path(r"C:\Tritone_Spotify\Data_analytics\tritone-assignment")
db_path = base_dir / "data" / "processed" / "unclaimed.duckdb"
log_path = base_dir / "logs" / "07B_pretest_isrc_overlap.log"

def log(msg):
    print(msg)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# === Load environment ===
load_dotenv(base_dir / ".env")

# === Spotify authentication (reuses local cache) ===
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
con.execute("PRAGMA threads=4;")
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

# === Helper: fetch up to 30 ISRCs per artist ===
def fetch_isrcs(artist_name, limit=30):
    search = sp.search(q=f"artist:{artist_name}", type="artist", limit=1)
    if not search["artists"]["items"]:
        log(f"Artist not found: {artist_name}")
        return []
    artist_id = search["artists"]["items"][0]["id"]
    albums = sp.artist_albums(artist_id, album_type="album,single,compilation", limit=10)
    isrcs = set()
    for a in albums["items"]:
        tracks = sp.album_tracks(a["id"])
        for t in tracks["items"]:
            isrc = t.get("external_ids", {}).get("isrc")
            if isrc:
                isrcs.add(isrc.upper().replace("-", "").strip())
            if len(isrcs) >= limit:
                return list(isrcs)
        time.sleep(0.2)
    return list(isrcs)

# === Iterate through candidates ===
for name in artists:
    log(f"Sampling artist: {name}")
    sample_isrcs = fetch_isrcs(name, limit=30)
    if not sample_isrcs:
        log(f"No ISRCs found for {name}\n")
        continue

    # query DuckDB for matches
    con.register("sample_isrcs", pd.DataFrame(sample_isrcs, columns=["isrc_norm"]))
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
log("🔒 Connection closed.\n")

# === Present results summary ===
df = pd.DataFrame(results, columns=["Artist", "Sampled_ISRCs", "Matches", "Coverage_%"])
summary_path = base_dir / "reports" / "pretest_overlap_summary.csv"
df.to_csv(summary_path, index=False)
log(f"Summary saved to: {summary_path}\n")

print("\n===== QUICK SUMMARY =====")
print(df.to_string(index=False))
print("==========================\n")
