import os, time
import duckdb, pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from openpyxl import Workbook

# === Paths ===
base_dir = Path(r"C:\Tritone_Spotify\Data_analytics\tritone-assignment")
db_path = base_dir / "data" / "processed" / "unclaimed.duckdb"
workbook_path = base_dir / "reports" / "tritone_multi_artist_report.xlsx"
log_path = base_dir / "logs" / "07C_multi_artist_crossref.log"

def log(msg):
    print(msg)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

load_dotenv(base_dir / ".env")

# === Spotify auth ===
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=os.getenv("SPOTIFY_CLIENT_ID"),
    client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
    redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
    scope="user-read-private",
    cache_path=base_dir / "config" / ".spotify_token_cache",
    open_browser=False
))
log("Spotify authentication successful.\n")

# === Connect to DuckDB ===
con = duckdb.connect(str(db_path))
log(f"Connected to DuckDB: {db_path}\n")

artists = ["Martin Jacoby", "Armin van Buuren", "Sizzla"]
summary_rows = []
writer = pd.ExcelWriter(workbook_path, engine="openpyxl")

def fetch_full_catalog(artist_name):
    search = sp.search(q=f"artist:{artist_name}", type="artist", limit=1)
    if not search["artists"]["items"]:
        return pd.DataFrame()
    artist_id = search["artists"]["items"][0]["id"]
    albums = sp.artist_albums(artist_id, album_type="album,single,compilation", limit=50)
    tracks = []
    for album in albums["items"]:
        album_id = album["id"]
        album_name = album["name"]
        release_date = album["release_date"]
        for t in sp.album_tracks(album_id)["items"]:
            isrc = t.get("external_ids", {}).get("isrc")
            if not isrc:
                full_t = sp.track(t["id"])
                isrc = full_t.get("external_ids", {}).get("isrc")
            if isrc:
                tracks.append({
                    "artist": artist_name,
                    "track_name": t["name"],
                    "album": album_name,
                    "release_date": release_date,
                    "isrc": isrc.upper().replace("-", "").strip()
                })
        time.sleep(0.2)
    return pd.DataFrame(tracks)

for name in artists:
    log(f"Processing artist: {name}")
    catalog_df = fetch_full_catalog(name)
    if catalog_df.empty:
        log(f"No catalog data for {name}\n")
        continue
    con.register("isrcs", catalog_df[["isrc", "track_name", "album", "release_date"]])
    matches_df = con.execute("""
        SELECT r.*, i.track_name, i.album, i.release_date
        FROM unclaimed_rights r
        JOIN isrcs i ON REPLACE(UPPER(r.ISRC), '-', '') = i.isrc
    """).fetchdf()
    con.unregister("isrcs")

    total_tracks = len(catalog_df)
    matched = len(matches_df["ISRC"].unique())
    coverage = (matched / total_tracks * 100) if total_tracks else 0
    summary_rows.append((name, total_tracks, matched, round(coverage, 2)))

    # save per-artist sheets
    catalog_df.to_excel(writer, sheet_name=f"{name[:20]}_Catalog", index=False)
    matches_df.to_excel(writer, sheet_name=f"{name[:20]}_Matches", index=False)
    log(f"{name}: {matched}/{total_tracks} matched ({coverage:.2f}%)\n")

con.close()
log("Connection closed.\n")

# === Summary sheet ===
summary_df = pd.DataFrame(summary_rows, columns=["Artist","Total Tracks","Matched","Coverage_%"])
summary_df.to_excel(writer, sheet_name="Summary", index=False)
writer.close()

log(f"Final workbook saved to: {workbook_path}\n")
print("\n===== FINAL SUMMARY =====")
print(summary_df.to_string(index=False))
print("=========================\n")
