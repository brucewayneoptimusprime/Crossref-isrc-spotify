import os
from pathlib import Path
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from tqdm import tqdm
import time

# === Base paths ===
base_dir = Path(r"C:\Tritone_Spotify\Data_analytics\tritone-assignment")
log_path = base_dir / "logs" / "05B_spotify_fetch.log"
output_csv = base_dir / "data" / "interim" / "spotify_catalog.csv"

def log(msg):
    print(msg)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# === Load environment ===
load_dotenv(base_dir / ".env")
client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI")

# === Authenticate ===
scope = "user-read-private"
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=client_id,
    client_secret=client_secret,
    redirect_uri=redirect_uri,
    scope=scope,
    open_browser=True,
    cache_path=base_dir / "config" / ".spotify_token_cache"
))
log("Authenticated with Spotify API.\n")

# === 1. Locate The Beatles artist ID ===
artist_name = "The Beatles"
search_results = sp.search(q=artist_name, type="artist", limit=5)
beatles = next((a for a in search_results['artists']['items']
                if a['name'].lower() == "the beatles"), None)
if not beatles:
    raise ValueError("Could not find The Beatles on Spotify.")
artist_id = beatles['id']
log(f"Artist: {beatles['name']} | ID: {artist_id}\n")

# === 2. Get all albums ===
albums = []
results = sp.artist_albums(artist_id, album_type='album,single,compilation,appears_on', limit=50)
albums.extend(results['items'])
while results['next']:
    results = sp.next(results)
    albums.extend(results['items'])
album_df = pd.DataFrame(albums).drop_duplicates(subset=['id'])
log(f"Albums fetched: {len(album_df)}")

# === 3. Fetch all tracks ===
tracks_data = []
for _, row in tqdm(album_df.iterrows(), total=len(album_df), desc="Fetching albums"):
    album_id = row['id']
    album_name = row['name']
    release_date = row.get('release_date', '')
    time.sleep(0.2)  # gentle throttle
    try:
        tracks = sp.album_tracks(album_id)
        for t in tracks['items']:
            isrc = t.get('external_ids', {}).get('isrc', None)
            tracks_data.append({
                'track_name': t['name'],
                'album_name': album_name,
                'release_date': release_date,
                'isrc': isrc,
                'spotify_track_id': t['id'],
                'spotify_album_id': album_id
            })
    except Exception as e:
        log(f"Error fetching album {album_id}: {e}")

# === 4. Clean & deduplicate ===
df = pd.DataFrame(tracks_data)
df.drop_duplicates(subset=['isrc', 'track_name'], inplace=True)
df.sort_values(by='release_date', inplace=True)

log(f"Total tracks collected: {len(df)}")

# === 5. Save output ===
output_csv.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(output_csv, index=False)
log(f"Saved cleaned catalog to: {output_csv}\n")

log("Spotify catalog fetch completed successfully.")
