import os
from pathlib import Path
from dotenv import load_dotenv

# === Locate .env file at project root ===
env_path = Path(r"C:\Tritone_Spotify\Data_analytics\tritone-assignment") / ".env"
load_dotenv(dotenv_path=env_path)

# === Read variables ===
client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI")

# === Verify ===
print("Checking environment variables...\n")

if not all([client_id, client_secret, redirect_uri]):
    print("Missing one or more variables. Please check your .env file.")
else:
    print("All Spotify environment variables loaded successfully!")
    print(f"Client ID: {client_id[:8]}... (hidden)")
    print(f"Redirect URI: {redirect_uri}")

print("\n.env located at:", env_path)
