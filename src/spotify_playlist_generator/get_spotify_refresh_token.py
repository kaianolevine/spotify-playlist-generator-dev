from dotenv import load_dotenv
from kaiano_common_utils import config
from spotipy import SpotifyOAuth

# Load credentials from your local .env file
load_dotenv()

sp_oauth = SpotifyOAuth(
    client_id=config.SPOTIPY_CLIENT_ID,
    client_secret=config.SPOTIPY_CLIENT_SECRET,
    redirect_uri=config.SPOTIPY_REDIRECT_URI,
    scope="playlist-modify-public playlist-modify-private",
    open_browser=True,  # Set to False if you'd prefer a manual link
)

token_info = sp_oauth.get_cached_token()

if token_info:
    print("✅ REFRESH TOKEN:", token_info.get("refresh_token"))
else:
    print(
        "❌ Failed to retrieve token. Please check your credentials and redirect URI."
    )
