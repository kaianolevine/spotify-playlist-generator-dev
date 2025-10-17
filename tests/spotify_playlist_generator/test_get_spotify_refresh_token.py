import os
from unittest.mock import patch

# =====================================================
# Successful token retrieval
# =====================================================


def test_spotify_token_success(monkeypatch):
    fake_token = {"access_token": "abc", "refresh_token": "xyz"}

    class FakeSpotifyOAuth:
        def __init__(self, *args, **kwargs):
            pass

        def get_access_token(self, as_dict=True):
            return fake_token

    monkeypatch.setattr(os, "getenv", lambda k: f"fake_{k}")

    with patch("spotipy.SpotifyOAuth", FakeSpotifyOAuth):
        import importlib

        from spotify_playlist_generator import get_spotify_refresh_token as gsr

        with patch("builtins.print") as mock_print:
            importlib.reload(gsr)
            mock_print.assert_any_call("✅ REFRESH TOKEN:", "xyz")


# =====================================================
# Failure (no token returned)
# =====================================================


def test_spotify_token_failure(monkeypatch):
    class FakeSpotifyOAuth:
        def __init__(self, *args, **kwargs):
            pass

        def get_access_token(self, as_dict=True):
            return None

    monkeypatch.setattr(os, "getenv", lambda k: f"fake_{k}")

    with patch("spotipy.SpotifyOAuth", FakeSpotifyOAuth):
        import importlib

        from spotify_playlist_generator import get_spotify_refresh_token as gsr

        with patch("builtins.print") as mock_print:
            importlib.reload(gsr)
            mock_print.assert_any_call(
                "❌ Failed to retrieve token. Please check your credentials and redirect URI."
            )
