"""
Full coverage tests for spotify_playlist_generator.sync
These tests mock all external dependencies (Google, Spotify, etc.)
and validate flow control, logging, and error handling.
"""

from unittest.mock import MagicMock, patch

import pytest

import spotify_playlist_generator.sync as sync


@pytest.fixture
def mock_services(monkeypatch):
    sheet_service = MagicMock(name="sheet_service")
    drive_service = MagicMock(name="drive_service")
    monkeypatch.setattr(sync.sheets, "get_sheets_service", lambda: sheet_service)
    monkeypatch.setattr(sync.drive, "get_drive_service", lambda: drive_service)
    return sheet_service, drive_service


@pytest.fixture
def mock_drive_and_sheets():
    with patch.object(sync, "drive") as mock_drive, patch.object(
        sync, "sheets"
    ) as mock_sheets:
        yield mock_drive, mock_sheets


def test_process_new_songs_handles_existing_and_missing(monkeypatch):
    songs = [
        ("artist", "title", "A"),
        ("artist", "title", "B"),
        ("artist", "title", "C"),
    ]
    assert sync.process_new_songs(songs, "B") == [("artist", "title", "C")]
    assert sync.process_new_songs(songs, None) == songs
    assert sync.process_new_songs(songs, "Z") == songs


def test_create_spotify_playlist_new(monkeypatch):
    monkeypatch.setattr(sync.spotify, "find_playlist_by_name", lambda name: None)
    monkeypatch.setattr(sync.spotify, "create_playlist", lambda n: "newid")
    monkeypatch.setattr(sync.spotify, "add_tracks_to_specific_playlist", MagicMock())
    playlist_id = sync.create_spotify_playlist_for_file(
        "2023-01-01", ["u1", "u2", "u1"]
    )
    assert playlist_id == "newid"


def test_create_spotify_playlist_existing(monkeypatch):
    monkeypatch.setattr(sync.spotify, "find_playlist_by_name", lambda n: {"id": "123"})
    monkeypatch.setattr(sync.spotify, "get_playlist_tracks", lambda i: ["u1"])
    monkeypatch.setattr(sync.spotify, "add_tracks_to_specific_playlist", MagicMock())
    playlist_id = sync.create_spotify_playlist_for_file("2023-01-01", ["u1", "u2"])
    assert playlist_id == "123"


def test_create_spotify_playlist_skips_empty(monkeypatch):
    monkeypatch.setattr(sync.spotify, "find_playlist_by_name", lambda n: None)
    result = sync.create_spotify_playlist_for_file("2023-01-01", [])
    assert result is None


def test_create_spotify_playlist_handles_exception(monkeypatch):
    monkeypatch.setattr(
        sync.spotify,
        "find_playlist_by_name",
        lambda n: (_ for _ in ()).throw(Exception("boom")),
    )
    result = sync.create_spotify_playlist_for_file("2023-01-01", ["u1"])
    assert result is None


def test_create_spotify_playlist_for_file_raises(monkeypatch):
    """Covers playlist creation failure."""
    monkeypatch.setattr(sync.spotify, "find_playlist_by_name", lambda n: None)
    monkeypatch.setattr(
        sync.spotify,
        "create_playlist",
        lambda n: (_ for _ in ()).throw(Exception("fail")),
    )
    result = sync.create_spotify_playlist_for_file("2023-01-01", ["u1"])
    assert result is None
