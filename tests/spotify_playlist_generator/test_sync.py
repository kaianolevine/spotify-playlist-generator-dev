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


def test_initialize_logging_spreadsheet_creates_sheets(monkeypatch):
    mock_sheet = MagicMock()
    monkeypatch.setattr(sync.sheets, "get_sheets_service", lambda: mock_sheet)
    monkeypatch.setattr(sync.sheets, "ensure_sheet_exists", MagicMock())
    monkeypatch.setattr(
        sync.sheets, "get_sheet_metadata", lambda *a, **kw: {"sheets": []}
    )
    sync.get_or_create_logging_spreadsheet()

    # Assert calls exist, but ignore the actual spreadsheet_id value
    calls = sync.sheets.ensure_sheet_exists.call_args_list
    assert any(call.args[2] == "Processed" for call in calls)
    assert any(call.args[2] == "Songs Added" for call in calls)


def test_initialize_logging_spreadsheet_deletes_default(monkeypatch):
    mock_sheet = MagicMock()
    monkeypatch.setattr(sync.sheets, "get_sheets_service", lambda: mock_sheet)
    monkeypatch.setattr(sync.sheets, "ensure_sheet_exists", MagicMock())
    monkeypatch.setattr(
        sync.sheets,
        "get_sheet_metadata",
        lambda *a, **kw: {
            "sheets": [{"properties": {"title": "Sheet1", "sheetId": 123}}]
        },
    )
    monkeypatch.setattr(sync.sheets, "delete_sheet_by_name", MagicMock())

    sync.get_or_create_logging_spreadsheet()
    call_args = sync.sheets.delete_sheet_by_name.call_args
    assert call_args.args[2] == "Sheet1"


def test_get_m3u_files_filters(monkeypatch):
    mock_drive = MagicMock()
    monkeypatch.setattr(
        sync.drive,
        "list_files_in_folder",
        lambda *_: [{"name": "a.m3u"}, {"name": "b.txt"}],
    )
    files = sync.get_m3u_files(mock_drive, "folder_id")
    assert len(files) == 1
    assert files[0]["name"].endswith(".m3u")


def test_load_processed_map(monkeypatch):
    mock_sheets = MagicMock()
    mock_data = [["file1", "date", "line1"], ["file2", "date", "line2"]]
    monkeypatch.setattr(sync.sheets, "read_sheet", lambda *_: mock_data)
    result = sync.load_processed_map(mock_sheets, "spreadsheet_id")
    assert result == {"file1": "line1", "file2": "line2"}


def test_process_new_songs_handles_existing_and_missing(monkeypatch):
    songs = [
        ("artist", "title", "A"),
        ("artist", "title", "B"),
        ("artist", "title", "C"),
    ]
    assert sync.process_new_songs(songs, "B") == [("artist", "title", "C")]
    assert sync.process_new_songs(songs, None) == songs
    assert sync.process_new_songs(songs, "Z") == songs


def test_update_spotify_radio_playlist_success(monkeypatch):
    monkeypatch.setattr(sync.spotify, "add_tracks_to_playlist", MagicMock())
    monkeypatch.setattr(sync.spotify, "trim_playlist_to_limit", MagicMock())
    sync.update_spotify_radio_playlist(["uri1", "uri2"])
    sync.spotify.add_tracks_to_playlist.assert_called_once()
    sync.spotify.trim_playlist_to_limit.assert_called_once()


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


def test_log_to_sheets(monkeypatch):
    mock_sheets = MagicMock()
    monkeypatch.setattr(sync.sheets, "append_rows", MagicMock())
    monkeypatch.setattr(
        sync.sheets, "read_sheet", lambda *a, **kw: [["f1", "date", "line1"]]
    )
    monkeypatch.setattr(sync.sheets, "update_row", MagicMock())
    monkeypatch.setattr(sync.sheets, "sort_sheet_by_column", MagicMock())
    sync.log_to_sheets(
        mock_sheets,
        "spreadsheet_id",
        "2023-01-01",
        [("a", "b")],
        ["uri1"],
        [("x", "y", "z")],
        "file1",
        [("a", "b", "c")],
        "line1",
        "playlist",
    )
    sync.sheets.append_rows.assert_any_call(
        mock_sheets, "spreadsheet_id", "Songs Added", [["2023-01-01", "b", "a"]]
    )
    sync.sheets.sort_sheet_by_column.assert_called_once()


def test_process_file_happy_path(monkeypatch):
    mock_sheet, mock_drive = MagicMock(), MagicMock()
    file = {"name": "test.m3u", "id": "fileid"}
    monkeypatch.setattr(
        sync.drive, "extract_date_from_filename", lambda n: "2023-01-01"
    )
    monkeypatch.setattr(sync.drive, "download_file", MagicMock())
    monkeypatch.setattr(sync.m3u, "parse_m3u", lambda *a, **kw: [("a", "b", "line1")])
    monkeypatch.setattr(sync.spotify, "search_track", lambda a, t: "uri1")
    monkeypatch.setattr(sync, "update_spotify_radio_playlist", MagicMock())
    monkeypatch.setattr(
        sync, "create_spotify_playlist_for_file", lambda d, u: "playlistid"
    )
    monkeypatch.setattr(sync, "log_to_sheets", MagicMock())
    processed_map = {}
    sync.process_file(file, processed_map, mock_sheet, "spreadsheet_id", mock_drive)
    sync.update_spotify_radio_playlist.assert_called_once()
    sync.log_to_sheets.assert_called_once()


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


def test_log_to_sheets_handles_update_failure(monkeypatch):
    """Covers log_to_sheets failure branch."""
    mock_sheets = MagicMock()
    monkeypatch.setattr(sync.sheets, "append_rows", MagicMock())
    monkeypatch.setattr(
        sync.sheets,
        "update_row",
        lambda *a, **kw: (_ for _ in ()).throw(Exception("fail")),
    )
    monkeypatch.setattr(sync.sheets, "sort_sheet_by_column", MagicMock())
    sync.log_to_sheets(
        mock_sheets,
        "spreadsheet_id",
        "2023-01-01",
        [("a", "b")],
        ["uri1"],
        [("x", "y", "z")],
        "file1",
        [("a", "b", "c")],
        "line1",
        "playlist",
    )


def test_process_file_skips_processed(monkeypatch):
    """Covers branch where file already processed."""
    mock_sheet, mock_drive = MagicMock(), MagicMock()
    file = {"name": "test.m3u", "id": "fileid"}
    processed = {"test.m3u": "already"}
    monkeypatch.setattr(
        sync.drive, "extract_date_from_filename", lambda n: "2023-01-01"
    )
    monkeypatch.setattr(sync.drive, "download_file", MagicMock())
    sync.process_file(file, processed, mock_sheet, "spreadsheet_id", mock_drive)
