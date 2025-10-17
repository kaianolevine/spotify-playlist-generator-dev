from unittest.mock import MagicMock

import pytest

from spotify_playlist_generator import sync


@pytest.fixture
def mock_services():
    """Provide mocked sheet and drive services and config IDs."""
    sheet_service = MagicMock()
    drive_service = MagicMock()
    spreadsheet_id = "mock_spreadsheet_id"
    folder_id = "mock_folder_id"
    return sheet_service, drive_service, spreadsheet_id, folder_id


def test_initialize_spreadsheet_deletes_sheet1(mocker):
    """Ensure 'Sheet1' is deleted if found."""
    mocker.patch(
        "kaiano_common_utils.google_sheets.get_sheets_service", return_value=MagicMock()
    )
    mocker.patch("kaiano_common_utils.google_sheets.ensure_sheet_exists")
    mocker.patch(
        "kaiano_common_utils.google_sheets.get_sheet_metadata",
        return_value={"sheets": [{"properties": {"title": "Sheet1", "sheetId": 123}}]},
    )
    mocker.patch("kaiano_common_utils.google_sheets.delete_sheet_by_name")
    sync.initialize_spreadsheet()
    core = pytest.importorskip("kaiano_common_utils.google_sheets")
    core.delete_sheet_by_name.assert_called_once()


def test_log_start_writes_info(mocker, mock_services):
    sheet_service, _, spreadsheet_id, _ = mock_services
    mocker.patch("kaiano_common_utils.google_sheets.log_info_sheet")
    sync.log_start(sheet_service, spreadsheet_id)
    core = pytest.importorskip("kaiano_common_utils.google_sheets")
    core.log_info_sheet.assert_called_once()
    assert "Westie Radio sync" in core.log_info_sheet.call_args[0][2]


def test_get_m3u_files_filters_and_sorts(mocker, mock_services):
    _, drive_service, _, folder_id = mock_services
    mocker.patch(
        "kaiano_common_utils.google_drive.list_files_in_folder",
        return_value=[
            {"name": "B.m3u", "id": "2"},
            {"name": "A.m3u", "id": "1"},
            {"name": "ignore.txt", "id": "3"},
        ],
    )
    files = sync.get_m3u_files(drive_service, folder_id)
    assert [f["name"] for f in files] == ["A.m3u", "B.m3u"]


def test_load_processed_map_returns_dict(mocker, mock_services):
    sheet_service, _, spreadsheet_id, _ = mock_services
    mocker.patch(
        "kaiano_common_utils.google_sheets.read_sheet",
        return_value=[
            ["file1.m3u", "2024-10-01", "line123"],
            ["file2.m3u", "2024-10-02", "line456"],
        ],
    )
    result = sync.load_processed_map(sheet_service, spreadsheet_id)
    assert result == {"file1.m3u": "line123", "file2.m3u": "line456"}


def test_process_new_songs_skips_already_processed(caplog):
    songs = [
        ("artist1", "title1", "line1"),
        ("artist2", "title2", "line2"),
        ("artist3", "title3", "line3"),
    ]
    new_songs = sync.process_new_songs(songs, "line2")
    assert new_songs == [("artist3", "title3", "line3")]


def test_process_new_songs_no_match_process_all(caplog):
    songs = [("a", "b", "1")]
    result = sync.process_new_songs(songs, "not_found")
    assert result == songs


def test_update_spotify_calls_playlist_methods(mocker):
    mock_add = mocker.patch("kaiano_common_utils.spotify.add_tracks_to_playlist")
    mock_trim = mocker.patch("kaiano_common_utils.spotify.trim_playlist_to_limit")
    sync.update_spotify(["uri1", "uri2"])
    mock_add.assert_called_once_with(["uri1", "uri2"])
    mock_trim.assert_called_once()


def test_log_to_sheets_appends_data(mocker, mock_services):
    sheet_service, _, spreadsheet_id, _ = mock_services
    mocker.patch(
        "kaiano_common_utils.google_sheets.read_sheet",
        return_value=[["test.m3u", "2025-10-16", "old_line"]],
    )
    mocker.patch("kaiano_common_utils.google_sheets.append_rows")
    mocker.patch("kaiano_common_utils.google_sheets.update_row")
    mocker.patch("kaiano_common_utils.google_sheets.sort_sheet_by_column")

    sync.log_to_sheets(
        sheet_service,
        spreadsheet_id,
        "2025-10-17",
        [("Artist", "Title")],
        ["spotify:track:123"],
        [("MissingArtist", "MissingTitle", "lineZ")],
        "test.m3u",
        [("Artist", "Title", "lineX")],
        "lineY",
    )

    core = pytest.importorskip("kaiano_common_utils.google_sheets")
    core.append_rows.assert_any_call(
        spreadsheet_id, "Songs Added", [["2025-10-17", "Title", "Artist"]]
    )
    core.append_rows.assert_any_call(
        spreadsheet_id,
        "Songs Not Found",
        [["2025-10-17", "MissingTitle", "MissingArtist"]],
    )
    core.update_row.assert_called()


def test_process_file_handles_full_flow(mocker, mock_services):
    sheet_service, drive_service, spreadsheet_id, _ = mock_services

    mocker.patch(
        "kaiano_common_utils.google_sheets.read_sheet",
        lambda *args, **kwargs: [["file.m3u", "2025-10-16", "line1"]],
    )
    mocker.patch(
        "kaiano_common_utils.google_drive.extract_date_from_filename",
        return_value="2025-10-17",
    )
    mocker.patch("kaiano_common_utils.google_drive.download_file")
    mocker.patch(
        "spotify_playlist_generator.sync.m3u.parse_m3u",
        return_value=[("Artist", "Title", "line1")],
    )
    mocker.patch(
        "kaiano_common_utils.spotify.search_track", return_value="spotify:track:1"
    )
    mock_update = mocker.patch("spotify_playlist_generator.sync.update_spotify")
    mock_log = mocker.patch("spotify_playlist_generator.sync.log_to_sheets")

    processed_map = {"file.m3u": None}
    file = {"name": "file.m3u", "id": "123"}

    sync.process_file(file, processed_map, sheet_service, spreadsheet_id, drive_service)

    mock_update.assert_called_once()
    mock_log.assert_called_once()


def test_process_file_handles_no_songs(mocker, mock_services):
    sheet_service, drive_service, spreadsheet_id, _ = mock_services
    file = {"name": "file.m3u", "id": "123"}
    processed_map = {"file.m3u": None}
    mocker.patch("kaiano_common_utils.google_drive.download_file")
    mocker.patch("kaiano_common_utils.m3u_parsing.parse_m3u", return_value=[])
    mock_log = mocker.patch("spotify_playlist_generator.sync.log_to_sheets")
    sync.process_file(file, processed_map, sheet_service, spreadsheet_id, drive_service)
    mock_log.assert_not_called()


def test_process_file_skips_processed(mocker, mock_services):
    sheet_service, drive_service, spreadsheet_id, _ = mock_services
    file = {"name": "file.m3u", "id": "123"}
    processed_map = {"file.m3u": "line123"}
    mocker.patch("kaiano_common_utils.google_drive.download_file")
    mocker.patch(
        "kaiano_common_utils.m3u_parsing.parse_m3u", return_value=[]
    )  # ⬅️ added line
    mock_log = mocker.patch("spotify_playlist_generator.sync.log_to_sheets")
    mock_update = mocker.patch("spotify_playlist_generator.sync.update_spotify")

    sync.process_file(file, processed_map, sheet_service, spreadsheet_id, drive_service)

    mock_log.assert_not_called()
    mock_update.assert_not_called()


def test_get_m3u_files_empty_returns_empty(mocker, mock_services):
    _, drive_service, _, folder_id = mock_services
    mocker.patch(
        "kaiano_common_utils.google_drive.list_files_in_folder", return_value=[]
    )
    assert sync.get_m3u_files(drive_service, folder_id) == []


def test_main_full_flow_mocks_everything(mocker):
    """Simulate a clean run of main() without API calls."""
    mocker.patch(
        "kaiano_common_utils.google_sheets.get_sheets_service", return_value=MagicMock()
    )
    mocker.patch(
        "kaiano_common_utils.google_drive.get_drive_service", return_value=MagicMock()
    )
    mocker.patch("kaiano_common_utils.config.HISTORY_TO_SPOTIFY_LOGGING", "mock_id")
    mocker.patch("kaiano_common_utils.config.VDJ_HISTORY_FOLDER_ID", "mock_folder")
    mocker.patch("spotify_playlist_generator.sync.initialize_spreadsheet")
    mocker.patch(
        "spotify_playlist_generator.sync.get_m3u_files",
        return_value=[{"name": "f.m3u", "id": "1"}],
    )
    mocker.patch("spotify_playlist_generator.sync.load_processed_map", return_value={})
    mocker.patch("spotify_playlist_generator.sync.process_file")
    mocker.patch("kaiano_common_utils.google_sheets.log_info_sheet")

    sync.main()

    sync.initialize_spreadsheet.assert_called_once()
    sync.get_m3u_files.assert_called_once()
    sync.process_file.assert_called_once()
    core = pytest.importorskip("kaiano_common_utils.google_sheets")
    core.log_info_sheet.assert_any_call(mocker.ANY, "mock_id", "✅ Sync complete.")


def test_main_handles_no_files(mocker):
    mocker.patch(
        "kaiano_common_utils.google_sheets.get_sheets_service", return_value=MagicMock()
    )
    mocker.patch(
        "kaiano_common_utils.google_drive.get_drive_service", return_value=MagicMock()
    )
    mocker.patch("kaiano_common_utils.config.HISTORY_TO_SPOTIFY_LOGGING", "mock_id")
    mocker.patch("kaiano_common_utils.config.VDJ_HISTORY_FOLDER_ID", "mock_folder")
    mocker.patch("spotify_playlist_generator.sync.initialize_spreadsheet")
    mocker.patch("spotify_playlist_generator.sync.get_m3u_files", return_value=[])
    mock_log = mocker.patch("kaiano_common_utils.google_sheets.log_info_sheet")
    sync.main()
    mock_log.assert_any_call(mocker.ANY, "mock_id", "❌ No .m3u files found.")
