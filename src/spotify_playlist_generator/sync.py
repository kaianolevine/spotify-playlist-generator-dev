"""
sync.py — Main integration script for Westie Radio automation.
"""

from datetime import datetime

import kaiano_common_utils.config as config
import kaiano_common_utils.google_drive as drive
import kaiano_common_utils.google_sheets as sheets
import kaiano_common_utils.m3u_parsing as m3u
from googleapiclient.errors import HttpError
from kaiano_common_utils import logger as log
from kaiano_common_utils import spotify

log = log.get_logger()

spreadsheet_id = config.HISTORY_TO_SPOTIFY_LOGGING


def initialize_spreadsheet():
    """Ensure necessary sheets exist and remove default 'Sheet1' if present."""
    sheet_service = sheets.get_sheets_service()

    # Ensure necessary sheets
    sheets.ensure_sheet_exists(
        sheet_service,
        spreadsheet_id,
        "Processed",
        headers=["Filename", "Date", "ExtVDJLine"],
    )
    sheets.ensure_sheet_exists(
        sheet_service,
        spreadsheet_id,
        "Songs Added",
        headers=["Date", "Title", "Artist"],
    )
    sheets.ensure_sheet_exists(
        sheet_service,
        spreadsheet_id,
        "Songs Not Found",
        headers=["Date", "Title", "Artist"],
    )

    # Attempt to delete 'Sheet1' if it exists
    try:
        metadata = sheets.get_sheet_metadata(sheet_service, spreadsheet_id)
        for sheet_info in metadata.get("sheets", []):
            title = sheet_info.get("properties", {}).get("title", "")
            sheet_id = sheet_info.get("properties", {}).get("sheetId", None)
            if title == "Sheet1" and sheet_id is not None:
                sheets.delete_sheet_by_name(sheet_service, spreadsheet_id, "Sheet1")
                log.debug("🗑 Deleted default 'Sheet1'.")
    except HttpError as e:
        log.debug(f"⚠️ Failed to delete 'Sheet1': {e}")


def log_start(sheet_service, spreadsheet_id):
    sheets.log_info_sheet(
        sheet_service,
        spreadsheet_id,
        f"🔄 Starting Westie Radio sync at {datetime.now().replace(microsecond=0).isoformat()}...",
    )
    log.debug("Starting debug logging for Westie Radio sync.")


def get_m3u_files(drive_service, folder_id):
    all_files = drive.list_files_in_folder(drive_service, folder_id)
    m3u_files = sorted(
        [f for f in all_files if f["name"].lower().endswith(".m3u")],
        key=lambda f: f["name"],
    )
    return m3u_files


def load_processed_map(sheet_service, spreadsheet_id):
    processed_rows = sheets.read_sheet(sheet_service, spreadsheet_id, "Processed!A2:C")
    processed_map = {row[0]: row[2] for row in processed_rows if len(row) >= 3}
    return processed_map


def process_new_songs(songs, last_extvdj_line):
    new_songs = songs
    if last_extvdj_line:
        try:
            last_index = [s[2] for s in songs].index(last_extvdj_line)
            new_songs = songs[last_index + 1 :]
            log.debug(f"⚙️ Skipping {last_index + 1} already-processed songs.")
            if not new_songs:
                log.debug("🛑 No new songs, skipping.")
                return []
        except ValueError:
            log.debug("⚠️ Last logged song not found, processing full file.")
    return new_songs


def update_spotify(found_uris):
    try:
        spotify.add_tracks_to_playlist(found_uris)
        spotify.trim_playlist_to_limit()
    except Exception as e:
        log.debug(f"Error updating Spotify playlist: {e}")


def log_to_sheets(
    sheet_service,
    spreadsheet_id,
    date,
    matched_songs,
    found_uris,
    unfound,
    filename,
    new_songs,
    last_extvdj_line,
):
    sheets.log_info_sheet(
        sheet_service,
        spreadsheet_id,
        f"✅ Found {len(found_uris)} tracks, ❌ {len(unfound)} unfound",
    )

    # Log Songs Added
    sheet = sheets.read_sheet(spreadsheet_id, "Songs Added")
    log.debug(f"📋 Loaded sheet: {sheet}")

    for (artist, title), uri in zip(matched_songs, found_uris):
        log.debug(
            spreadsheet_id, f"📝 Would log synced track: {date}, {title} - {artist}"
        )
    rows_to_append = [[date, title, artist] for (artist, title) in matched_songs]
    if rows_to_append:
        log.debug(f"🧪 Writing {len(rows_to_append)} rows to sheet...")
        try:
            sheets.append_rows(spreadsheet_id, "Songs Added", rows_to_append)
        except Exception as e:
            log.debug(f"Failed to append to Songs Added: {e}")
    else:
        log.debug("🧪 No rows to write to Songs Added.")

    # Log unfound tracks info messages
    for artist, title, _ in unfound:
        sheets.log_info_sheet(
            sheet_service,
            spreadsheet_id,
            f"❌ Would log unfound track: {date} - {artist} - {title}",
        )

    # Log unfound songs to "Songs Not Found"
    unfound_rows = [[date, title, artist] for artist, title, _ in unfound]
    if unfound_rows:
        try:
            sheets.append_rows(spreadsheet_id, "Songs Not Found", unfound_rows)
        except Exception as e:
            log.debug(f"Failed to append to Songs Not Found: {e}")

    # Log processing summary to "Processed" tab
    last_logged_extvdj_line = new_songs[-1][2] if new_songs else last_extvdj_line
    updated_row = [filename, date, last_logged_extvdj_line]
    try:
        all_rows = sheets.read_sheet(spreadsheet_id, "Processed!A2:C")
        filenames = [row[0] for row in all_rows]
        if filename in filenames:
            row_index = filenames.index(filename) + 2  # account for header
            sheets.update_row(
                spreadsheet_id,
                f"Processed!A{row_index}:C{row_index}",
                [updated_row],
            )
        else:
            sheets.append_rows(spreadsheet_id, "Processed", [updated_row])
        sheets.sort_sheet_by_column(
            spreadsheet_id, "Processed", column_index=2, descending=True
        )
    except Exception as e:
        log.debug(f"Failed to update Processed log: {e}")


def process_file(file, processed_map, sheet_service, spreadsheet_id, drive_service):
    filename = file["name"]
    file_id = file["id"]
    date = drive.extract_date_from_filename(filename)
    sheets.log_info_sheet(
        sheet_service, spreadsheet_id, f"🎶 Processing file: {filename}"
    )

    drive.download_file(drive_service, file_id, filename)
    songs = m3u.parse_m3u(sheet_service, filename, spreadsheet_id)

    last_extvdj_line = processed_map.get(filename)
    new_songs = process_new_songs(songs, last_extvdj_line)
    if not new_songs:
        return

    # --- Spotify: search and collect URIs ---
    found_uris = []
    matched_songs = []
    matched_extvdj_lines = []
    unfound = []
    for artist, title, extvdj_line in new_songs:
        uri = spotify.search_track(artist, title)
        log.debug(
            f"Searching for track - Artist: {artist}, Title: {title}, Found URI: {uri}"
        )
        if uri:
            found_uris.append(uri)
            matched_songs.append((artist, title))
            matched_extvdj_lines.append(extvdj_line)
        else:
            unfound.append((artist, title, extvdj_line))

    update_spotify(found_uris)

    log_to_sheets(
        sheet_service,
        spreadsheet_id,
        date,
        matched_songs,
        found_uris,
        unfound,
        filename,
        new_songs,
        last_extvdj_line,
    )


def main():
    sheet_service = sheets.get_sheets_service()
    spreadsheet_id = config.HISTORY_TO_SPOTIFY_LOGGING
    log_start(sheet_service, spreadsheet_id)

    initialize_spreadsheet()

    folder_id = config.VDJ_HISTORY_FOLDER_ID
    if not folder_id:
        raise ValueError("Missing environment variable: VDJ_HISTORY_FOLDER_ID")
    log.debug(f"📁 Loaded VDJ_HISTORY_FOLDER_ID: {folder_id}")

    drive_service = drive.get_drive_service()
    m3u_files = get_m3u_files(drive_service, folder_id)

    if not m3u_files:
        sheets.log_info_sheet(sheet_service, spreadsheet_id, "❌ No .m3u files found.")
        return

    processed_map = load_processed_map(sheet_service, spreadsheet_id)

    for file in m3u_files:
        process_file(file, processed_map, sheet_service, spreadsheet_id, drive_service)

    sheets.log_info_sheet(sheet_service, spreadsheet_id, "✅ Sync complete.")


if __name__ == "__main__":
    main()
