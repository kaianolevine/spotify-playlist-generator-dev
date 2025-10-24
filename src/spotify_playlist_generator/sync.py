"""
sync.py — Main integration script for Westie Radio automation.
"""

import os
from datetime import datetime

import kaiano_common_utils.google_drive as drive
import kaiano_common_utils.google_sheets as sheets
import kaiano_common_utils.m3u_parsing as m3u
import kaiano_common_utils.sheets_formatting as formatting
from dotenv import load_dotenv
from googleapiclient.errors import HttpError
from kaiano_common_utils import logger as log
from kaiano_common_utils import spotify

import spotify_playlist_generator.config as config

log = log.get_logger()


# --- Logging spreadsheet management ---
def get_or_create_logging_spreadsheet():
    """
    Locate the logging spreadsheet by name in the configured folder, or create it if missing.
    Ensures the required sheets exist.
    """
    folder_id = config.HISTORY_TO_SPOTIFY_FOLDER_ID
    spreadsheet_name = config.HISTORY_TO_SPOTIFY_SPREADSHEET_NAME
    drive_service = drive.get_drive_service()

    # Search for a Google Sheet with the given name in the folder
    files = drive.list_files_in_folder(drive_service, folder_id)
    for f in files:
        if f["name"] == spreadsheet_name and f.get("mimeType", "").startswith(
            "application/vnd.google-apps.spreadsheet"
        ):
            spreadsheet_id = f["id"]
            setup_logging_spreadsheet(spreadsheet_id)
            return spreadsheet_id

    # Not found: create new spreadsheet, move to folder
    spreadsheet_id = drive.create_spreadsheet(
        drive_service, spreadsheet_name, folder_id
    )
    setup_logging_spreadsheet(spreadsheet_id)
    log.info(
        f"Created new logging spreadsheet '{spreadsheet_name}' in folder {folder_id}."
    )
    return spreadsheet_id


def setup_logging_spreadsheet(spreadsheet_id):
    """
    Ensure the logging spreadsheet contains only the required sheets with correct headers.
    """
    required_sheets = {
        "Info": ["Timestamp", "Message", "Processed", "Found", "Unfound"],
        "Processed": ["Filename", "Playlist ID", "ExtVDJLine"],
        "Songs Added": ["Date", "Title", "Artist"],
        "Songs Not Found": ["Date", "Title", "Artist"],
    }
    sheet_service = sheets.get_sheets_service()
    # Create/ensure each required sheet with headers
    for sheet_name, headers in required_sheets.items():
        sheets.ensure_sheet_exists(
            sheet_service, spreadsheet_id, sheet_name, headers=headers
        )
    # Remove any other sheets
    try:
        metadata = sheets.get_sheet_metadata(sheet_service, spreadsheet_id)
        for sheet_info in metadata.get("sheets", []):
            title = sheet_info.get("properties", {}).get("title", "")
            if title not in required_sheets:
                sheets.delete_sheet_by_name(sheet_service, spreadsheet_id, title)
                log.info(f"🗑 Deleted extraneous sheet '{title}'.")
    except HttpError as e:
        log.error(f"⚠️ Failed to clean up sheets: {e}")


def log_info_sheet(
    service,
    spreadsheet_id: str,
    message: str = None,
    processed: str = None,
    found: str = None,
    unfound: str = None,
):

    timestamp = datetime.now().replace(microsecond=0).isoformat(sep=" ")

    # Ensure Info sheet exists with correct headers
    sheets.ensure_sheet_exists(
        service,
        spreadsheet_id,
        "Info",
        headers=["Timestamp", "Message", "Processed", "Found", "Unfound"],
    )

    # Determine which type of row to append
    if all(v is not None for v in [message, processed, found, unfound]):
        row = [[timestamp, message, processed, found, unfound]]
    elif message:
        row = [[timestamp, message, "", "", ""]]
    else:
        log.warning("⚠️ No message or data provided to log_info_sheet.")
        return

    try:
        sheets.append_rows(service, spreadsheet_id, "Info!A1", row)
        log.info(f"🧾 Logged Info row: {row}")
    except Exception as e:
        log.error(f"⚠️ Failed to append Info row: {e}")


def log_start(sheet_service, spreadsheet_id):
    log_info_sheet(
        sheet_service,
        spreadsheet_id,
        "🔄 Starting Radio Sync...",
    )
    log.info("Starting debug logging for Westie Radio sync.")


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


def update_spotify_radio_playlist(playlist_id, found_uris):
    try:
        spotify.add_tracks_to_specific_playlist(playlist_id, found_uris)
        spotify.trim_playlist_to_limit()
    except Exception as e:
        log.error(f"Error updating Spotify playlist: {e}")


def create_spotify_playlist_for_file(date_str: str, found_uris: list[str]) -> str:
    if not found_uris:
        log.warning("No URIs provided; skipping playlist creation.")
        return None

    playlist_name = f"{date_str} History Set"
    log.debug(f"🎵 Preparing to create/update Spotify playlist: {playlist_name}")
    try:
        # Check for existing playlist
        existing_playlist = spotify.find_playlist_by_name(playlist_name)
        if existing_playlist:
            playlist_id = existing_playlist["id"]
            log.info(
                f"📝 Playlist '{playlist_name}' already exists, updating existing playlist (ID: {playlist_id})."
            )

            if found_uris:
                spotify.add_tracks_to_specific_playlist(playlist_id, found_uris)
                log.debug(
                    f"✅ Added {len(found_uris)} (not necessarily) new tracks to existing playlist {playlist_name} (ID: {playlist_id})."
                )
            else:
                log.debug(
                    f"ℹ️ No new tracks to add to existing playlist {playlist_name}."
                )
            return playlist_id
        else:
            # Create new playlist as before
            playlist_id = spotify.create_playlist(playlist_name)
            if not playlist_id:
                log.error(f"❌ Failed to create playlist: {playlist_name}")
                return None
            unique_uris = list(dict.fromkeys(found_uris))
            duplicates_count = len(found_uris) - len(unique_uris)
            log.debug(f"🔍 Removing duplicates: {duplicates_count} duplicates removed.")
            spotify.add_tracks_to_specific_playlist(playlist_id, unique_uris)
            log.debug(
                f"✅ Created playlist {playlist_name} with ID {playlist_id} containing {len(unique_uris)} tracks."
            )
            return playlist_id
    except Exception as e:
        log.error(
            f"❌ Exception while creating/updating Spotify playlist {playlist_name}: {e}"
        )
        return None


whitespace_buffer = ""


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
    playlist_id=None,
):
    log_info_sheet(
        sheet_service,
        spreadsheet_id,
        f"🎶 Processed file: {filename}{whitespace_buffer}",
        f"Processed rows: {len(new_songs)}{whitespace_buffer}",
        f"✅ Found tracks: {len(found_uris)}{whitespace_buffer}",
        f"❌ Unfound tracks: {len(unfound)}{whitespace_buffer}",
    )

    # Log Songs Added
    sheet = sheets.read_sheet(sheet_service, spreadsheet_id, "Songs Added")
    log.debug(f"📋 Loaded sheet: {sheet}")

    for (artist, title), uri in zip(matched_songs, found_uris):
        log.debug(f"📝 Would log synced track: {date}, {title} - {artist}")
    rows_to_append = [[date, title, artist] for (artist, title) in matched_songs]
    if rows_to_append:
        log.debug(f"🧪 Writing {len(rows_to_append)} rows to sheet...")
        try:
            sheets.append_rows(
                sheet_service, spreadsheet_id, "Songs Added", rows_to_append
            )
        except Exception as e:
            log.error(f"Failed to append to Songs Added: {e}")
    else:
        log.debug("🧪 No rows to write to Songs Added.")

    # Log unfound songs to "Songs Not Found"
    unfound_rows = [[date, title, artist] for artist, title, _ in unfound]
    if unfound_rows:
        log.debug(f"🧪 Unfound Tracks: {len(unfound_rows)}")
        try:
            sheets.append_rows(
                sheet_service, spreadsheet_id, "Songs Not Found", unfound_rows
            )
        except Exception as e:
            log.error(f"Failed to append to Songs Not Found: {e}")

    # Log processing summary to "Processed" tab
    last_logged_extvdj_line = new_songs[-1][2] if new_songs else last_extvdj_line
    if playlist_id:
        updated_row = [filename, playlist_id, last_logged_extvdj_line]
        log.debug(f"Logging playlist ID in Processed sheet: {playlist_id}")
    else:
        updated_row = [filename, last_logged_extvdj_line]
    try:
        log.debug(f"Updating Processed log: {updated_row}")
        log.debug(f"Last logged ExtVDJ line: {last_logged_extvdj_line}")
        all_rows = sheets.read_sheet(sheet_service, spreadsheet_id, "Processed!A2:C")
        log.debug(f"all rows: {all_rows}")
        filenames = [row[0] for row in all_rows]
        log.debug(f"all filenames: {filenames}")
        if filename in filenames:
            log.debug(f"Found filename in processed: {filename}")
            row_index = filenames.index(filename) + 2  # account for header
            log.debug(f"Updating row {row_index} in Processed")
            sheets.update_row(
                spreadsheet_id,
                f"Processed!A{row_index}:C{row_index}",
                [updated_row],
            )
        else:
            sheets.append_rows(
                sheet_service, spreadsheet_id, "Processed", [updated_row]
            )
            log.debug(f"Appended new row to Processed: {updated_row}")
        sheets.sort_sheet_by_column(
            sheet_service, spreadsheet_id, "Processed", column_index=2, ascending=False
        )
    except Exception as e:
        log.error(f"Failed to update Processed log: {e}")


def process_file(file, processed_map, sheet_service, spreadsheet_id, drive_service):
    filename = file["name"]
    file_id = file["id"]
    date = drive.extract_date_from_filename(filename)

    try:
        drive.download_file(drive_service, file_id, filename)
        songs = m3u.parse_m3u(sheet_service, filename, spreadsheet_id)

        last_extvdj_line = processed_map.get(filename)
        new_songs = process_new_songs(songs, last_extvdj_line)
        if not new_songs:
            log.debug(
                f"🎶 Processed file: {filename}, "
                f"Processed rows: {0}, "
                f"✅ Found tracks: {0}, "
                f"❌ Unfound tracks: {0}"
            )
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

        update_spotify_radio_playlist(config.SPOTIFY_PLAYLIST_ID, found_uris)

        playlist_id = create_spotify_playlist_for_file(date, found_uris)
        if playlist_id:
            log.info(f"✅ Playlist created successfully with ID: {playlist_id}")
        else:
            log.error(f"❌ Playlist creation failed for date: {date}")

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
            playlist_id=playlist_id,
        )
    finally:
        try:
            os.remove(filename)
            log.debug(f"🧹 Deleted temporary file: {filename}")
        except Exception as e:
            log.warning(f"⚠️ Could not delete file {filename}: {e}")


def main():
    load_dotenv()  # load environment variables

    sheet_service = sheets.get_sheets_service()
    spreadsheet_id = get_or_create_logging_spreadsheet()
    log_start(sheet_service, spreadsheet_id)

    folder_id = config.VDJ_HISTORY_FOLDER_ID
    if not folder_id:
        log.critical("Missing environment variable: VDJ_HISTORY_FOLDER_ID")
        raise ValueError("Missing environment variable: VDJ_HISTORY_FOLDER_ID")
    log.info(f"📁 Loaded VDJ_HISTORY_FOLDER_ID: {folder_id}")

    drive_service = drive.get_drive_service()
    m3u_files = get_m3u_files(drive_service, folder_id)

    if not m3u_files:
        log_info_sheet(sheet_service, spreadsheet_id, "❌ No .m3u files found.")
        return

    processed_map = load_processed_map(sheet_service, spreadsheet_id)

    for file in m3u_files:
        process_file(file, processed_map, sheet_service, spreadsheet_id, drive_service)

    formatting.apply_formatting_to_sheet(spreadsheet_id)
    log_info_sheet(sheet_service, spreadsheet_id, "✅ Sync complete.")
    log.info("✅ Sync complete.")


if __name__ == "__main__":
    main()
