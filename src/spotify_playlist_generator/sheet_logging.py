import time
from datetime import datetime

import kaiano_common_utils.logger as log
import kaiano_common_utils.sheets_formatting as formatting
from googleapiclient.errors import HttpError
from kaiano_common_utils.api.google import GoogleAPI


def delete_sheet_by_name(g: GoogleAPI, spreadsheet_id: str, sheet_name: str) -> None:
    """Delete a sheet tab by its title."""
    meta = g.sheets.get_metadata(spreadsheet_id)
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == sheet_name:
            sheet_id = props.get("sheetId")
            g.sheets.batch_update(
                spreadsheet_id, [{"deleteSheet": {"sheetId": sheet_id}}]
            )
            return


# --- Logging spreadsheet management ---
def get_or_create_logging_spreadsheet(
    g: GoogleAPI, folder_id: str, spreadsheet_name: str
) -> str:
    """
    Locate the logging spreadsheet by name in the configured folder, or create it if missing.
    Ensures the required sheets exist.
    """

    # Search for a Google Sheet with the given name in the folder
    files = g.drive.list_files(folder_id, trashed=False, include_folders=True)
    for f in files:
        if f.name == spreadsheet_name and (f.mime_type or "").startswith(
            "application/vnd.google-apps.spreadsheet"
        ):
            spreadsheet_id = f.id
            setup_logging_spreadsheet(g, spreadsheet_id)
            return spreadsheet_id

    # Not found: create new spreadsheet, move to folder
    spreadsheet_id = g.drive.create_spreadsheet_in_folder(spreadsheet_name, folder_id)

    if not wait_for_spreadsheet_ready(g, spreadsheet_id):
        log.error("❌ Spreadsheet did not become ready in time, continuing anyway...")

    setup_logging_spreadsheet(g, spreadsheet_id)
    log.info(
        f"Created new logging spreadsheet '{spreadsheet_name}' in folder {folder_id}."
    )
    return spreadsheet_id


def wait_for_spreadsheet_ready(
    g: GoogleAPI, spreadsheet_id: str, retries: int = 5, delay: int = 1
) -> bool:
    """Poll until the spreadsheet metadata can be fetched.

    Newly-created Sheets can briefly fail while propagating. This uses the unified
    Sheets facade rather than calling the raw Sheets API directly.
    """
    for attempt in range(1, retries + 1):
        try:
            g.sheets.get_metadata(spreadsheet_id)
            return True
        except Exception:
            log.warning(
                f"Waiting for spreadsheet to propagate ({attempt}/{retries})..."
            )
            time.sleep(delay)

    return False


def setup_logging_spreadsheet(g: GoogleAPI, spreadsheet_id):
    """
    Ensure the logging spreadsheet contains only the required sheets with correct headers.
    """
    required_sheets = {
        "Info": ["Timestamp", "Message", "Processed", "Found", "Unfound"],
        "Processed": ["Filename", "Playlist ID", "ExtVDJLine"],
        "Songs Added": ["Date", "Title", "Artist"],
        "Songs Not Found": ["Date", "Title", "Artist"],
    }
    # Create/ensure each required sheet with headers
    for sheet_name, headers in required_sheets.items():
        g.sheets.ensure_sheet_exists(spreadsheet_id, sheet_name, headers=headers)
    # Remove any other sheets
    try:
        metadata = g.sheets.get_metadata(spreadsheet_id)
        for sheet_info in metadata.get("sheets", []):
            title = sheet_info.get("properties", {}).get("title", "")
            if title not in required_sheets:
                delete_sheet_by_name(g, spreadsheet_id, title)
                log.info(f"🗑 Deleted extraneous sheet '{title}'.")
    except HttpError as e:
        log.error(f"⚠️ Failed to clean up sheets: {e}")


def log_info_sheet(
    g: GoogleAPI,
    spreadsheet_id: str,
    message: str = None,
    processed: str = None,
    found: str = None,
    unfound: str = None,
):

    timestamp = datetime.now().replace(microsecond=0).isoformat(sep=" ")

    # Ensure Info sheet exists with correct headers
    g.sheets.ensure_sheet_exists(
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
        g.sheets.append_values(spreadsheet_id, "Info!A1", row, value_input_option="RAW")
        log.debug(f"🧾 Logged Info row: {row}")
    except Exception as e:
        log.error(f"⚠️ Failed to append Info row: {e}")


def log_start(g: GoogleAPI, spreadsheet_id):
    log_info_sheet(
        g,
        spreadsheet_id,
        "🔄 Starting Radio Sync...",
    )
    log.debug("Starting debug logging for Westie Radio sync.")


def load_processed_map(g: GoogleAPI, spreadsheet_id):
    processed_rows = g.sheets.read_values(spreadsheet_id, "Processed!A2:C")
    processed_map = {row[0]: row[2] for row in processed_rows if len(row) >= 3}
    return processed_map


whitespace_buffer = ""


def log_to_sheets(
    g: GoogleAPI,
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
        g,
        spreadsheet_id,
        f"🎶 Processed file: {filename}{whitespace_buffer}",
        f"Processed rows: {len(new_songs)}{whitespace_buffer}",
        f"✅ Found tracks: {len(found_uris)}{whitespace_buffer}",
        f"❌ Unfound tracks: {len(unfound)}{whitespace_buffer}",
    )

    for (artist, title), uri in zip(matched_songs, found_uris):
        log.debug(f"📝 Log synced track: {date}, {title} - {artist}")
    rows_to_append = [[date, title, artist] for (artist, title) in matched_songs]
    if rows_to_append:
        log.debug(f"🧪 Writing {len(rows_to_append)} rows to sheet...")
        try:
            g.sheets.append_values(
                spreadsheet_id, "Songs Added", rows_to_append, value_input_option="RAW"
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
            g.sheets.append_values(
                spreadsheet_id,
                "Songs Not Found",
                unfound_rows,
                value_input_option="RAW",
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
        all_rows = g.sheets.read_values(spreadsheet_id, "Processed!A2:C")
        log.debug(f"all rows: {all_rows}")
        filenames = [row[0] for row in all_rows]
        log.debug(f"all filenames: {filenames}")
        if filename in filenames:
            log.debug(f"Found filename in processed: {filename}")
            row_index = filenames.index(filename) + 2  # account for header
            log.debug(f"Updating row {row_index} in Processed")
            g.sheets.write_values(
                spreadsheet_id,
                f"Processed!A{row_index}:C{row_index}",
                [updated_row],
                value_input_option="RAW",
            )
        else:
            g.sheets.append_values(
                spreadsheet_id, "Processed", [updated_row], value_input_option="RAW"
            )
            log.debug(f"Appended new row to Processed: {updated_row}")
        g.sheets.sort_sheet(
            spreadsheet_id, "Processed", column_index=2, ascending=False, start_row=2
        )
    except Exception as e:
        log.error(f"Failed to update Processed log: {e}")


def format(spreadsheet_id):
    formatting.apply_formatting_to_sheet(spreadsheet_id)
