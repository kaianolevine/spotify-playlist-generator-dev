from __future__ import annotations

import time
from datetime import datetime

import kaiano_common_utils.logger as log
import kaiano_common_utils.sheets_formatting as formatting
from googleapiclient.errors import HttpError
from kaiano_common_utils.api.google import GoogleAPI

log = log.get_logger()


class SpreadsheetLogger:
    def __init__(self, g: GoogleAPI, *, spreadsheet_id: str | None = None):
        self.g = g
        self.spreadsheet_id = spreadsheet_id

    def with_spreadsheet(self, spreadsheet_id: str) -> "SpreadsheetLogger":
        self.spreadsheet_id = spreadsheet_id
        return self

    def _require_spreadsheet_id(self) -> str:
        if not self.spreadsheet_id:
            raise ValueError("spreadsheet_id is required for this operation")
        return self.spreadsheet_id

    def delete_sheet_by_name(self, sheet_name: str) -> None:
        """Delete a sheet tab by its title."""
        spreadsheet_id = self._require_spreadsheet_id()
        meta = self.g.sheets.get_metadata(spreadsheet_id)
        for sheet in meta.get("sheets", []):
            props = sheet.get("properties", {})
            if props.get("title") == sheet_name:
                sheet_id = props.get("sheetId")
                self.g.sheets.batch_update(
                    spreadsheet_id, [{"deleteSheet": {"sheetId": sheet_id}}]
                )
                return

    def get_or_create_logging_spreadsheet(
        self, folder_id: str, spreadsheet_name: str
    ) -> str:
        """
        Locate the logging spreadsheet by name in the configured folder, or create it if missing.
        Ensures the required sheets exist.
        """
        files = self.g.drive.list_files(folder_id, trashed=False, include_folders=True)
        for f in files:
            if f.name == spreadsheet_name and (f.mime_type or "").startswith(
                "application/vnd.google-apps.spreadsheet"
            ):
                spreadsheet_id = f.id
                self.setup_logging_spreadsheet(spreadsheet_id)
                return spreadsheet_id

        spreadsheet_id = self.g.drive.create_spreadsheet_in_folder(
            spreadsheet_name, folder_id
        )

        if not self.wait_for_spreadsheet_ready(spreadsheet_id):
            log.error(
                "❌ Spreadsheet did not become ready in time, continuing anyway..."
            )

        self.setup_logging_spreadsheet(spreadsheet_id)
        log.info(
            f"Created new logging spreadsheet '{spreadsheet_name}' in folder {folder_id}."
        )
        return spreadsheet_id

    def wait_for_spreadsheet_ready(
        self, spreadsheet_id: str, retries: int = 5, delay: int = 1
    ) -> bool:
        """Poll until the spreadsheet metadata can be fetched."""
        for attempt in range(1, retries + 1):
            try:
                self.g.sheets.get_metadata(spreadsheet_id)
                return True
            except Exception:
                log.warning(
                    f"Waiting for spreadsheet to propagate ({attempt}/{retries})..."
                )
                time.sleep(delay)
        return False

    def setup_logging_spreadsheet(self, spreadsheet_id: str) -> None:
        """
        Ensure the logging spreadsheet contains only the required sheets with correct headers.
        """
        required_sheets = {
            "Info": ["Timestamp", "Message", "Processed", "Found", "Unfound"],
            "Processed": ["Filename", "Playlist ID", "ExtVDJLine"],
            "Songs Added": ["Date", "Title", "Artist"],
            "Songs Not Found": ["Date", "Title", "Artist"],
        }
        for sheet_name, headers in required_sheets.items():
            self.g.sheets.ensure_sheet_exists(
                spreadsheet_id, sheet_name, headers=headers
            )
        try:
            metadata = self.g.sheets.get_metadata(spreadsheet_id)
            for sheet_info in metadata.get("sheets", []):
                title = sheet_info.get("properties", {}).get("title", "")
                if title not in required_sheets:
                    self.delete_sheet_by_name(title)
                    log.info(f"🗑 Deleted extraneous sheet '{title}'.")
        except HttpError as e:
            log.error(f"⚠️ Failed to clean up sheets: {e}")

    def _append_values(
        self,
        range_name: str,
        values: list[list[str]],
        value_input_option: str = "RAW",
    ) -> None:
        spreadsheet_id = self._require_spreadsheet_id()
        try:
            self.g.sheets.append_values(
                spreadsheet_id,
                range_name,
                values,
                value_input_option=value_input_option,
            )
        except Exception as e:
            log.error(f"⚠️ Failed to append values to {range_name}: {e}")

    def log_spreadsheet(
        self,
        *,
        info_message: str | None = None,
        processed_summary: str | None = None,
        found_summary: str | None = None,
        unfound_summary: str | None = None,
        songs_added: list[list[str]] | None = None,
        songs_not_found: list[list[str]] | None = None,
        processed_update: dict | None = None,
    ) -> None:
        """Unified spreadsheet logger."""
        spreadsheet_id = self._require_spreadsheet_id()
        timestamp = datetime.now().replace(microsecond=0).isoformat(sep=" ")

        # --- Info tab ---
        if info_message is not None:
            self.g.sheets.ensure_sheet_exists(
                spreadsheet_id,
                "Info",
                headers=["Timestamp", "Message", "Processed", "Found", "Unfound"],
            )
            row = [
                [
                    timestamp,
                    info_message,
                    processed_summary or "",
                    found_summary or "",
                    unfound_summary or "",
                ]
            ]
            self._append_values("Info!A1", row, value_input_option="RAW")

        # --- Songs Added ---
        if songs_added:
            self._append_values(
                "Songs Added!A1",
                songs_added,
                value_input_option="RAW",
            )

        # --- Songs Not Found ---
        if songs_not_found:
            self._append_values(
                "Songs Not Found!A1",
                songs_not_found,
                value_input_option="RAW",
            )

        # --- Processed ---
        if processed_update:
            filename = processed_update.get("filename")
            extvdj_line = processed_update.get("extvdj_line")
            playlist_id = processed_update.get("playlist_id")

            if not filename or not extvdj_line:
                log.error(
                    "⚠️ processed_update missing required keys: filename/extvdj_line"
                )
                return

            all_rows = self.g.sheets.read_values(spreadsheet_id, "Processed!A2:C")
            filenames = [row[0] for row in all_rows if row]

            updated_row = [filename, playlist_id or "", extvdj_line]

            if filename in filenames:
                row_index = filenames.index(filename) + 2
                self.g.sheets.write_values(
                    spreadsheet_id,
                    f"Processed!A{row_index}:C{row_index}",
                    [updated_row],
                    value_input_option="RAW",
                )
            else:
                self._append_values(
                    "Processed!A1",
                    [updated_row],
                    value_input_option="RAW",
                )
            try:
                self.g.sheets.sort_sheet(
                    spreadsheet_id,
                    "Processed",
                    column_index=2,
                    ascending=False,
                    start_row=2,
                )
            except Exception as e:
                log.error(f"⚠️ Failed to sort Processed sheet: {e}")

    def log_info_sheet(
        self,
        message: str = None,
        processed: str = None,
        found: str = None,
        unfound: str = None,
    ):
        if all(v is not None for v in [message, processed, found, unfound]):
            self.log_spreadsheet(
                info_message=message,
                processed_summary=processed,
                found_summary=found,
                unfound_summary=unfound,
            )
            return
        if message:
            self.log_spreadsheet(info_message=message)
            return
        log.warning("⚠️ No message or data provided to log_info_sheet.")

    def log_start(self) -> None:
        self.log_spreadsheet(info_message="🔄 Starting Radio Sync...")
        log.debug("Starting debug logging for Westie Radio sync.")

    def load_processed_map(self) -> dict:
        spreadsheet_id = self._require_spreadsheet_id()
        processed_rows = self.g.sheets.read_values(spreadsheet_id, "Processed!A2:C")
        processed_map = {row[0]: row[2] for row in processed_rows if len(row) >= 3}
        return processed_map

    def log_to_sheets(
        self,
        date,
        matched_songs,
        found_uris,
        unfound,
        filename,
        new_songs,
        last_extvdj_line,
        playlist_id=None,
    ):
        # whitespace_buffer is a module-level global
        global whitespace_buffer
        self.log_spreadsheet(
            info_message=f"🎶 Processed file: {filename}{whitespace_buffer}",
            processed_summary=f"Processed rows: {len(new_songs)}{whitespace_buffer}",
            found_summary=f"✅ Found tracks: {len(found_uris)}{whitespace_buffer}",
            unfound_summary=f"❌ Unfound tracks: {len(unfound)}{whitespace_buffer}",
            songs_added=(
                [[date, title, artist] for (artist, title) in matched_songs]
                if matched_songs
                else None
            ),
            songs_not_found=(
                [[date, title, artist] for artist, title, _ in unfound]
                if unfound
                else None
            ),
            processed_update={
                "filename": filename,
                "playlist_id": playlist_id,
                "extvdj_line": (new_songs[-1][2] if new_songs else last_extvdj_line),
            },
        )

    def format(self) -> None:
        spreadsheet_id = self._require_spreadsheet_id()
        formatting.apply_formatting_to_sheet(spreadsheet_id)


whitespace_buffer = ""

# --- Module-level wrappers for backward compatibility ---


def delete_sheet_by_name(g: GoogleAPI, spreadsheet_id: str, sheet_name: str) -> None:
    return SpreadsheetLogger(g, spreadsheet_id=spreadsheet_id).delete_sheet_by_name(
        sheet_name
    )


def get_or_create_logging_spreadsheet(
    g: GoogleAPI, folder_id: str, spreadsheet_name: str
) -> str:
    return SpreadsheetLogger(g).get_or_create_logging_spreadsheet(
        folder_id, spreadsheet_name
    )


def wait_for_spreadsheet_ready(
    g: GoogleAPI, spreadsheet_id: str, retries: int = 5, delay: int = 1
) -> bool:
    return SpreadsheetLogger(g).wait_for_spreadsheet_ready(
        spreadsheet_id, retries=retries, delay=delay
    )


def setup_logging_spreadsheet(g: GoogleAPI, spreadsheet_id):
    return SpreadsheetLogger(g).setup_logging_spreadsheet(spreadsheet_id)


def log_spreadsheet(
    g: GoogleAPI,
    spreadsheet_id: str,
    *,
    info_message: str | None = None,
    processed_summary: str | None = None,
    found_summary: str | None = None,
    unfound_summary: str | None = None,
    songs_added: list[list[str]] | None = None,
    songs_not_found: list[list[str]] | None = None,
    processed_update: dict | None = None,
) -> None:
    return SpreadsheetLogger(g, spreadsheet_id=spreadsheet_id).log_spreadsheet(
        info_message=info_message,
        processed_summary=processed_summary,
        found_summary=found_summary,
        unfound_summary=unfound_summary,
        songs_added=songs_added,
        songs_not_found=songs_not_found,
        processed_update=processed_update,
    )


def log_info_sheet(
    g: GoogleAPI,
    spreadsheet_id: str,
    message: str = None,
    processed: str = None,
    found: str = None,
    unfound: str = None,
):
    return SpreadsheetLogger(g, spreadsheet_id=spreadsheet_id).log_info_sheet(
        message=message, processed=processed, found=found, unfound=unfound
    )


def log_start(g: GoogleAPI, spreadsheet_id):
    return SpreadsheetLogger(g, spreadsheet_id=spreadsheet_id).log_start()


def load_processed_map(g: GoogleAPI, spreadsheet_id):
    return SpreadsheetLogger(g, spreadsheet_id=spreadsheet_id).load_processed_map()


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
    return SpreadsheetLogger(g, spreadsheet_id=spreadsheet_id).log_to_sheets(
        date,
        matched_songs,
        found_uris,
        unfound,
        filename,
        new_songs,
        last_extvdj_line,
        playlist_id=playlist_id,
    )


def format(spreadsheet_id):
    formatting.apply_formatting_to_sheet(spreadsheet_id)
