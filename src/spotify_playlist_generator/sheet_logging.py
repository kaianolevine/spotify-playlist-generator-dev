from __future__ import annotations

import time
from datetime import datetime

from googleapiclient.errors import HttpError
from kaiano import logger as logger_mod
from kaiano.google import GoogleAPI

log = logger_mod.get_logger()


class SpreadsheetLogger:
    def __init__(
        self,
        g: GoogleAPI,
        *,
        folder_id: str,
        spreadsheet_name: str,
    ):
        self.g = g
        self.folder_id = folder_id
        self.spreadsheet_name = spreadsheet_name

        self.spreadsheet_id = self._get_logging_spreadsheet()

    def delete_sheet_by_name(self, sheet_name: str) -> None:
        """Delete a sheet tab by its title."""
        meta = self.g.sheets.get_metadata(self.spreadsheet_id)
        for sheet in meta.get("sheets", []):
            props = sheet.get("properties", {})
            if props.get("title") == sheet_name:
                sheet_id = props.get("sheetId")
                self.g.sheets.batch_update(
                    self.spreadsheet_id, [{"deleteSheet": {"sheetId": sheet_id}}]
                )
                return

    def _get_logging_spreadsheet(self) -> str:
        """
        Locate the logging spreadsheet by name in the configured folder, or create it if missing.
        Ensures the required sheets exist.
        """
        files = self.g.drive.list_files(
            self.folder_id, trashed=False, include_folders=True
        )
        for f in files:
            if f.name == self.spreadsheet_name and (f.mime_type or "").startswith(
                "application/vnd.google-apps.spreadsheet"
            ):
                spreadsheet_id = f.id
                self.spreadsheet_id = spreadsheet_id
                self._setup_logging_spreadsheet()
                return spreadsheet_id

        spreadsheet_id = self.g.drive.create_spreadsheet_in_folder(
            self.spreadsheet_name, self.folder_id
        )
        self.spreadsheet_id = spreadsheet_id

        if not self._wait_for_spreadsheet_ready(spreadsheet_id):
            log.error(
                "❌ Spreadsheet did not become ready in time, continuing anyway..."
            )

        self._setup_logging_spreadsheet()
        log.info(
            f"Created new logging spreadsheet '{self.spreadsheet_name}' in folder {self.folder_id}."
        )
        return spreadsheet_id

    def _wait_for_spreadsheet_ready(
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

    def _setup_logging_spreadsheet(self) -> None:
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
                self.spreadsheet_id, sheet_name, headers=headers
            )
        try:
            metadata = self.g.sheets.get_metadata(self.spreadsheet_id)
            for sheet_info in metadata.get("sheets", []):
                props = sheet_info.get("properties", {})
                title = props.get("title", "")
                sheet_id = props.get("sheetId")

                if title not in required_sheets and sheet_id is not None:
                    self.g.sheets.batch_update(
                        self.spreadsheet_id,
                        [{"deleteSheet": {"sheetId": sheet_id}}],
                    )
                    log.info(f"🗑 Deleted extraneous sheet '{title}'.")
        except HttpError as e:
            log.error(f"⚠️ Failed to clean up sheets: {e}")

    def _append_values(
        self,
        range_name: str,
        values: list[list[str]],
        value_input_option: str = "RAW",
    ) -> None:
        try:
            self.g.sheets.append_values(
                self.spreadsheet_id,
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
        timestamp = datetime.now().replace(microsecond=0).isoformat(sep=" ")

        # --- Info tab ---
        if info_message is not None:
            self.g.sheets.ensure_sheet_exists(
                self.spreadsheet_id,
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

            all_rows = self.g.sheets.read_values(self.spreadsheet_id, "Processed!A2:C")
            filenames = [row[0] for row in all_rows if row]

            updated_row = [filename, playlist_id or "", extvdj_line]

            if filename in filenames:
                row_index = filenames.index(filename) + 2
                self.g.sheets.write_values(
                    self.spreadsheet_id,
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
                    self.spreadsheet_id,
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
        processed_rows = self.g.sheets.read_values(
            self.spreadsheet_id, "Processed!A2:C"
        )
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

        self.log_spreadsheet(
            info_message=f"🎶 Processed file: {filename}",
            processed_summary=f"Processed rows: {len(new_songs)}",
            found_summary=f"✅ Found tracks: {len(found_uris)}",
            unfound_summary=f"❌ Unfound tracks: {len(unfound)}",
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
        self.g.sheets.formatter.apply_formatting_to_sheet(self.spreadsheet_id)
