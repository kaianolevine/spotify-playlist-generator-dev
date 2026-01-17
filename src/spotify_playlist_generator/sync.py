from __future__ import annotations

import os
import tempfile

import kaiano_common_utils.logger as log
from dotenv import load_dotenv
from kaiano_common_utils.api.google import GoogleAPI
from kaiano_common_utils.api.spotify.spotify import SpotifyAPI
from kaiano_common_utils.library.vdj.m3u.api import M3UToolbox

from spotify_playlist_generator import config
from spotify_playlist_generator.sheet_logging import SpreadsheetLogger

log = log.get_logger()


DEFAULT_PLAYLIST_DESCRIPTION = (
    "Generated automatically by Deejay Marvel Automation Tools. "
    "Spreadsheets of history and song-not-found logs can be found at "
    "www.kaianolevine.com/dj-marvel"
)


def extract_date_from_filename(filename: str) -> str:
    """Extract YYYY-MM-DD prefix from a filename if present."""
    base = os.path.basename(filename)
    if len(base) >= 10 and base[4] == "-" and base[7] == "-":
        return base[:10]
    return base


def process_new_songs(
    songs: list[tuple[str, str, str]],
    last_extvdj_line: str | None,
) -> list[tuple[str, str, str]]:
    """Return only songs appearing after the last processed EXTVDJ line."""
    if not songs:
        return []
    if not last_extvdj_line:
        return songs

    try:
        idx = next(
            i
            for i, (_artist, _title, line) in enumerate(songs)
            if line == last_extvdj_line
        )
        return songs[idx + 1 :]
    except StopIteration:
        return songs


def update_spotify_radio_playlist(
    sp: SpotifyAPI, playlist_id: str, found_uris: list[str]
) -> None:
    """Append tracks to the main radio playlist and trim."""
    if not found_uris:
        return

    try:
        sp.add_tracks_to_specific_playlist(playlist_id, found_uris)
        sp.trim_playlist_to_limit()
    except Exception as e:
        log.error(f"❌ Error updating Spotify radio playlist: {e}", exc_info=True)


def create_spotify_playlist_for_file(
    sp: SpotifyAPI, date_str: str, found_uris: list[str]
) -> str | None:
    """Create or update a per-day Spotify playlist."""
    if not found_uris:
        return None

    playlist_name = f"{date_str} History Set"

    try:
        existing = sp.find_playlist_by_name(playlist_name)
        if existing:
            playlist_id = existing["id"]
            sp.add_tracks_to_specific_playlist(playlist_id, found_uris)
            return playlist_id

        playlist_id = sp.create_playlist(playlist_name, DEFAULT_PLAYLIST_DESCRIPTION)
        if not playlist_id:
            return None

        unique_uris = list(dict.fromkeys(found_uris))
        sp.add_tracks_to_specific_playlist(playlist_id, unique_uris)
        return playlist_id

    except Exception as e:
        log.error(
            f"❌ Failed creating/updating playlist '{playlist_name}': {e}",
            exc_info=True,
        )
        return None


def process_file(
    file: dict,
    processed_map: dict[str, str],
    g: GoogleAPI,
    m3u_tool: M3UToolbox,
    sp: SpotifyAPI,
    logger: SpreadsheetLogger,
) -> None:
    filename = file["name"]
    file_id = file["id"]
    date = extract_date_from_filename(filename)

    temp_path = os.path.join(tempfile.gettempdir(), f"{file_id}_{filename}")

    try:
        g.drive.download_file(file_id, temp_path)

        # parse_m3u returns (artist, title, extvdj_line)
        songs = m3u_tool.parse.parse_m3u(None, temp_path, logger.spreadsheet_id)

        last_extvdj_line = processed_map.get(filename)
        new_songs = process_new_songs(songs, last_extvdj_line)

        if not new_songs:
            return

        found_uris: list[str] = []
        matched_songs: list[tuple[str, str]] = []
        unfound: list[tuple[str, str, str]] = []

        for artist, title, extvdj_line in new_songs:
            uri = sp.search_track(artist, title)
            if uri:
                found_uris.append(uri)
                matched_songs.append((artist, title))
            else:
                unfound.append((artist, title, extvdj_line))

        update_spotify_radio_playlist(sp, config.SPOTIFY_PLAYLIST_ID, found_uris)
        playlist_id = create_spotify_playlist_for_file(sp, date, found_uris)

        logger.log_to_sheets(
            date,
            matched_songs,
            found_uris,
            unfound,
            filename,
            new_songs,
            last_extvdj_line,
            playlist_id=playlist_id,
        )

        processed_map[filename] = new_songs[-1][2]

    finally:
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass


def main() -> None:
    load_dotenv()

    g = GoogleAPI.from_env()
    sp = SpotifyAPI.from_env()
    m3u_tool = M3UToolbox()

    logger = SpreadsheetLogger(
        g,
        folder_id=config.HISTORY_TO_SPOTIFY_FOLDER_ID,
        spreadsheet_name=config.HISTORY_TO_SPOTIFY_SPREADSHEET_NAME,
    )
    logger.log_start()

    m3u_files = g.drive.get_all_m3u_files()
    if not m3u_files:
        logger.log_info_sheet("❌ No .m3u files found.")
        return

    processed_map = logger.load_processed_map()

    for file in m3u_files:
        process_file(file, processed_map, g, m3u_tool, sp, logger)

    logger.format()
    logger.log_info_sheet("✅ Sync complete.")


if __name__ == "__main__":
    main()
