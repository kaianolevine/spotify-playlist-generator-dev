"""
sync.py — Main integration script for Westie Radio automation.
"""

import os

import config
import kaiano_common_utils.logger as log
from dotenv import load_dotenv
from kaiano_common_utils.api.google import GoogleAPI
from kaiano_common_utils.api.spotify.spotify import SpotifyAPI
from kaiano_common_utils.library.vdj.m3u.api import M3UToolbox
from sheet_logging import (
    format,
    get_or_create_logging_spreadsheet,
    load_processed_map,
    log_info_sheet,
    log_start,
    log_to_sheets,
)

log = log.get_logger()


def extract_date_from_filename(filename: str) -> str:
    """Extract YYYY-MM-DD prefix from a filename; fall back to filename if missing."""
    base = os.path.basename(filename)
    if len(base) >= 10 and base[4] == "-" and base[7] == "-":
        return base[:10]
    return base


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


def update_spotify_radio_playlist(sp: SpotifyAPI, playlist_id, found_uris):
    try:
        sp.add_tracks_to_specific_playlist(playlist_id, found_uris)
        sp.trim_playlist_to_limit()
    except Exception as e:
        log.error(f"Error updating Spotify playlist: {e}")


def create_spotify_playlist_for_file(
    sp: SpotifyAPI, date_str: str, found_uris: list[str]
) -> str:
    if not found_uris:
        log.warning("No URIs provided; skipping playlist creation.")
        return None

    playlist_name = f"{date_str} History Set"
    log.debug(f"🎵 Preparing to create/update Spotify playlist: {playlist_name}")
    try:
        # Check for existing playlist
        existing_playlist = sp.find_playlist_by_name(playlist_name)
        if existing_playlist:
            playlist_id = existing_playlist["id"]
            log.info(
                f"📝 Playlist '{playlist_name}' already exists, updating existing playlist (ID: {playlist_id})."
            )

            if found_uris:
                sp.add_tracks_to_specific_playlist(playlist_id, found_uris)
                log.debug(
                    f"✅ Added {len(found_uris)} new tracks (minus duplicates) to existing playlist {playlist_name} (ID: {playlist_id})."
                )
            else:
                log.debug(
                    f"ℹ️ No new tracks to add to existing playlist {playlist_name}."
                )
            return playlist_id
        else:
            # Create new playlist as before
            playlist_id = sp.create_playlist(playlist_name)
            if not playlist_id:
                log.error(f"❌ Failed to create playlist: {playlist_name}")
                return None
            unique_uris = list(dict.fromkeys(found_uris))
            duplicates_count = len(found_uris) - len(unique_uris)
            log.debug(f"🔍 Removing duplicates: {duplicates_count} duplicates removed.")
            sp.add_tracks_to_specific_playlist(playlist_id, unique_uris)
            log.debug(
                f"✅ Created playlist {playlist_name} with ID {playlist_id} containing {len(unique_uris)} tracks."
            )
            return playlist_id
    except Exception as e:
        log.error(
            f"❌ Exception while creating/updating Spotify playlist {playlist_name}: {e}"
        )
        return None


def process_file(
    file,
    processed_map,
    g: GoogleAPI,
    spreadsheet_id,
    m3u_tool: M3UToolbox,
    sp: SpotifyAPI,
):
    filename = file["name"]
    file_id = file["id"]
    date = extract_date_from_filename(filename)

    try:
        g.drive.download_file(file_id, filename)
        songs = m3u_tool.parse.parse_m3u(None, filename, spreadsheet_id)

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
            uri = sp.search_track(artist, title)
            log.debug(
                f"Searching for track - Artist: {artist}, Title: {title}, Found URI: {uri}"
            )
            if uri:
                found_uris.append(uri)
                matched_songs.append((artist, title))
                matched_extvdj_lines.append(extvdj_line)
            else:
                unfound.append((artist, title, extvdj_line))

        update_spotify_radio_playlist(sp, config.SPOTIFY_PLAYLIST_ID, found_uris)

        playlist_id = create_spotify_playlist_for_file(sp, date, found_uris)
        if playlist_id:
            log.info(f"✅ Playlist created successfully with ID: {playlist_id}")

        log_to_sheets(
            g,
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

    g = GoogleAPI.from_env()
    sp = SpotifyAPI.from_env()

    spreadsheet_id = get_or_create_logging_spreadsheet(
        g,
        config.HISTORY_TO_SPOTIFY_FOLDER_ID,
        config.HISTORY_TO_SPOTIFY_SPREADSHEET_NAME,
    )
    log_start(g, spreadsheet_id)

    folder_id = config.VDJ_HISTORY_FOLDER_ID
    if not folder_id:
        log.critical("Missing environment variable: VDJ_HISTORY_FOLDER_ID")
        raise ValueError("Missing environment variable: VDJ_HISTORY_FOLDER_ID")
    log.info(f"📁 Loaded VDJ_HISTORY_FOLDER_ID: {folder_id}")

    m3u_files = g.drive.get_all_m3u_files()

    if not m3u_files:
        log_info_sheet(g, spreadsheet_id, "❌ No .m3u files found.")
        return

    processed_map = load_processed_map(g, spreadsheet_id)
    m3u_tool = M3UToolbox()

    for file in m3u_files:
        process_file(file, processed_map, g, spreadsheet_id, m3u_tool, sp)

    format(spreadsheet_id)
    log_info_sheet(g, spreadsheet_id, "✅ Sync complete.")
    log.info("✅ Sync complete.")


if __name__ == "__main__":
    main()
