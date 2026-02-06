from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Any

import kaiano.logger as log
from dotenv import load_dotenv
from kaiano.google import GoogleAPI
from kaiano.spotify import SpotifyAPI
from kaiano.vdj.m3u.m3u import M3UToolbox

from spotify_playlist_generator import config
from spotify_playlist_generator.sheet_logging import SpreadsheetLogger

log = log.get_logger()


DEFAULT_PLAYLIST_DESCRIPTION = (
    "Generated automatically by Deejay Marvel Automation Tools. "
    "Spreadsheets of history and song-not-found logs can be found at "
    "www.kaianolevine.com/dj-marvel"
)


# ---------------------------
# Playlist snapshot utilities
# ---------------------------


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _first_attr(obj: Any, names: list[str]) -> Any:
    """Return the first existing attribute value on obj from a list of names."""
    for n in names:
        if hasattr(obj, n):
            return getattr(obj, n)
    return None


def _call_first(sp: Any, method_names: list[str], *args: Any, **kwargs: Any) -> Any:
    """Call the first method that exists on sp; return its result."""
    for name in method_names:
        fn = getattr(sp, name, None)
        if callable(fn):
            return fn(*args, **kwargs)
    raise AttributeError(f"None of these methods exist on SpotifyAPI: {method_names}")


def _extract_external_url(playlist: dict) -> str:
    # Spotify returns external_urls: {spotify: 'https://open.spotify.com/playlist/...'}
    external = playlist.get("external_urls") or {}
    if isinstance(external, dict):
        return external.get("spotify", "") or ""
    return ""


def _normalize_playlist_item(p: dict) -> dict:
    """Normalize a Spotify playlist object into a stable JSON-friendly dict."""
    owner = p.get("owner") or {}
    tracks = p.get("tracks") or {}

    return {
        "id": p.get("id", ""),
        "name": p.get("name", ""),
        "url": _extract_external_url(p),
        "uri": p.get("uri", ""),
        "type": p.get("type", "playlist"),
        "public": p.get("public"),
        "collaborative": p.get("collaborative"),
        "snapshot_id": p.get("snapshot_id", ""),
        "tracks_total": tracks.get("total"),
        "owner": {
            "id": owner.get("id", ""),
            "display_name": owner.get("display_name", ""),
        },
    }


def fetch_all_playlists(sp: Any) -> list[dict]:
    """Fetch all playlists visible to the account.

    This is intentionally defensive because SpotifyAPI wrappers differ.
    We try a handful of common method names; if none exist, we log and return [].

    Expected return shape is a list of raw Spotify playlist dicts.
    """
    # 1) If your wrapper already has a direct helper, prefer it.
    try:
        return _call_first(
            sp,
            [
                "get_all_playlists",
                "get_user_playlists",
                "list_playlists",
                "get_playlists",
                "fetch_playlists",
            ],
        )
    except Exception:
        pass

    # 2) Try to find an underlying spotipy-like client.
    client = _first_attr(sp, ["client", "spotify", "sp", "_client", "_sp"])
    if client is None:
        return []

    # spotipy style: current_user_playlists(limit=50, offset=0) -> {items: [...], next: ...}
    fn = getattr(client, "current_user_playlists", None)
    if not callable(fn):
        return []

    items: list[dict] = []
    limit = 50
    offset = 0

    while True:
        page = fn(limit=limit, offset=offset)
        if not isinstance(page, dict):
            break

        page_items = page.get("items") or []
        if isinstance(page_items, list):
            items.extend(page_items)

        # Prefer Spotify pagination semantics when available.
        if page.get("next"):
            offset += limit
            continue

        break

    return items


def write_playlist_snapshot_json(sp: Any) -> str | None:
    """Write a JSON snapshot of all playlists to disk and return the output path."""
    # Determine output path.
    json_output_path = (
        os.getenv("SPOTIFY_PLAYLIST_SNAPSHOT_JSON_PATH")
        or getattr(config, "SPOTIFY_PLAYLIST_SNAPSHOT_JSON_PATH", None)
        or "site_data/spotify_playlists.json"
    )

    raw_playlists = fetch_all_playlists(sp)
    normalized = [
        _normalize_playlist_item(p) for p in raw_playlists if isinstance(p, dict)
    ]

    snapshot = {
        "generated_at": _now_utc_iso(),
        "playlist_count": len(normalized),
        "playlists": sorted(
            normalized, key=lambda x: (x.get("name", "") or "").lower()
        ),
    }

    try:
        os.makedirs(os.path.dirname(json_output_path) or ".", exist_ok=True)
        with open(json_output_path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2)
        return json_output_path
    except Exception:
        log.exception(
            f"❌ Failed to write playlist snapshot JSON to: {json_output_path}"
        )
        return None


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
            log.info(f"⏭ No new songs found in {filename}")
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

        log.info(
            f"🔎 {filename}: {len(matched_songs)} found on Spotify, {len(unfound)} not found"
        )

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
    log.info("✅ Google API initialized")

    sp = SpotifyAPI.from_env()
    log.info("✅ Spotify API initialized")

    # Build a JSON snapshot of all playlists visible to this Spotify account.
    # This is useful for rendering playlist lists on the website, similar to DJ set collection snapshots.
    snapshot_path = write_playlist_snapshot_json(sp)
    if snapshot_path:
        log.info(f"🧾 Wrote Spotify playlist snapshot JSON to: {snapshot_path}")
    else:
        log.warning(
            "⚠️ Spotify playlist snapshot JSON was not written (see logs above)."
        )

    m3u_tool = M3UToolbox()
    log.info("✅ M3U toolbox initialized")

    logger = SpreadsheetLogger(
        g,
        folder_id=config.HISTORY_TO_SPOTIFY_FOLDER_ID,
        spreadsheet_name=config.HISTORY_TO_SPOTIFY_SPREADSHEET_NAME,
    )
    log.info(f"📄 Logging spreadsheet ready (ID: {logger.spreadsheet_id})")
    logger.log_start()

    m3u_files = g.drive.get_all_m3u_files()
    log.info(f"🎶 Found {len(m3u_files)} .m3u files to process")
    if not m3u_files:
        logger.log_info_sheet("❌ No .m3u files found.")
        return

    processed_map = logger.load_processed_map()

    for file in m3u_files:
        log.info(f"➡️ Processing M3U file: {file.get('name')}")
        process_file(file, processed_map, g, m3u_tool, sp, logger)

    logger.format()
    log.info("🏁 Spotify history sync complete")
    logger.log_info_sheet("✅ Sync complete.")


if __name__ == "__main__":
    main()
