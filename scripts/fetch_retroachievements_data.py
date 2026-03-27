#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
ALLOWLIST_PATH = RAW_DIR / "thegamesdb-system-allowlist.json"
OUTPUT_PATH = RAW_DIR / "retroachievements-catalog.json"
DETAIL_CACHE_PATH = RAW_DIR / "retroachievements-game-detail-cache.json"
API_URL_BASE = "https://retroachievements.org/API"
MEDIA_URL_BASE = "https://retroachievements.org"

SYSTEM_MATCH_ALIASES = {
    "dreamcast": ["dreamcast", "sega dreamcast"],
    "gamecube": ["gamecube", "nintendo gamecube"],
    "gb": ["game boy", "gb"],
    "gba": ["game boy advance", "gba"],
    "gbc": ["game boy color", "gbc"],
    "mastersystem": ["master system", "sega master system", "sms"],
    "megadrive": [
        "mega drive",
        "genesis",
        "genesis mega drive",
        "genesis/mega drive",
        "sega genesis/mega drive",
        "sega mega drive",
    ],
    "n64": ["nintendo 64", "n64"],
    "nes": ["nintendo entertainment system", "nes", "famicom", "nes/famicom", "nes famicom"],
    "psp": ["playstation portable", "psp"],
    "psx": ["sony playstation", "playstation", "ps1", "psx"],
    "saturn": ["saturn", "sega saturn"],
    "snes": [
        "super nintendo entertainment system",
        "super nintendo",
        "snes",
        "super famicom",
        "snes/super famicom",
        "snes super famicom",
    ],
}


def load_json(path: Path, default: object | None = None) -> object:
    if not path.exists():
        if default is not None:
            return default
        raise FileNotFoundError(f"Missing required file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def text(value: object) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def integer(value: object) -> int | None:
    cleaned = text(value)
    if cleaned is None:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def normalize_title(title: str) -> str:
    return re.sub(r"^[Tt]he\s+", "", title.strip())


def parse_release_year(raw_value: str | None) -> int | None:
    if not raw_value:
        return None
    match = re.match(r"(\d{4})", raw_value)
    if not match:
        return None
    year = integer(match.group(1))
    if year == 1970:
        return None
    return year


def normalize_alias(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def split_genres(raw_value: str | None) -> list[str]:
    cleaned = text(raw_value)
    if not cleaned:
        return []
    parts = re.split(r"\s*[,/|;]\s*", cleaned)
    result = []
    seen: set[str] = set()
    for part in parts:
        name = text(part)
        if not name or name in seen:
            continue
        seen.add(name)
        result.append(name)
    return result


def resolve_ra_media_url(raw_path: object) -> str | None:
    cleaned = text(raw_path)
    if not cleaned:
        return None
    if cleaned.startswith(("http://", "https://")):
        return cleaned
    return urllib.parse.urljoin(MEDIA_URL_BASE, cleaned)


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def load_allowlist(path: Path) -> list[dict]:
    payload = load_json(path)
    if isinstance(payload, dict):
        systems = payload.get("systems")
        if isinstance(systems, list):
            return [item for item in systems if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    raise SystemExit(f"Expected a list or object-with-systems in {path}")


def load_existing_catalog(path: Path) -> dict | None:
    if not path.exists():
        return None
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return None
    if not isinstance(payload.get("systems"), list) or not isinstance(payload.get("games"), list):
        return None
    return payload


def merge_catalog(existing_catalog: dict | None, systems: list[dict], games: list[dict]) -> dict:
    existing_systems = existing_catalog.get("systems", []) if isinstance(existing_catalog, dict) else []
    existing_games = existing_catalog.get("games", []) if isinstance(existing_catalog, dict) else []

    incoming_system_ids = {
        system.get("id")
        for system in systems
        if isinstance(system, dict) and system.get("id") is not None
    }

    merged_systems = [
        system
        for system in existing_systems
        if isinstance(system, dict) and system.get("id") not in incoming_system_ids
    ]
    merged_systems.extend(systems)
    merged_systems.sort(key=lambda item: (text(item.get("name")) or "", integer(item.get("id")) or 0))

    merged_games = [
        game
        for game in existing_games
        if isinstance(game, dict) and game.get("systemId") not in incoming_system_ids
    ]
    merged_games.extend(games)
    merged_games.sort(
        key=lambda item: (
            text(item.get("systemName")) or "",
            text(item.get("sortTitle")) or text(item.get("title")) or "",
            integer(item.get("id")) or 0,
        )
    )

    return {"systems": merged_systems, "games": merged_games}


def load_detail_cache(path: Path) -> dict[str, dict]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return {}
    return {str(key): value for key, value in payload.items() if isinstance(value, dict)}


def save_detail_cache(path: Path, cache: dict[str, dict]) -> None:
    serializable = {key: cache[key] for key in sorted(cache.keys(), key=lambda item: int(item))}
    path.write_text(json.dumps(serializable, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def build_output_payload(
    merged: dict,
    detail_requests_made: int,
    fetched_system_keys: list[str],
) -> dict:
    return {
        "metadata": {
            "importedAt": datetime.now(timezone.utc).isoformat(),
            "catalogSource": "retroachievements",
            "provider": "retroachievements",
            "systemCount": len(merged["systems"]),
            "gameCount": len(merged["games"]),
            "systems": [system["key"] for system in merged["systems"]],
            "detailRequestsMade": detail_requests_made,
            "fetchedSystemsThisRun": fetched_system_keys,
            "notes": [
                "This catalog is curated from RetroAchievements using the local atlas allowlist.",
                "Per-game summaries are cached locally so repeated refreshes only fetch missing metadata.",
                "RetroAchievements does not expose the same region and country fields as TheGamesDB, so atlas geography will be sparse unless another metadata layer is added later.",
            ],
        },
        "systems": merged["systems"],
        "games": merged["games"],
    }


def write_catalog_snapshot(
    output_path: Path,
    existing_catalog: dict | None,
    systems: list[dict],
    games: list[dict],
    detail_requests_made: int,
    fetched_system_keys: list[str],
) -> dict:
    merged = merge_catalog(existing_catalog, systems, games)
    output = build_output_payload(merged, detail_requests_made, fetched_system_keys)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    return output


def fetch_json(endpoint: str, **params: str | int) -> object:
    url = f"{API_URL_BASE}/{endpoint}?{urllib.parse.urlencode({key: str(value) for key, value in params.items() if value is not None})}"
    request = urllib.request.Request(url, headers={"User-Agent": "videogame-atlas"})
    with urllib.request.urlopen(request, timeout=90) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_all_systems(api_key: str) -> list[dict]:
    payload = fetch_json("API_GetConsoleIDs.php", y=api_key, a=1, g=1)
    if not isinstance(payload, list):
        raise SystemExit("RetroAchievements systems response was not a list")
    return [item for item in payload if isinstance(item, dict)]


def fetch_game_list(api_key: str, console_id: int, page_size: int, delay_seconds: float) -> list[dict]:
    records: list[dict] = []
    offset = 0
    while True:
        payload = fetch_json(
            "API_GetGameList.php",
            y=api_key,
            i=console_id,
            h=0,
            f=0,
            o=offset,
            c=page_size,
        )
        if not isinstance(payload, list):
            raise SystemExit(f"RetroAchievements game list response for console {console_id} was not a list")
        page_rows = [item for item in payload if isinstance(item, dict)]
        if not page_rows:
            break
        records.extend(page_rows)
        if len(page_rows) < page_size:
            break
        offset += page_size
        if delay_seconds > 0:
            time.sleep(delay_seconds)
    return records


def fetch_game_summary(api_key: str, game_id: int) -> dict:
    payload = fetch_json("API_GetGame.php", y=api_key, i=game_id)
    if not isinstance(payload, dict):
        raise SystemExit(f"RetroAchievements game summary response for game {game_id} was not an object")
    return payload


def build_alias_candidates(entry: dict) -> list[str]:
    candidates = []
    for value in (
        entry.get("key"),
        entry.get("name"),
        entry.get("shortName"),
        *(SYSTEM_MATCH_ALIASES.get(text(entry.get("key")) or "", [])),
    ):
        cleaned = text(value)
        if cleaned:
            candidates.append(normalize_alias(cleaned))
    deduped = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)
    return deduped


def match_system(entry: dict, systems_by_alias: dict[str, dict]) -> dict | None:
    for alias in build_alias_candidates(entry):
        if alias in systems_by_alias:
            return systems_by_alias[alias]
    return None


def build_system_record(entry: dict, retro_system: dict) -> dict:
    system_id = integer(retro_system.get("ID") or retro_system.get("id"))
    name = text(entry.get("name")) or text(retro_system.get("Name")) or f"System {system_id}"
    return {
        "id": system_id,
        "key": text(entry.get("key")) or str(system_id),
        "name": name,
        "shortName": text(entry.get("shortName")) or name,
        "manufacturer": text(entry.get("manufacturer")) or "Unknown",
        "category": text(entry.get("category")) or "Unknown",
        "generation": text(entry.get("generation")) or "Unknown",
        "releaseYear": integer(entry.get("releaseYear")),
        "endYear": integer(entry.get("endYear")),
        "summary": text(entry.get("summary")),
        "logo": {
            "url": resolve_ra_media_url(retro_system.get("IconURL") or retro_system.get("iconUrl")),
            "provider": "retroachievements",
            "kind": "system-icon",
            "alt": f"System icon for {name}",
        },
        "sourceAttribution": {
            "baseProvider": "retroachievements",
            "metadataProvider": "retroachievements",
            "metadataRecordUrl": None,
            "providerIds": ["retroachievements"],
        },
        "wikiUrl": None,
    }


def build_game_record(row: dict, system: dict, detail: dict | None) -> dict | None:
    game_id = integer(row.get("ID") or row.get("id"))
    title = text(row.get("Title") or row.get("title"))
    if game_id is None or not title:
        return None

    summary = detail if isinstance(detail, dict) else {}
    box_art_url = resolve_ra_media_url(summary.get("ImageBoxArt") or summary.get("imageBoxArt"))
    icon_url = resolve_ra_media_url(summary.get("ImageIcon") or summary.get("imageIcon") or row.get("ImageIcon"))
    title_art_url = resolve_ra_media_url(summary.get("ImageTitle") or summary.get("imageTitle"))
    ingame_url = resolve_ra_media_url(summary.get("ImageIngame") or summary.get("imageIngame"))
    media_url = box_art_url or title_art_url or icon_url
    genres = split_genres(text(summary.get("Genre") or summary.get("genre")))
    released_raw = text(summary.get("Released") or summary.get("released"))

    return {
        "id": game_id,
        "slug": f"{system['key']}-{re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-') or 'game'}",
        "title": title,
        "sortTitle": normalize_title(title),
        "systemId": system["id"],
        "systemName": system["name"],
        "releaseYear": parse_release_year(released_raw),
        "developer": text(summary.get("Developer") or summary.get("developer")),
        "publisher": text(summary.get("Publisher") or summary.get("publisher")),
        "genres": genres,
        "summary": None,
        "media": {
            "boxFront": {
                "url": media_url,
                "provider": "retroachievements",
                "kind": "box-art" if box_art_url else "game-icon",
                "alt": f"Artwork for {title}",
            },
            "screenshot": {
                "url": ingame_url,
                "provider": "retroachievements",
                "kind": "ingame",
                "alt": f"In-game screenshot for {title}" if ingame_url else None,
            },
            "logo": {
                "url": title_art_url,
                "provider": "retroachievements",
                "kind": "title-art",
                "alt": f"Title art for {title}" if title_art_url else None,
            },
            "video": {"url": None, "provider": "retroachievements", "kind": None, "alt": None},
        },
        "image": {
            "iconUrl": media_url,
            "alt": f"Artwork for {title}",
        },
        "hashes": [],
        "forumTopicId": integer(row.get("ForumTopicID") or row.get("forumTopicId") or summary.get("ForumTopicID")),
        "dateModified": text(row.get("DateModified") or row.get("dateModified")),
        "sourceAttribution": {
            "baseProvider": "retroachievements",
            "metadataProvider": "retroachievements",
            "metadataRecordUrl": None,
            "boxArtProvider": "retroachievements",
            "boxArtRecordUrl": None,
            "providerIds": ["retroachievements"],
            "retroachievementsGameId": game_id,
        },
        "releaseInfo": {
            "players": None,
            "regionCode": None,
            "rating": None,
            "regionId": None,
            "countryId": None,
            "released": released_raw,
        },
        "achievements": {
            "count": integer(row.get("NumAchievements") or row.get("numAchievements")) or 0,
            "leaderboards": integer(row.get("NumLeaderboards") or row.get("numLeaderboards")) or 0,
            "points": integer(row.get("Points") or row.get("points")) or 0,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch a curated RetroAchievements catalog for the videogame atlas."
    )
    parser.add_argument(
        "--allowlist",
        default=str(ALLOWLIST_PATH),
        help="Path to the system allowlist JSON that decides which systems belong in the atlas.",
    )
    parser.add_argument(
        "--systems",
        nargs="+",
        default=None,
        help="Optional allowlist keys to fetch, for example snes gb gba.",
    )
    parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Only fetch allowlist systems that are not already present in the existing local catalog.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=500,
        help="Maximum number of game-list rows to fetch per RetroAchievements page.",
    )
    parser.add_argument(
        "--list-delay-seconds",
        type=float,
        default=0.5,
        help="Pause between paginated RetroAchievements game-list requests.",
    )
    parser.add_argument(
        "--detail-delay-seconds",
        type=float,
        default=0.15,
        help="Pause between per-game summary requests when filling metadata and box art.",
    )
    parser.add_argument(
        "--skip-game-details",
        action="store_true",
        help="Only fetch per-system game lists and skip per-game summary requests.",
    )
    parser.add_argument(
        "--max-game-detail-requests",
        type=int,
        default=None,
        help="Optional cap on how many uncached per-game summary requests may be made in this run.",
    )
    args = parser.parse_args()

    api_key = require_env("RETROACHIEVEMENTS_WEB_API_KEY")
    allowlist = load_allowlist(Path(args.allowlist).expanduser())
    existing_catalog = load_existing_catalog(OUTPUT_PATH)
    detail_cache = load_detail_cache(DETAIL_CACHE_PATH)

    if args.systems:
        wanted_keys = {text(item) for item in args.systems if text(item)}
        allowlist = [entry for entry in allowlist if text(entry.get("key")) in wanted_keys]

    if args.missing_only:
        existing_system_keys = {
            text(system.get("key"))
            for system in ((existing_catalog or {}).get("systems") or [])
            if isinstance(system, dict) and text(system.get("key"))
        }
        allowlist = [entry for entry in allowlist if text(entry.get("key")) not in existing_system_keys]

    if not allowlist:
        raise SystemExit("No systems remain to fetch after applying the current filters.")

    retro_systems = fetch_all_systems(api_key)
    systems_by_alias: dict[str, dict] = {}
    for retro_system in retro_systems:
        name = text(retro_system.get("Name") or retro_system.get("name"))
        if not name:
            continue
        systems_by_alias[normalize_alias(name)] = retro_system

    systems: list[dict] = []
    games: list[dict] = []
    detail_requests_made = 0
    fetched_system_keys: list[str] = []

    for entry in allowlist:
        retro_system = match_system(entry, systems_by_alias)
        if not retro_system:
            print(f"Could not match allowlist system {text(entry.get('key')) or text(entry.get('name'))} to RetroAchievements.")
            continue

        system = build_system_record(entry, retro_system)
        fetched_system_keys.append(system["key"])
        systems.append(system)
        print(f"Fetching {system['name']} ({system['key']})...")

        game_rows = fetch_game_list(
            api_key=api_key,
            console_id=system["id"],
            page_size=max(args.page_size, 1),
            delay_seconds=max(args.list_delay_seconds, 0),
        )

        for row in game_rows:
            game_id = integer(row.get("ID") or row.get("id"))
            if game_id is None:
                continue

            detail = detail_cache.get(str(game_id))
            if detail is None and not args.skip_game_details:
                if args.max_game_detail_requests is not None and detail_requests_made >= args.max_game_detail_requests:
                    detail = None
                else:
                    detail = fetch_game_summary(api_key, game_id)
                    detail_cache[str(game_id)] = detail
                    detail_requests_made += 1
                    if args.detail_delay_seconds > 0:
                        time.sleep(args.detail_delay_seconds)

            game = build_game_record(row, system, detail)
            if game is not None:
                games.append(game)

        save_detail_cache(DETAIL_CACHE_PATH, detail_cache)
        output = write_catalog_snapshot(
            OUTPUT_PATH,
            existing_catalog=existing_catalog,
            systems=systems,
            games=games,
            detail_requests_made=detail_requests_made,
            fetched_system_keys=fetched_system_keys,
        )

    if systems or games:
        output = write_catalog_snapshot(
            OUTPUT_PATH,
            existing_catalog=existing_catalog,
            systems=systems,
            games=games,
            detail_requests_made=detail_requests_made,
            fetched_system_keys=fetched_system_keys,
        )
    else:
        output = build_output_payload(
            merge_catalog(existing_catalog, systems, games),
            detail_requests_made=detail_requests_made,
            fetched_system_keys=fetched_system_keys,
        )

    print(
        f"Wrote {len(output['systems'])} systems and {len(output['games'])} games to {OUTPUT_PATH} "
        f"({len(fetched_system_keys)} systems fetched this run, {detail_requests_made} new game summaries cached)"
    )


if __name__ == "__main__":
    main()
