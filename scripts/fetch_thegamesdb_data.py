#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import re
import time
import urllib.parse
import urllib.request
from urllib.error import HTTPError
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
ALLOWLIST_PATH = RAW_DIR / "thegamesdb-system-allowlist.json"
OUTPUT_PATH = RAW_DIR / "thegamesdb-catalog.json"
API_URL_BASE = "https://api.thegamesdb.net"

COUNTRY_REGION_OVERRIDES = {
    18: "eu",
    28: "jp",
    50: "us",
}

REGION_ID_TO_CODE = {
    1: "na",
    2: "us",
    4: "jp",
    6: "eu",
    9: "wr",
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


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


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


def join_names(ids: object, lookup: dict[int, str]) -> str | None:
    if not isinstance(ids, list):
        return None
    names = [lookup[item] for item in ids if isinstance(item, int) and item in lookup]
    if not names:
        return None
    return ", ".join(names)


def list_names(ids: object, lookup: dict[int, str]) -> list[str]:
    if not isinstance(ids, list):
        return []
    names = [lookup[item] for item in ids if isinstance(item, int) and item in lookup]
    return [name for name in names if name]


def choose_box_art(include: dict) -> str | None:
    if not isinstance(include, dict):
        return None
    boxart = include.get("boxart")
    if not isinstance(boxart, dict):
        return None
    base_url = boxart.get("base_url")
    if not isinstance(base_url, dict):
        return None
    image_map = boxart.get("data")
    if not isinstance(image_map, dict):
        return None

    preferred_sizes = ["medium", "large", "original", "small", "thumb"]
    base = next((text(base_url.get(size)) for size in preferred_sizes if text(base_url.get(size))), None)
    if not base:
        return None

    return base, image_map


def choose_front_box_art_for_game(game_id: int, include: dict) -> str | None:
    resolved = choose_box_art(include)
    if not resolved:
        return None
    base, image_map = resolved
    entries = image_map.get(str(game_id))
    if not isinstance(entries, list):
        return None

    front_boxart = [
        entry
        for entry in entries
        if isinstance(entry, dict)
        and text(entry.get("type")) == "boxart"
        and text(entry.get("side")) == "front"
        and text(entry.get("filename"))
    ]
    generic_boxart = [
        entry
        for entry in entries
        if isinstance(entry, dict)
        and text(entry.get("type")) == "boxart"
        and text(entry.get("filename"))
    ]
    chosen = front_boxart[0] if front_boxart else (generic_boxart[0] if generic_boxart else None)
    if chosen is None:
        return None
    filename = text(chosen.get("filename"))
    if not filename:
        return None
    return urllib.parse.urljoin(base, filename)


def map_region_code(region_id: int | None, country_id: int | None) -> str | None:
    if country_id in COUNTRY_REGION_OVERRIDES:
        return COUNTRY_REGION_OVERRIDES[country_id]
    if region_id in REGION_ID_TO_CODE:
        return REGION_ID_TO_CODE[region_id]
    return None


class TheGamesDBClient:
    def __init__(self, api_key: str, user_agent: str = "videogame-atlas", max_retries: int = 5) -> None:
        self.api_key = api_key
        self.user_agent = user_agent
        self.max_retries = max_retries

    def fetch_json(self, endpoint: str, **params: str | int) -> dict:
        query = {"apikey": self.api_key}
        for key, value in params.items():
            if value is None:
                continue
            query[key] = str(value)

        url = f"{API_URL_BASE}{endpoint}?{urllib.parse.urlencode(query)}"
        request = urllib.request.Request(url, headers={"User-Agent": self.user_agent})
        attempt = 0
        while True:
            try:
                with urllib.request.urlopen(request, timeout=90) as response:
                    payload = response.read()
                return json.loads(payload.decode("utf-8"))
            except HTTPError as exc:
                if exc.code != 429 or attempt >= self.max_retries:
                    raise
                attempt += 1
                wait_seconds = min(30, max(2, 2 ** attempt))
                time.sleep(wait_seconds)

    def fetch_lookup_table(self, endpoint: str, collection_name: str) -> dict[int, str]:
        payload = self.fetch_json(endpoint)
        collection = ((payload.get("data") or {}).get(collection_name) or {})
        if not isinstance(collection, dict):
            return {}
        result: dict[int, str] = {}
        for key, row in collection.items():
            if not isinstance(row, dict):
                continue
            item_id = integer(row.get("id") or key)
            name = text(row.get("name"))
            if item_id is None or not name:
                continue
            result[item_id] = name
        return result


def load_allowlist(path: Path) -> list[dict]:
    payload = load_json(path)
    if isinstance(payload, dict):
        systems = payload.get("systems")
        if isinstance(systems, list):
            return [item for item in systems if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    raise SystemExit(f"Expected a list or object-with-systems in {path}")


def fetch_platforms(client: TheGamesDBClient, platform_ids: list[int]) -> dict[int, dict]:
    if not platform_ids:
        return {}
    payload = client.fetch_json(
        "/v1/Platforms/ByPlatformID",
        id=",".join(str(platform_id) for platform_id in platform_ids),
        fields="overview,developer",
    )
    platforms = ((payload.get("data") or {}).get("platforms") or {})
    if not isinstance(platforms, dict):
        return {}
    result: dict[int, dict] = {}
    for key, row in platforms.items():
        if not isinstance(row, dict):
            continue
        platform_id = integer(row.get("id") or key)
        if platform_id is None:
            continue
        result[platform_id] = row
    return result


def fetch_platform_games(
    client: TheGamesDBClient,
    platform_id: int,
    delay_seconds: float,
) -> tuple[list[dict], int | None]:
    games: list[dict] = []
    remaining_allowance: int | None = None
    page = 1

    while True:
        payload = client.fetch_json(
            "/v1/Games/ByPlatformID",
            id=platform_id,
            fields="players,publishers,genres,overview,last_updated,rating,alternates",
            include="boxart",
            page=page,
        )
        remaining_allowance = integer(payload.get("remaining_monthly_allowance"))
        data = payload.get("data") or {}
        page_games = data.get("games") or []
        include = payload.get("include") or {}
        if not isinstance(page_games, list) or not page_games:
            break

        for row in page_games:
            if not isinstance(row, dict):
                continue
            row = dict(row)
            game_id = integer(row.get("id"))
            if game_id is None:
                continue
            row["_boxart_url"] = choose_front_box_art_for_game(game_id, include)
            games.append(row)

        pages = payload.get("pages") or {}
        next_page = pages.get("next") if isinstance(pages, dict) else None
        if not next_page:
            break

        page += 1
        time.sleep(max(delay_seconds, 0))

    return games, remaining_allowance


def build_system_record(entry: dict, platform: dict) -> dict:
    platform_id = integer(entry.get("platformId"))
    name = text(entry.get("name")) or text(platform.get("name")) or f"Platform {platform_id}"
    return {
        "id": platform_id,
        "key": text(entry.get("key")) or text(platform.get("alias")) or str(platform_id),
        "name": name,
        "shortName": text(entry.get("shortName")) or name,
        "manufacturer": text(entry.get("manufacturer")) or text(platform.get("developer")) or "Unknown",
        "category": text(entry.get("category")) or "Unknown",
        "generation": text(entry.get("generation")) or "Unknown",
        "releaseYear": integer(entry.get("releaseYear")),
        "endYear": integer(entry.get("endYear")),
        "summary": text(entry.get("summary")) or text(platform.get("overview")),
        "sourceAttribution": {
            "baseProvider": "thegamesdb",
            "metadataProvider": "thegamesdb",
            "metadataRecordUrl": "https://thegamesdb.net/",
            "providerIds": ["thegamesdb"],
        },
        "wikiUrl": None,
    }


def build_game_record(
    row: dict,
    system: dict,
    genres: dict[int, str],
    developers: dict[int, str],
    publishers: dict[int, str],
) -> dict | None:
    game_id = integer(row.get("id"))
    title = text(row.get("game_title"))
    if game_id is None or not title:
        return None

    region_id = integer(row.get("region_id"))
    country_id = integer(row.get("country_id"))
    region_code = map_region_code(region_id, country_id)
    developer_name = join_names(row.get("developers"), developers)
    publisher_name = join_names(row.get("publishers"), publishers)
    genre_names = list_names(row.get("genres"), genres)
    image_url = text(row.get("_boxart_url"))

    return {
        "id": game_id,
        "slug": f"{system['key']}-{re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-') or 'game'}",
        "title": title,
        "sortTitle": normalize_title(title),
        "systemId": system["id"],
        "systemName": system["name"],
        "releaseYear": parse_release_year(text(row.get("release_date"))),
        "developer": developer_name,
        "publisher": publisher_name,
        "genres": genre_names,
        "summary": text(row.get("overview")),
        "media": {
            "boxFront": {
                "url": image_url,
                "provider": "thegamesdb",
                "kind": "front-cover",
                "alt": f"Box art for {title}",
            },
            "screenshot": {"url": None, "provider": "thegamesdb", "kind": None, "alt": None},
            "logo": {"url": None, "provider": "thegamesdb", "kind": None, "alt": None},
            "video": {"url": None, "provider": "thegamesdb", "kind": None, "alt": None},
        },
        "image": {
            "iconUrl": image_url,
            "alt": f"Box art for {title}",
        },
        "hashes": [],
        "forumTopicId": None,
        "dateModified": text(row.get("last_updated")),
        "sourceAttribution": {
            "baseProvider": "thegamesdb",
            "metadataProvider": "thegamesdb",
            "metadataRecordUrl": f"https://thegamesdb.net/game.php?id={game_id}",
            "boxArtProvider": "thegamesdb",
            "boxArtRecordUrl": f"https://thegamesdb.net/game.php?id={game_id}",
            "providerIds": ["thegamesdb"],
            "thegamesdbGameId": game_id,
        },
        "releaseInfo": {
            "players": text(row.get("players")),
            "regionCode": region_code,
            "rating": text(row.get("rating")),
            "regionId": region_id,
            "countryId": country_id,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch a curated TheGamesDB catalog for the videogame atlas."
    )
    parser.add_argument(
        "--allowlist",
        default=str(ALLOWLIST_PATH),
        help="Path to the system allowlist JSON that decides which platforms belong in the atlas.",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=0.5,
        help="Pause between paginated platform requests.",
    )
    parser.add_argument(
        "--limit-games-per-system",
        type=int,
        default=None,
        help="Only keep the first N games for each selected system.",
    )
    args = parser.parse_args()

    api_key = require_env("THEGAMESDB_API_KEY")
    client = TheGamesDBClient(api_key=api_key)
    allowlist = load_allowlist(Path(args.allowlist).expanduser())

    selected_platform_ids = [
        platform_id
        for platform_id in (integer(entry.get("platformId")) for entry in allowlist)
        if platform_id is not None
    ]
    if not selected_platform_ids:
        raise SystemExit("The system allowlist does not contain any valid platformId values.")

    genres = client.fetch_lookup_table("/v1/Genres", "genres")
    developers = client.fetch_lookup_table("/v1/Developers", "developers")
    publishers = client.fetch_lookup_table("/v1/Publishers", "publishers")
    platforms = fetch_platforms(client, selected_platform_ids)

    systems: list[dict] = []
    games: list[dict] = []
    remaining_allowance: int | None = None

    for entry in allowlist:
        platform_id = integer(entry.get("platformId"))
        if platform_id is None:
            continue
        platform = platforms.get(platform_id, {})
        system = build_system_record(entry, platform)
        systems.append(system)

        platform_games, remaining_allowance = fetch_platform_games(
            client,
            platform_id=platform_id,
            delay_seconds=args.delay_seconds,
        )
        if args.limit_games_per_system is not None:
            platform_games = platform_games[: args.limit_games_per_system]

        for row in platform_games:
            game = build_game_record(row, system, genres, developers, publishers)
            if game is not None:
                games.append(game)

    output = {
        "metadata": {
            "importedAt": datetime.now(timezone.utc).isoformat(),
            "catalogSource": "thegamesdb",
            "provider": "thegamesdb",
            "systemCount": len(systems),
            "gameCount": len(games),
            "remainingMonthlyAllowance": remaining_allowance,
            "systems": [system["key"] for system in systems],
            "notes": [
                "This catalog is curated from TheGamesDB using a local system allowlist.",
                "Game images are stored as remote TheGamesDB URLs so the atlas can stay lightweight.",
                "Release region codes are approximated from TheGamesDB region_id and country_id fields when a common mapping is known.",
            ],
        },
        "systems": systems,
        "games": games,
    }

    OUTPUT_PATH.write_text(json.dumps(output, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    print(f"Wrote {len(systems)} systems and {len(games)} games to {OUTPUT_PATH}")
    if remaining_allowance is not None:
        print(f"Remaining monthly allowance: {remaining_allowance}")


if __name__ == "__main__":
    main()
