#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
SYSTEMS_PATH = RAW_DIR / "retroachievements-systems.json"
SYSTEM_MAP_PATH = RAW_DIR / "screenscraper-system-map.json"
OUTPUT_PATH = RAW_DIR / "screenscraper-game-enrichment.json"
USER_INFO_PATH = RAW_DIR / "screenscraper-user-info.json"
API_URL_BASE = "https://api.screenscraper.fr/api2"


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


def normalize_text(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "unknown"


def normalize_url(url: str | None) -> str | None:
    if not url:
        return None
    return url.replace("#screenscraperserveur#", "https://www.screenscraper.fr/").replace(" ", "%20")


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def env_or_default(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value or default


def unique_nonempty(values: list[str | None]) -> list[str]:
    seen: set[str] = set()
    items: list[str] = []
    for value in values:
        cleaned = text(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        items.append(cleaned)
    return items


def deep_merge(base: object, overlay: object) -> object:
    if isinstance(base, dict) and isinstance(overlay, dict):
        merged = dict(base)
        for key, value in overlay.items():
            merged[key] = deep_merge(merged.get(key), value)
        return merged

    if isinstance(overlay, list):
        return overlay if overlay else (base if isinstance(base, list) else [])

    if overlay in (None, "", {}):
        return base

    return overlay


def parse_query_string(raw: str) -> dict[str, str]:
    return {
        key: values[-1]
        for key, values in urllib.parse.parse_qs(raw, keep_blank_values=False).items()
        if values
    }


def find_first_child_by_attribute(
    parent: ET.Element | None,
    tag_name: str,
    attribute_name: str,
    preferred_values: list[str],
) -> ET.Element | None:
    if parent is None:
        return None

    children = [child for child in parent.findall(tag_name)]
    for preferred in preferred_values:
        for child in children:
            if (child.attrib.get(attribute_name) or "").lower() == preferred.lower():
                return child

    return children[0] if children else None


def collect_texts_by_language(parent: ET.Element | None, tag_name: str, language: str) -> list[str]:
    if parent is None:
        return []

    items: list[str] = []
    fallback: list[str] = []
    for child in parent.findall(tag_name):
        value = text(child.text)
        if not value:
            continue
        child_language = (child.attrib.get("langue") or "").lower()
        if child_language == language.lower():
            items.append(value)
        elif child_language in {"en", "wor", ""}:
            fallback.append(value)

    return items or fallback


def parse_release_year(game_node: ET.Element, region: str) -> int | None:
    date_node = find_first_child_by_attribute(
        game_node.find("dates"),
        "date",
        "region",
        [region, "wor", "us", "eu", "jp", "ss", ""],
    )
    raw_value = text(date_node.text if date_node is not None else None)
    if not raw_value:
        return None
    match = re.match(r"(\d{4})", raw_value)
    return integer(match.group(1)) if match else None


def choose_media(
    medias_node: ET.Element | None,
    media_types: list[str],
    region: str,
    language: str,
) -> dict | None:
    if medias_node is None:
        return None

    candidates = []
    for media_node in medias_node.findall("media"):
        media_type = media_node.attrib.get("type")
        if media_type not in media_types:
            continue
        candidates.append(media_node)

    if not candidates:
        return None

    preferred_regions = [language.lower(), region.lower(), "wor", "us", "eu", "jp", "ss", "cus", ""]
    for preferred_region in preferred_regions:
        for node in candidates:
            node_region = (node.attrib.get("region") or "").lower()
            if node_region == preferred_region:
                return {
                    "url": normalize_url(text(node.text)),
                    "provider": "screenscraper",
                    "kind": node.attrib.get("type"),
                    "alt": None,
                }

    node = candidates[0]
    return {
        "url": normalize_url(text(node.text)),
        "provider": "screenscraper",
        "kind": node.attrib.get("type"),
        "alt": None,
    }


@dataclass
class ScreenScraperClient:
    dev_login: str
    user: str
    password: str
    softname: str

    def build_url(self, endpoint: str, **params: str | int) -> str:
        query = parse_query_string(self.dev_login)
        query["softname"] = self.softname
        query["output"] = "xml"
        query["ssid"] = self.user
        query["sspassword"] = self.password

        for key, value in params.items():
            if value is None:
                continue
            query[key] = str(value)

        return f"{API_URL_BASE}/{endpoint}?{urllib.parse.urlencode(query)}"

    def fetch_xml(self, endpoint: str, **params: str | int) -> ET.Element:
        url = self.build_url(endpoint, **params)
        request = urllib.request.Request(url, headers={"User-Agent": self.softname})
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = response.read()
        return ET.fromstring(payload)

    def fetch_user_info(self) -> dict:
        root = self.fetch_xml("ssuserInfos.php")
        user_node = root.find("./ssuser") or root.find("./Data/ssuser")
        if user_node is None:
            return {}

        def node_text(name: str) -> str | None:
            child = user_node.find(name)
            return text(child.text if child is not None else None)

        return {
            "id": node_text("id"),
            "maxThreads": integer(node_text("maxthreads")),
            "requestsToday": integer(node_text("requeststoday")),
            "requestsKoToday": integer(node_text("requestskotoday")),
            "maxRequestsPerMinute": integer(node_text("maxrequestspermin")),
            "maxRequestsPerDay": integer(node_text("maxrequestsperday")),
            "maxRequestsKoPerDay": integer(node_text("maxrequestskoperday")),
        }

    def search_game(self, title: str, system_id: int | None) -> ET.Element | None:
        params: dict[str, str | int] = {"recherche": title}
        if system_id is not None:
            params["systemeid"] = system_id

        root = self.fetch_xml("jeuRecherche.php", **params)
        data_node = root.find("./jeux") or root.find("./Data/jeux")
        if data_node is None:
            return None
        game_nodes = data_node.findall("jeu")
        if not game_nodes:
            return None

        normalized_title = normalize_text(title)
        for node in game_nodes:
            names = node.find("noms")
            for name_node in names.findall("nom") if names is not None else []:
                if normalize_text(name_node.text or "") == normalized_title:
                    return node

        return game_nodes[0]


def load_games() -> list[dict]:
    systems_payload = load_json(SYSTEMS_PATH)
    system_rows = systems_payload if isinstance(systems_payload, list) else []
    known_system_ids = {
        integer((row or {}).get("ID") or (row or {}).get("id"))
        for row in system_rows
        if isinstance(row, dict)
    }

    games: list[dict] = []
    for path in sorted(RAW_DIR.glob("retroachievements-game-list-*.json")):
        payload = load_json(path)
        if not isinstance(payload, list):
            continue
        for row in payload:
            if not isinstance(row, dict):
                continue
            game_id = integer(row.get("ID") or row.get("id"))
            system_id = integer(row.get("ConsoleID") or row.get("consoleId") or row.get("systemId"))
            title = text(row.get("Title") or row.get("title") or row.get("Name") or row.get("name"))
            if game_id is None or system_id is None or not title:
                continue
            if known_system_ids and system_id not in known_system_ids:
                continue
            games.append(
                {
                    "id": game_id,
                    "title": title,
                    "systemId": system_id,
                    "systemName": text(row.get("ConsoleName") or row.get("consoleName") or row.get("systemName")),
                }
            )
    return games


def parse_game_result(
    game: dict,
    system_map: dict[str, dict],
    game_node: ET.Element,
    language: str,
    region: str,
) -> dict:
    names = collect_texts_by_language(game_node.find("noms"), "nom", language)
    synopsis_nodes = collect_texts_by_language(game_node.find("synopsis"), "synopsis", language)
    genres = collect_texts_by_language(game_node.find("genres"), "genre", language)
    medias_node = game_node.find("medias")

    box_front = choose_media(medias_node, ["box-2D", "box-3D"], region, language)
    screenshot = choose_media(medias_node, ["ss", "sstitle"], region, language)
    logo = choose_media(
        medias_node,
        ["wheel", "wheel-hd", "wheel-steel", "wheel-carbon", "screenmarqueesmall", "screenmarquee"],
        region,
        language,
    )
    video = choose_media(medias_node, ["video-normalized", "video"], region, language)

    screenscraper_id = text(game_node.attrib.get("id"))
    system_entry = system_map.get(str(game["systemId"]), {})
    system_record_url = None
    if screenscraper_id:
        system_ssid = text(system_entry.get("screenscraperSystemId"))
        if system_ssid:
            system_record_url = (
                "https://www.screenscraper.fr/gameinfos.php?gameid="
                f"{urllib.parse.quote(screenscraper_id)}&action=onglet&zone=gameinfosinfos"
            )

    provider_ids = ["screenscraper"]
    return {
        "metadata": {
            "releaseYear": parse_release_year(game_node, region),
            "developer": text(game_node.findtext("developpeur")),
            "publisher": text(game_node.findtext("editeur")),
            "genres": unique_nonempty(genres),
            "summary": synopsis_nodes[0] if synopsis_nodes else None,
        },
        "media": {
            "boxFront": deep_merge(
                {"alt": f"Box art for {game['title']}"},
                box_front or {"provider": "screenscraper", "kind": "box-2D", "url": None},
            ),
            "screenshot": deep_merge(
                {"alt": f"Screenshot for {game['title']}"},
                screenshot or {"provider": "screenscraper", "kind": "ss", "url": None},
            ),
            "logo": deep_merge(
                {"alt": f"Logo for {game['title']}"},
                logo or {"provider": "screenscraper", "kind": "wheel", "url": None},
            ),
            "video": deep_merge(
                {"alt": f"Video preview for {game['title']}"},
                video or {"provider": "screenscraper", "kind": "video", "url": None},
            ),
        },
        "sources": {
            "metadataProvider": "screenscraper",
            "metadataRecordUrl": system_record_url or "https://screenscraper.fr/",
            "boxArtProvider": "screenscraper",
            "boxArtRecordUrl": system_record_url or "https://screenscraper.fr/",
            "providerIds": provider_ids,
            "screenscraperGameId": screenscraper_id,
            "matchedName": names[0] if names else game["title"],
            "screenscraperSystemId": system_entry.get("screenscraperSystemId"),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch ScreenScraper metadata and media references for the videogame atlas."
    )
    parser.add_argument("--limit", type=int, default=None, help="Only fetch the first N games.")
    parser.add_argument("--game-id", type=int, action="append", default=[], help="Fetch only specific game IDs.")
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=1.2,
        help="Pause between requests to respect ScreenScraper rate limits.",
    )
    parser.add_argument(
        "--write-user-info",
        action="store_true",
        help="Also fetch ScreenScraper user quota info into data/raw/screenscraper-user-info.json.",
    )
    args = parser.parse_args()

    dev_login = require_env("SCREENSCRAPER_DEV_LOGIN")
    user = require_env("SCREENSCRAPER_USER")
    password = require_env("SCREENSCRAPER_PASSWORD")
    softname = env_or_default("SCREENSCRAPER_SOFTNAME", "videogame-atlas")
    region = env_or_default("SCREENSCRAPER_REGION", "us")
    language = env_or_default("SCREENSCRAPER_LANGUAGE", "en")

    client = ScreenScraperClient(
        dev_login=dev_login,
        user=user,
        password=password,
        softname=softname,
    )

    games = load_games()
    if args.game_id:
        wanted_ids = set(args.game_id)
        games = [game for game in games if game["id"] in wanted_ids]
    if args.limit is not None:
        games = games[: args.limit]

    system_map = load_json(SYSTEM_MAP_PATH, default={})
    if not isinstance(system_map, dict):
        raise SystemExit(f"Expected an object in {SYSTEM_MAP_PATH}")

    existing = load_json(OUTPUT_PATH, default={})
    if not isinstance(existing, dict):
        existing = {}

    results = dict(existing)
    for index, game in enumerate(games, start=1):
        system_entry = system_map.get(str(game["systemId"]), {})
        screenscraper_system_id = integer(system_entry.get("screenscraperSystemId"))
        if screenscraper_system_id is None:
            print(
                f"Skipping game {game['id']} ({game['title']}) because there is no ScreenScraper system mapping.",
                file=sys.stderr,
            )
            continue

        print(f"[{index}/{len(games)}] Fetching {game['title']} ({game['systemName'] or game['systemId']})")
        game_node = client.search_game(game["title"], screenscraper_system_id)
        if game_node is None:
            print(f"  No ScreenScraper match returned for {game['title']}.", file=sys.stderr)
            continue

        results[str(game["id"])] = parse_game_result(game, system_map, game_node, language, region)
        OUTPUT_PATH.write_text(json.dumps(results, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        time.sleep(max(args.delay_seconds, 0))

    if args.write_user_info:
        user_info = client.fetch_user_info()
        USER_INFO_PATH.write_text(json.dumps(user_info, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
