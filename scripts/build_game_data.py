#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import shutil
import urllib.parse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
OUTPUT_JSON = ROOT / "data" / "game-database.json"
OUTPUT_JS = ROOT / "data" / "game-database.js"
BATOCERA_LIBRARY_PATH = RAW_DIR / "batocera-library.json"
THEGAMESDB_CATALOG_PATH = RAW_DIR / "thegamesdb-catalog.json"
RETROACHIEVEMENTS_CATALOG_PATH = RAW_DIR / "retroachievements-catalog.json"
PROVIDER_CATALOG_PATH = RAW_DIR / "provider-catalog.json"

NINTENDO_KEYS = {
    "3ds",
    "fds",
    "gameandwatch",
    "gamecube",
    "gb",
    "gba",
    "gbc",
    "n64",
    "nds",
    "nes",
    "satellaview",
    "snes",
    "snes-msu1",
    "virtualboy",
    "wii",
    "wiiu",
}
SEGA_KEYS = {
    "dreamcast",
    "gamegear",
    "mastersystem",
    "megacd",
    "megadrive",
    "saturn",
    "sega32x",
    "sg1000",
}
SONY_KEYS = {"psp", "ps2", "ps3", "psx"}
MICROSOFT_KEYS = {"xbox", "xbox360"}
ATARI_KEYS = {"atari2600", "atari5200", "atari7800", "atari800", "atarist", "jaguar", "lynx"}
SNK_KEYS = {"neogeo", "neogeocd", "ngpc"}
NEC_KEYS = {"pcengine", "pcenginecd", "pcfx", "supergrafx"}
COMMODORE_KEYS = {"amiga1200", "c64"}
ACORN_KEYS = {"archimedes", "atom", "bbc"}
BANDAI_KEYS = {"sufami", "wswan", "wswanc"}
COMMUNITY_RUNTIME_KEYS = {
    "cannonball",
    "devilutionx",
    "mrboom",
    "odcommander",
    "ports",
    "prboom",
    "pygame",
    "sdlpop",
    "xash3d_fwgs",
    "zmachine",
}
HOME_CONSOLE_KEYS = {
    "3do",
    "arcadia",
    "astrocde",
    "atari2600",
    "atari5200",
    "atari7800",
    "channelf",
    "colecovision",
    "dreamcast",
    "gamecube",
    "intellivision",
    "jaguar",
    "mastersystem",
    "megadrive",
    "n64",
    "neogeo",
    "neogeocd",
    "nes",
    "o2em",
    "pcengine",
    "pcfx",
    "ps2",
    "ps3",
    "psx",
    "saturn",
    "sg1000",
    "snes",
    "supergrafx",
    "vc4000",
    "vectrex",
    "wii",
    "wiiu",
    "xbox",
    "xbox360",
}
HANDHELD_KEYS = {
    "3ds",
    "gameandwatch",
    "gamegear",
    "gb",
    "gba",
    "gbc",
    "lynx",
    "ngpc",
    "psp",
    "virtualboy",
    "wswan",
    "wswanc",
}
COMPUTER_KEYS = {
    "adam",
    "amiga1200",
    "amstradcpc",
    "apple2",
    "archimedes",
    "atari800",
    "atarist",
    "atom",
    "bbc",
    "c64",
    "dos",
    "msx1",
    "msx2",
    "oricatmos",
    "x1",
    "x68000",
    "zx81",
    "zxspectrum",
}
ARCADE_KEYS = {"atomiswave", "mame"}
ADDON_KEYS = {"fds", "megacd", "pcenginecd", "satellaview", "sega32x", "snes-msu1", "sufami"}
BATOCERA_NAME_OVERRIDES = {
    "3ds": "Nintendo 3DS",
    "atari2600": "Atari 2600",
    "bbc": "BBC Micro",
    "fds": "Family Computer Disk System",
    "gameandwatch": "Game & Watch",
    "gamecube": "Nintendo GameCube",
    "gb": "Game Boy",
    "gba": "Game Boy Advance",
    "gbc": "Game Boy Color",
    "megacd": "Mega-CD",
    "neogeocd": "Neo Geo CD",
    "ngpc": "Neo Geo Pocket Color",
    "nds": "Nintendo DS",
    "o2em": "Magnavox Odyssey 2",
    "pcengine": "PC Engine",
    "pcenginecd": "PC Engine CD",
    "ps2": "PlayStation 2",
    "ps3": "PlayStation 3",
    "psp": "PlayStation Portable",
    "psx": "PlayStation",
    "pygame": "Pygame Collection",
    "snes-msu1": "SNES MSU-1",
    "vc4000": "Interton VC 4000",
    "wiiu": "Wii U",
    "wswan": "WonderSwan",
    "wswanc": "WonderSwan Color",
    "xash3d_fwgs": "Half-Life",
    "zmachine": "Z-machine",
    "zxspectrum": "ZX Spectrum",
}


def load_json(path: Path, default: object | None = None) -> object:
    if not path.exists():
        if default is not None:
            return default
        raise FileNotFoundError(f"Missing required source file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def extract_rows(payload: object) -> list[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("items", "results", "data", "systems", "games"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    raise ValueError("Expected a list payload or an object containing a list")


def text(value: object) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def integer(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    cleaned = text(value)
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "unknown"


def normalize_title(title: str) -> str:
    normalized = re.sub(r"^[Tt]he\s+", "", title.strip())
    return normalized


def make_search_blob(parts: list[object]) -> str:
    return " ".join(text(part) or "" for part in parts).strip().lower()


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


def dedupe_nonempty(values: list[object]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        cleaned = text(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append(cleaned)
    return deduped


def normalize_media_entry(entry: object, fallback_url: str | None, fallback_alt: str) -> dict:
    payload = entry if isinstance(entry, dict) else {}
    return {
        "url": text(payload.get("url")) or fallback_url,
        "provider": text(payload.get("provider")),
        "kind": text(payload.get("kind")),
        "alt": text(payload.get("alt")) or fallback_alt,
    }


def load_provider_catalog() -> dict:
    payload = load_json(
        PROVIDER_CATALOG_PATH if PROVIDER_CATALOG_PATH.exists() else (RAW_DIR / "batocera-provider-catalog.json"),
        default={},
    )
    if not isinstance(payload, dict):
        return {"providers": [], "atlasStrategy": {}}
    providers = payload.get("providers")
    strategy = payload.get("atlasStrategy")
    return {
        "providers": [item for item in providers if isinstance(item, dict)] if isinstance(providers, list) else [],
        "atlasStrategy": strategy if isinstance(strategy, dict) else {},
    }


def normalize_relative_asset_path(value: object) -> str | None:
    cleaned = text(value)
    if not cleaned:
        return None
    raw_path = cleaned.replace("\\", "/")
    if raw_path.startswith("/") or re.match(r"^[A-Za-z]:/", raw_path):
        return None
    parts = []
    for part in PurePosixPath(raw_path).parts:
        if part in ("", "."):
            continue
        if part == "..":
            return None
        parts.append(part)
    if not parts:
        return None
    return PurePosixPath(*parts).as_posix()


def build_public_asset_url(base_url: str, relative_path: str) -> str:
    encoded_path = "/".join(urllib.parse.quote(part) for part in PurePosixPath(relative_path).parts)
    return base_url.rstrip("/") + "/" + encoded_path


def infer_batocera_manufacturer(system_key: str) -> str | None:
    if system_key in NINTENDO_KEYS:
        return "Nintendo"
    if system_key in SEGA_KEYS:
        return "Sega"
    if system_key in SONY_KEYS:
        return "Sony"
    if system_key in MICROSOFT_KEYS:
        return "Microsoft"
    if system_key in ATARI_KEYS:
        return "Atari"
    if system_key in SNK_KEYS:
        return "SNK"
    if system_key in NEC_KEYS:
        return "NEC"
    if system_key in COMMODORE_KEYS:
        return "Commodore"
    if system_key in ACORN_KEYS:
        return "Acorn"
    if system_key in BANDAI_KEYS:
        return "Bandai"
    if system_key in COMMUNITY_RUNTIME_KEYS:
        return "Community"
    explicit_map = {
        "3do": "The 3DO Company",
        "adam": "Coleco",
        "amstradcpc": "Amstrad",
        "apple2": "Apple",
        "arcadia": "Emerson",
        "astrocde": "Bally",
        "atomiswave": "Sammy",
        "cannonball": "Community",
        "channelf": "Fairchild",
        "colecovision": "Coleco",
        "dos": "IBM PC compatibles",
        "intellivision": "Mattel",
        "mame": "Various arcade manufacturers",
        "msx1": "MSX consortium",
        "msx2": "MSX consortium",
        "o2em": "Magnavox",
        "oricatmos": "Oric",
        "steam": "Valve",
        "vc4000": "Interton",
        "vectrex": "GCE",
        "x1": "Sharp",
        "x68000": "Sharp",
        "zx81": "Sinclair Research",
        "zxspectrum": "Sinclair Research",
    }
    return explicit_map.get(system_key)


def infer_batocera_category(system_key: str) -> str:
    if system_key in HOME_CONSOLE_KEYS:
        return "Home console"
    if system_key in HANDHELD_KEYS:
        return "Handheld"
    if system_key in COMPUTER_KEYS:
        return "Computer"
    if system_key in ARCADE_KEYS:
        return "Arcade"
    if system_key in ADDON_KEYS:
        return "Add-on / expansion"
    if system_key in COMMUNITY_RUNTIME_KEYS:
        return "Software platform"
    if system_key in {"steam"}:
        return "PC storefront"
    return "Specialized platform"


def infer_batocera_generation(system_key: str, category: str, release_year: int | None) -> str:
    if category == "Software platform":
        return "Modern runtime"
    if category == "PC storefront":
        return "Digital distribution era"
    if category == "Add-on / expansion":
        return "Expansion era"
    if category == "Computer":
        if release_year is None:
            return "Computer era"
        if release_year < 1984:
            return "Early microcomputer era"
        if release_year < 1990:
            return "Home computer era"
        if release_year < 2000:
            return "Personal computer era"
        return "Modern computer era"
    if category == "Arcade":
        if release_year is None:
            return "Arcade era"
        if release_year < 1990:
            return "Classic arcade era"
        if release_year < 2000:
            return "Arcade 3D era"
        return "Late arcade era"
    if category == "Handheld":
        if release_year is None:
            return "Portable era"
        if release_year < 1990:
            return "Early handheld era"
        if release_year < 2000:
            return "8-bit handheld era"
        if release_year < 2012:
            return "Portable 3D era"
        return "Modern handheld era"
    if release_year is None:
        return "Console era"
    if release_year < 1983:
        return "Second generation"
    if release_year < 1988:
        return "Third generation"
    if release_year < 1994:
        return "Fourth generation"
    if release_year < 2000:
        return "Fifth generation"
    if release_year < 2006:
        return "Sixth generation"
    if release_year < 2013:
        return "Seventh generation"
    return "Eighth generation"


def enrich_batocera_system(system: dict) -> dict:
    enriched = dict(system)
    system_key = text(enriched.get("key")) or ""
    release_year = integer(enriched.get("releaseYear"))
    manufacturer = text(enriched.get("manufacturer")) or infer_batocera_manufacturer(system_key)
    category = text(enriched.get("category"))
    if not category or category == "Imported system":
        category = infer_batocera_category(system_key)
    generation = text(enriched.get("generation"))
    if not generation or generation == "Unspecified":
        generation = infer_batocera_generation(system_key, category, release_year)

    if system_key in BATOCERA_NAME_OVERRIDES:
        enriched["name"] = BATOCERA_NAME_OVERRIDES[system_key]
        if text(enriched.get("shortName")) in {None, system.get("name")}:
            enriched["shortName"] = BATOCERA_NAME_OVERRIDES[system_key]

    enriched["manufacturer"] = manufacturer
    enriched["category"] = category
    enriched["generation"] = generation
    return enriched


def resolve_batocera_box_art_source(
    game: dict,
    batocera_roms_root: Path | None,
) -> tuple[Path | None, str | None]:
    batocera = game.get("batocera")
    if not isinstance(batocera, dict):
        return None, None

    system_key = text(batocera.get("systemKey"))
    relative_image_path = normalize_relative_asset_path(batocera.get("relativeImagePath"))
    if not system_key or not relative_image_path:
        return None, None

    publish_relative_path = PurePosixPath(system_key).joinpath(PurePosixPath(relative_image_path)).as_posix()

    gamelist_path = text(batocera.get("gamelistPath"))
    if not gamelist_path:
        return None, publish_relative_path

    source_path: Path | None = None
    gamelist_candidate = Path(gamelist_path).expanduser()
    if gamelist_candidate.is_absolute():
        source_path = (gamelist_candidate.resolve().parent / relative_image_path).resolve()
    elif batocera_roms_root is not None:
        source_path = (batocera_roms_root / gamelist_candidate.parent / relative_image_path).resolve()

    return source_path, publish_relative_path


def stage_batocera_box_art_for_web(
    games: list[dict],
    publish_root: Path,
    publish_base_url: str,
    batocera_roms_root: Path | None = None,
) -> dict:
    publish_root.mkdir(parents=True, exist_ok=True)

    copied_assets: set[str] = set()
    staged_count = 0
    missing_count = 0

    for game in games:
        media = game.get("media")
        image = game.get("image")
        source_attribution = game.get("sourceAttribution")
        if not isinstance(media, dict) or not isinstance(image, dict) or not isinstance(source_attribution, dict):
            continue

        box_front = media.get("boxFront")
        if not isinstance(box_front, dict):
            continue

        source_path, publish_relative_path = resolve_batocera_box_art_source(game, batocera_roms_root)
        destination = (
            publish_root.joinpath(*PurePosixPath(publish_relative_path).parts)
            if publish_relative_path
            else None
        )
        if not publish_relative_path or destination is None:
            if text(box_front.get("url")):
                missing_count += 1
            box_front["url"] = None
            image["iconUrl"] = None
            continue

        destination.parent.mkdir(parents=True, exist_ok=True)
        if publish_relative_path not in copied_assets and source_path and source_path.exists():
            if not destination.exists() or destination.stat().st_size != source_path.stat().st_size:
                shutil.copyfile(source_path, destination)
            copied_assets.add(publish_relative_path)
        elif publish_relative_path not in copied_assets and destination.exists():
            copied_assets.add(publish_relative_path)
        elif publish_relative_path not in copied_assets:
            if text(box_front.get("url")):
                missing_count += 1
            box_front["url"] = None
            image["iconUrl"] = None
            continue

        public_url = build_public_asset_url(publish_base_url, publish_relative_path)
        box_front["url"] = public_url
        image["iconUrl"] = public_url
        source_attribution["boxArtPublishedUrl"] = public_url
        source_attribution["boxArtPublishPath"] = publish_relative_path
        staged_count += 1

    return {
        "mode": "published-web-url",
        "boxArtBaseUrl": publish_base_url.rstrip("/"),
        "boxArtStagingDir": str(publish_root),
        "publishedGameImageCount": staged_count,
        "missingBoxArtCount": missing_count,
        "copiedAssetCount": len(copied_assets),
    }


def load_batocera_library() -> dict | None:
    payload = load_json(BATOCERA_LIBRARY_PATH, default=None)
    if not isinstance(payload, dict):
        return None
    systems = payload.get("systems")
    games = payload.get("games")
    if not isinstance(systems, list) or not isinstance(games, list):
        return None
    return payload


def load_thegamesdb_catalog() -> dict | None:
    payload = load_json(THEGAMESDB_CATALOG_PATH, default=None)
    if not isinstance(payload, dict):
        return None
    systems = payload.get("systems")
    games = payload.get("games")
    if not isinstance(systems, list) or not isinstance(games, list):
        return None
    return payload


def load_retroachievements_catalog() -> dict | None:
    payload = load_json(RETROACHIEVEMENTS_CATALOG_PATH, default=None)
    if not isinstance(payload, dict):
        return None
    systems = payload.get("systems")
    games = payload.get("games")
    if not isinstance(systems, list) or not isinstance(games, list):
        return None
    return payload


def load_systems() -> list[dict]:
    systems_payload = load_json(RAW_DIR / "retroachievements-systems.json")
    systems_rows = extract_rows(systems_payload)
    enrichments = load_json(RAW_DIR / "system-enrichment.json", default={})

    systems: list[dict] = []
    for row in systems_rows:
        system_id = integer(row.get("ID") or row.get("id"))
        name = text(row.get("Name") or row.get("name"))
        if system_id is None or not name:
            continue
        extra = enrichments.get(str(system_id), {}) if isinstance(enrichments, dict) else {}
        sources = extra.get("sources", {}) if isinstance(extra, dict) and isinstance(extra.get("sources"), dict) else {}
        system = {
            "id": system_id,
            "name": name,
            "shortName": text(extra.get("shortName")) or name,
            "manufacturer": text(extra.get("manufacturer")),
            "category": text(extra.get("category")) or "Unknown",
            "generation": text(extra.get("generation")) or "Unknown",
            "releaseYear": integer(extra.get("releaseYear")),
            "endYear": integer(extra.get("endYear")),
            "summary": text(extra.get("summary")),
            "sourceAttribution": {
                "baseProvider": "retroachievements",
                "metadataProvider": text(sources.get("metadataProvider")),
                "metadataRecordUrl": text(sources.get("metadataRecordUrl")),
                "providerIds": dedupe_nonempty(["retroachievements", sources.get("metadataProvider")]),
            },
        }
        systems.append(system)

    return sorted(systems, key=lambda item: item["name"])


def load_games() -> list[dict]:
    manual_enrichments = load_json(RAW_DIR / "game-enrichment.json", default={})
    screenscraper_enrichments = load_json(RAW_DIR / "screenscraper-game-enrichment.json", default={})
    if not isinstance(manual_enrichments, dict):
        manual_enrichments = {}
    if not isinstance(screenscraper_enrichments, dict):
        screenscraper_enrichments = {}
    records: list[dict] = []

    for path in sorted(RAW_DIR.glob("retroachievements-game-list-*.json")):
        payload = load_json(path)
        rows = extract_rows(payload)
        for row in rows:
            provider_id = integer(row.get("ID") or row.get("id"))
            title = text(row.get("Title") or row.get("title") or row.get("Name") or row.get("name"))
            system_id = integer(row.get("ConsoleID") or row.get("consoleId") or row.get("systemId"))
            system_name = text(row.get("ConsoleName") or row.get("consoleName") or row.get("systemName"))
            if provider_id is None or system_id is None or not title:
                continue
            extra = deep_merge(
                manual_enrichments.get(str(provider_id), {}),
                screenscraper_enrichments.get(str(provider_id), {}),
            )
            metadata = extra.get("metadata", {}) if isinstance(extra, dict) and isinstance(extra.get("metadata"), dict) else extra
            sources = extra.get("sources", {}) if isinstance(extra, dict) and isinstance(extra.get("sources"), dict) else {}
            media = extra.get("media", {}) if isinstance(extra, dict) and isinstance(extra.get("media"), dict) else {}
            hashes = row.get("Hashes") or row.get("hashes") or []
            if not isinstance(hashes, list):
                hashes = []
            fallback_icon_url = text(row.get("ImageIcon") or row.get("imageIcon"))
            box_front = normalize_media_entry(media.get("boxFront"), fallback_icon_url, f"Box art for {title}")
            screenshot = normalize_media_entry(media.get("screenshot"), None, f"Screenshot for {title}")
            logo = normalize_media_entry(media.get("logo"), None, f"Logo for {title}")
            video = normalize_media_entry(media.get("video"), None, f"Video preview for {title}")
            provider_ids = dedupe_nonempty(
                [
                    "retroachievements",
                    sources.get("metadataProvider"),
                    sources.get("boxArtProvider"),
                    *(sources.get("providerIds") if isinstance(sources.get("providerIds"), list) else []),
                    box_front.get("provider"),
                    screenshot.get("provider"),
                    logo.get("provider"),
                    video.get("provider"),
                ]
            )
            game = {
                "id": provider_id,
                "slug": f"{system_id}-{slugify(title)}",
                "title": title,
                "sortTitle": normalize_title(title),
                "systemId": system_id,
                "systemName": system_name,
                "releaseYear": integer(metadata.get("releaseYear")),
                "developer": text(metadata.get("developer")),
                "publisher": text(metadata.get("publisher")),
                "genres": [text(item) for item in metadata.get("genres", []) if text(item)] if isinstance(metadata, dict) else [],
                "summary": text(metadata.get("summary")),
                "media": {
                    "boxFront": box_front,
                    "screenshot": screenshot,
                    "logo": logo,
                    "video": video,
                },
                "image": {
                    "iconUrl": box_front["url"],
                    "alt": box_front["alt"],
                },
                "achievements": {
                    "count": integer(row.get("NumAchievements") or row.get("numAchievements")) or 0,
                    "leaderboards": integer(row.get("NumLeaderboards") or row.get("numLeaderboards")) or 0,
                    "points": integer(row.get("Points") or row.get("points")) or 0,
                },
                "hashes": [text(item) for item in hashes if text(item)],
                "forumTopicId": integer(row.get("ForumTopicID") or row.get("forumTopicId")),
                "dateModified": text(row.get("DateModified") or row.get("dateModified")),
                "sourceAttribution": {
                    "baseProvider": "retroachievements",
                    "metadataProvider": text(sources.get("metadataProvider")),
                    "metadataRecordUrl": text(sources.get("metadataRecordUrl")),
                    "boxArtProvider": text(sources.get("boxArtProvider")) or box_front["provider"],
                    "boxArtRecordUrl": text(sources.get("boxArtRecordUrl")),
                    "providerIds": provider_ids,
                },
            }
            records.append(game)

    return records


def build_database(
    catalog_source: str = "auto",
    publish_box_art_root: Path | None = None,
    publish_box_art_base_url: str | None = None,
    batocera_roms_root: Path | None = None,
) -> dict:
    if bool(publish_box_art_root) != bool(publish_box_art_base_url):
        raise ValueError(
            "Portable box-art publishing requires both publish_box_art_root and publish_box_art_base_url."
        )

    retroachievements_catalog = load_retroachievements_catalog() if catalog_source in {"auto", "retroachievements"} else None
    thegamesdb_catalog = load_thegamesdb_catalog() if catalog_source in {"auto", "thegamesdb"} else None
    if retroachievements_catalog:
        systems = [dict(item) for item in retroachievements_catalog.get("systems", []) if isinstance(item, dict)]
        games = [dict(item) for item in retroachievements_catalog.get("games", []) if isinstance(item, dict)]
        catalog_context = {
            "kind": "retroachievements",
            "metadata": retroachievements_catalog.get("metadata", {})
            if isinstance(retroachievements_catalog.get("metadata"), dict)
            else {},
        }
    elif thegamesdb_catalog:
        systems = [dict(item) for item in thegamesdb_catalog.get("systems", []) if isinstance(item, dict)]
        games = [dict(item) for item in thegamesdb_catalog.get("games", []) if isinstance(item, dict)]
        catalog_context = {
            "kind": "thegamesdb",
            "metadata": thegamesdb_catalog.get("metadata", {})
            if isinstance(thegamesdb_catalog.get("metadata"), dict)
            else {},
        }
    else:
        systems = load_systems()
        games = load_games()
        catalog_context = {"kind": "retroachievements", "metadata": {}}

    provider_catalog = load_provider_catalog()
    system_map = {system["id"]: dict(system) for system in systems}
    games_by_system: dict[int, list[dict]] = defaultdict(list)

    for game in games:
        if game["systemId"] not in system_map:
            continue
        system = system_map[game["systemId"]]
        game["searchBlob"] = make_search_blob(
            [
                game["title"],
                system["name"],
                system["shortName"],
                system["manufacturer"],
                game["developer"],
                game["publisher"],
                " ".join(game["genres"]),
                game["summary"],
            ]
        )
        games_by_system[game["systemId"]].append(game)

    normalized_games = sorted(
        [game for game in games if game["systemId"] in system_map],
        key=lambda item: (item["systemName"] or "", item["sortTitle"], item["id"]),
    )

    normalized_systems: list[dict] = []
    genre_counter: Counter[str] = Counter()
    manufacturer_counter: Counter[str] = Counter()
    provider_counter: Counter[str] = Counter()

    for system in systems:
        system_games = sorted(
            games_by_system.get(system["id"], []),
            key=lambda item: (item["sortTitle"], item["id"]),
        )
        total_achievements = sum(
            ((game.get("achievements") or {}).get("count") or 0) for game in system_games
        )
        total_points = sum(((game.get("achievements") or {}).get("points") or 0) for game in system_games)
        total_leaderboards = sum(
            ((game.get("achievements") or {}).get("leaderboards") or 0) for game in system_games
        )
        years = [game["releaseYear"] for game in system_games if game["releaseYear"] is not None]
        top_genres = Counter(
            genre for game in system_games for genre in game["genres"] if genre
        ).most_common(4)

        system["gameCount"] = len(system_games)
        system["totalAchievements"] = total_achievements
        system["totalPoints"] = total_points
        system["totalLeaderboards"] = total_leaderboards
        system["visibleYears"] = {
            "min": min(years) if years else system["releaseYear"],
            "max": max(years) if years else system["endYear"] or system["releaseYear"],
        }
        system["topGenres"] = [name for name, _count in top_genres]
        system["gameIds"] = [game["id"] for game in system_games]
        system["sampleTitles"] = [game["title"] for game in system_games[:4]]
        system["searchBlob"] = make_search_blob(
            [
                system["name"],
                system["shortName"],
                system["manufacturer"],
                system["category"],
                system["generation"],
                system["summary"],
                " ".join(system["topGenres"]),
                " ".join(system["sampleTitles"]),
            ]
        )
        normalized_systems.append(system)

        if system["manufacturer"]:
            manufacturer_counter[system["manufacturer"]] += 1
        for genre in system["topGenres"]:
            genre_counter[genre] += 1

    for game in normalized_games:
        for provider_id in game["sourceAttribution"]["providerIds"]:
            if provider_id in {"retroachievements", "batocera"}:
                continue
            provider_counter[provider_id] += 1

    notes = [
        "Games are currently modeled as system-scoped release entries to avoid incorrect cross-platform merges.",
        "The atlas keeps only the systems listed in the local allowlist so the public bundle stays small and intentional.",
    ]
    sources: list[dict] = []

    if catalog_context["kind"] == "thegamesdb":
        notes.extend(
            [
                "The current catalog source is a curated TheGamesDB import, filtered by a local allowlist of atlas systems.",
                "Remote TheGamesDB cover URLs are preserved directly in the bundle so the site does not need to copy box art into the repository.",
                "TheGamesDB region and country fields can be used to drive atlas-style release geography in the UI.",
            ]
        )
        sources.extend(
            [
                {
                    "name": "TheGamesDB API",
                    "url": "https://api.thegamesdb.net/",
                    "role": "Primary catalog source for systems, games, metadata, and remote cover URLs",
                },
                {
                    "name": "TheGamesDB site",
                    "url": "https://thegamesdb.net/",
                    "role": "Human-readable reference site for the imported game and platform records",
                },
            ]
        )
    else:
        if publish_box_art_root or publish_box_art_base_url:
            raise ValueError(
                "Portable box-art publishing is only needed when a catalog source lacks remote image URLs."
            )
        notes.extend(
            [
                "RetroAchievements-style exports provide the system hierarchy for the checked-in sample data.",
                "If present, generated screenscraper-game-enrichment.json data is merged into the manual game enrichment layer during build.",
                "The normalized output is designed so box art, screenshots, logos, and metadata can be sourced separately without changing the UI contract.",
            ]
        )
        sources.extend(
            [
                {
                    "name": "RetroAchievements All Systems",
                    "url": "https://api-docs.retroachievements.org/v1/get-console-ids.html",
                    "role": "System list source and target raw export shape",
                },
                {
                    "name": "RetroAchievements All System Games",
                    "url": "https://api-docs.retroachievements.org/v1/get-game-list.html",
                    "role": "Per-system game list source and target raw export shape",
                },
                {
                    "name": "IGDB API docs",
                    "url": "https://api-docs.igdb.com/?getting-started=",
                    "role": "Alternative metadata source requiring Client ID and Client secret",
                },
            ]
        )

    if catalog_context["kind"] == "thegamesdb":
        asset_strategy = {
            "mode": "remote-source-url",
            "boxArtBaseUrl": None,
            "boxArtStagingDir": None,
            "publishedGameImageCount": sum(
                1 for game in normalized_games if text(((game.get("media") or {}).get("boxFront") or {}).get("url"))
            ),
            "missingBoxArtCount": sum(
                1 for game in normalized_games if not text(((game.get("media") or {}).get("boxFront") or {}).get("url"))
            ),
            "copiedAssetCount": 0,
        }
        notes.append(
            "This build preserves remote TheGamesDB cover URLs directly, so no local box-art staging directory is needed."
        )
    elif publish_box_art_root and publish_box_art_base_url:
        asset_strategy = stage_batocera_box_art_for_web(
            normalized_games,
            publish_box_art_root,
            publish_box_art_base_url,
            batocera_roms_root=batocera_roms_root,
        )
        notes.append(
            "This build staged box art into a publish directory and rewrote the atlas image URLs to a web base URL for portable hosting."
        )
    else:
        asset_strategy = {
            "mode": "source-linked",
            "boxArtBaseUrl": None,
            "boxArtStagingDir": None,
            "publishedGameImageCount": 0,
            "missingBoxArtCount": 0,
            "copiedAssetCount": 0,
        }

    metadata = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "systemCount": len(normalized_systems),
        "gameCount": len(normalized_games),
        "manufacturers": sorted(manufacturer_counter.keys()),
        "topGenres": [name for name, _count in genre_counter.most_common(8)],
        "providers": provider_catalog["providers"],
        "atlasStrategy": provider_catalog["atlasStrategy"],
        "assetStrategy": asset_strategy,
        "providerUsage": dict(sorted(provider_counter.items())),
        "catalogSource": catalog_context["kind"],
        "notes": notes,
        "sources": sources,
    }

    return {
        "metadata": metadata,
        "systems": normalized_systems,
        "games": normalized_games,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the videogame atlas database."
    )
    parser.add_argument(
        "--publish-box-art-root",
        default=None,
        help="Optional local directory where referenced box art should be copied for upload to a bucket or CDN.",
    )
    parser.add_argument(
        "--publish-box-art-base-url",
        default=None,
        help="Optional web base URL that should serve the staged box art, for example https://cdn.example.com/videogame-atlas/box-art",
    )
    parser.add_argument(
        "--catalog-source",
        choices=("auto", "retroachievements", "thegamesdb"),
        default="auto",
        help="Which normalized catalog source should drive the build when more than one is available.",
    )
    args = parser.parse_args()

    database = build_database(
        catalog_source=args.catalog_source,
        publish_box_art_root=Path(args.publish_box_art_root).expanduser() if args.publish_box_art_root else None,
        publish_box_art_base_url=text(args.publish_box_art_base_url),
    )
    OUTPUT_JSON.write_text(json.dumps(database, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    OUTPUT_JS.write_text(
        "window.VIDEOGAME_ATLAS_DATA = " + json.dumps(database, ensure_ascii=True) + ";\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
