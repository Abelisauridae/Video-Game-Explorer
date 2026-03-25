#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import shutil
import urllib.parse
import xml.etree.ElementTree as ET
import zlib
from pathlib import Path, PurePosixPath


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
OUTPUT_PATH = RAW_DIR / "batocera-library.json"


def text(value: object) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def integer(value: object) -> int | None:
    cleaned = text(value)
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "unknown"


def normalize_title(title: str) -> str:
    return re.sub(r"^[Tt]he\s+", "", title.strip())


def make_search_blob(parts: list[object]) -> str:
    return " ".join(text(part) or "" for part in parts).strip().lower()


def stable_id(seed: str) -> int:
    return zlib.crc32(seed.encode("utf-8")) & 0x7FFFFFFF


def normalize_relative_catalog_path(value: object) -> str | None:
    cleaned = text(value)
    if not cleaned:
        return None

    raw_path = cleaned.replace("\\", "/")
    if raw_path.startswith("/") or re.match(r"^[A-Za-z]:/", raw_path):
        return None

    parts: list[str] = []
    for part in PurePosixPath(raw_path).parts:
        if part in ("", "."):
            continue
        if part == "..":
            return None
        parts.append(part)

    if not parts:
        return None

    return PurePosixPath(*parts).as_posix()


def split_genres(raw_genre: str | None) -> list[str]:
    if not raw_genre:
        return []
    parts = re.split(r"\s*/\s*|,\s*", raw_genre)
    seen: set[str] = set()
    genres: list[str] = []
    for part in parts:
        cleaned = text(part)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        genres.append(cleaned)
    return genres


def parse_release_year(raw_value: str | None) -> int | None:
    if not raw_value:
        return None
    match = re.match(r"(\d{4})", raw_value)
    return integer(match.group(1)) if match else None


def parse_float(raw_value: str | None) -> float | None:
    if not raw_value:
        return None
    try:
        return float(raw_value)
    except ValueError:
        return None


def format_system_label(raw_label: str | None, fallback: str) -> str:
    if not raw_label:
        raw_label = fallback.replace("-", " ").replace("_", " ")
    pieces = []
    for token in raw_label.split():
        if token.isalpha() and token.isupper():
            pieces.append(token.capitalize())
        else:
            pieces.append(token)
    return " ".join(pieces) or fallback


def parse_system_info(system_dir: Path) -> dict:
    info_path = system_dir / "_info.txt"
    raw_label = None
    wiki_url = None
    if info_path.exists():
        content = info_path.read_text(encoding="utf-8", errors="replace")
        label_match = re.search(r"##\s+SYSTEM\s+(.+?)\s+##", content)
        if label_match:
            raw_label = label_match.group(1).strip()
        wiki_match = re.search(r"For more info:\s*(https?://\S+)", content)
        if wiki_match:
            wiki_url = wiki_match.group(1).strip()

    display_name = format_system_label(raw_label, system_dir.name)
    return {
        "key": system_dir.name,
        "name": display_name,
        "shortName": display_name,
        "wikiUrl": wiki_url,
    }


def materialize_image(
    image_text: str | None,
    system_dir: Path,
    system_key: str,
    copy_root: Path | None,
) -> tuple[str | None, str | None]:
    relative_image_path = normalize_relative_catalog_path(image_text)
    if not relative_image_path:
        return None, None

    absolute_image_path = (system_dir / Path(relative_image_path)).resolve()
    if not absolute_image_path.exists():
        return None, relative_image_path

    if copy_root is not None:
        destination = copy_root / system_key / Path(relative_image_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.exists():
            shutil.copy2(absolute_image_path, destination)

    return None, relative_image_path


def import_system(
    system_dir: Path,
    copy_root: Path | None,
    limit_games: int | None,
) -> tuple[dict, list[dict]]:
    gamelist_path = system_dir / "gamelist.xml"
    tree = ET.parse(gamelist_path)
    root = tree.getroot()

    system_info = parse_system_info(system_dir)
    system_id = stable_id(system_info["key"])
    games: list[dict] = []

    for index, game_node in enumerate(root.findall("game")):
        if limit_games is not None and index >= limit_games:
            break

        rom_path = normalize_relative_catalog_path(game_node.findtext("path"))
        title = (
            text(game_node.findtext("name"))
            or text(PurePosixPath(rom_path or "").stem)
            or "Unknown"
        )
        description = text(game_node.findtext("desc"))
        image_url, relative_image_path = materialize_image(
            text(game_node.findtext("image")),
            system_dir,
            system_info["key"],
            copy_root,
        )
        source_name = text(game_node.attrib.get("source")) or text(game_node.findtext("scrap"))
        provider_id = slugify(source_name or "batocera")
        scraper_game_id = integer(game_node.attrib.get("id"))
        game_id = stable_id(f"{system_info['key']}::{rom_path or title}::{scraper_game_id or ''}")
        release_year = parse_release_year(text(game_node.findtext("releasedate")))
        developer = text(game_node.findtext("developer"))
        publisher = text(game_node.findtext("publisher"))
        genres = split_genres(text(game_node.findtext("genre")))
        rating = parse_float(text(game_node.findtext("rating")))
        players = text(game_node.findtext("players"))
        region = text(game_node.findtext("region"))
        language = text(game_node.findtext("lang"))
        family = text(game_node.findtext("family"))
        md5 = text(game_node.findtext("md5"))
        scraped_at = None
        scrap_node = game_node.find("scrap")
        if scrap_node is not None:
            scraped_at = text(scrap_node.attrib.get("date"))

        game = {
            "id": game_id,
            "slug": f"{system_info['key']}-{slugify(title)}",
            "title": title,
            "sortTitle": normalize_title(title),
            "systemId": system_id,
            "systemName": system_info["name"],
            "releaseYear": release_year,
            "developer": developer,
            "publisher": publisher,
            "genres": genres,
            "summary": description,
            "media": {
                "boxFront": {
                    "url": image_url,
                    "provider": provider_id,
                    "kind": "batocera-image",
                    "alt": f"Box art for {title}",
                },
                "screenshot": {"url": None, "provider": provider_id, "kind": None, "alt": None},
                "logo": {"url": None, "provider": provider_id, "kind": None, "alt": None},
                "video": {"url": None, "provider": provider_id, "kind": None, "alt": None},
            },
            "image": {
                "iconUrl": image_url,
                "alt": f"Box art for {title}",
            },
            "achievements": {
                "count": 0,
                "leaderboards": 0,
                "points": 0,
            },
            "hashes": [md5] if md5 else [],
            "forumTopicId": None,
            "dateModified": scraped_at,
            "sourceAttribution": {
                "baseProvider": "batocera",
                "metadataProvider": provider_id,
                "metadataRecordUrl": f"{system_info['key']}/gamelist.xml",
                "boxArtProvider": provider_id,
                "boxArtRecordUrl": relative_image_path,
                "providerIds": [provider_id] if provider_id else [],
                "scraperGameId": scraper_game_id,
            },
            "batocera": {
                "systemKey": system_info["key"],
                "romPath": rom_path,
                "gamelistPath": f"{system_info['key']}/gamelist.xml",
                "scraperGameId": scraper_game_id,
                "players": players,
                "rating": rating,
                "region": region,
                "language": language,
                "family": family,
                "wikiUrl": system_info["wikiUrl"],
                "relativeImagePath": relative_image_path,
            },
        }
        game["searchBlob"] = make_search_blob(
            [
                title,
                system_info["name"],
                developer,
                publisher,
                " ".join(genres),
                description,
                players,
                region,
                language,
                family,
            ]
        )
        games.append(game)

    years = [game["releaseYear"] for game in games if game["releaseYear"] is not None]
    system = {
        "id": system_id,
        "key": system_info["key"],
        "name": system_info["name"],
        "shortName": system_info["shortName"],
        "manufacturer": None,
        "category": "Imported system",
        "generation": "Unspecified",
        "releaseYear": min(years) if years else None,
        "endYear": max(years) if years else None,
        "summary": (
            f"Imported from Batocera system folder '{system_info['key']}'"
            + (f" with metadata from {games[0]['sourceAttribution']['metadataProvider']}." if games else ".")
        ),
        "sourceAttribution": {
            "baseProvider": "batocera",
            "metadataProvider": games[0]["sourceAttribution"]["metadataProvider"] if games else None,
            "metadataRecordUrl": f"{system_info['key']}/gamelist.xml",
            "providerIds": games[0]["sourceAttribution"]["providerIds"] if games else [],
        },
        "wikiUrl": system_info["wikiUrl"],
    }
    system["searchBlob"] = make_search_blob(
        [system["name"], system["shortName"], system["summary"], system["wikiUrl"], system["key"]]
    )

    return system, games


def discover_system_dirs(roms_root: Path, wanted_systems: set[str] | None) -> list[Path]:
    system_dirs: list[Path] = []
    for child in sorted(roms_root.iterdir(), key=lambda path: path.name.lower()):
        if not child.is_dir():
            continue
        if wanted_systems and child.name not in wanted_systems:
            continue
        if (child / "gamelist.xml").exists():
            system_dirs.append(child)
    return system_dirs


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import Batocera gamelist metadata and box art into the videogame atlas raw data."
    )
    parser.add_argument(
        "--roms-root",
        required=True,
        help="Batocera roms root, for example /path/to/batocera/roms.",
    )
    parser.add_argument(
        "--systems",
        nargs="*",
        default=[],
        help="Optional Batocera system folders to import, for example gb megadrive snes.",
    )
    parser.add_argument(
        "--limit-games-per-system",
        type=int,
        default=None,
        help="Only import the first N games from each system, useful for quick tests.",
    )
    parser.add_argument(
        "--copy-box-art-root",
        default=None,
        help="Optional local directory to copy only referenced box-art images into before building file URLs.",
    )
    args = parser.parse_args()

    roms_root = Path(args.roms_root).expanduser()
    if not roms_root.exists():
        raise SystemExit(f"Roms root does not exist: {roms_root}")

    wanted_systems = set(args.systems) if args.systems else None
    copy_root = Path(args.copy_box_art_root).expanduser() if args.copy_box_art_root else None

    system_dirs = discover_system_dirs(roms_root, wanted_systems)
    systems: list[dict] = []
    games: list[dict] = []

    for system_dir in system_dirs:
        system, imported_games = import_system(system_dir, copy_root, args.limit_games_per_system)
        if not imported_games:
            continue
        systems.append(system)
        games.extend(imported_games)

    output = {
        "metadata": {
            "importedAt": __import__("datetime").datetime.utcnow().isoformat() + "Z",
            "romsRoot": None,
            "copiedBoxArtRoot": None,
            "systemCount": len(systems),
            "gameCount": len(games),
            "imageMode": "copied" if copy_root else "linked",
            "systems": [system["key"] for system in systems],
            "notes": [
                "This import reads Batocera gamelist.xml files and keeps only box-art image references by default.",
                "Video, marquee, thumbnail, and other extra media are intentionally ignored to save space and import time.",
                "Local machine paths are intentionally scrubbed from the saved import so the file is safer to commit.",
            ],
        },
        "systems": systems,
        "games": games,
    }

    OUTPUT_PATH.write_text(json.dumps(output, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    print(f"Wrote {len(systems)} systems and {len(games)} games to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
