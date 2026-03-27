#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import build_game_data


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "docs"
STATIC_FILES = ("index.html", "app.js", "styles.css")
DEFAULT_MAX_CHUNK_BYTES = 24 * 1024 * 1024
CHUNK_PREFIX = "window.VIDEOGAME_ATLAS_CHUNKS = window.VIDEOGAME_ATLAS_CHUNKS || {};\n"
CHUNK_KEY_PREFIX = "window.VIDEOGAME_ATLAS_CHUNKS["
CHUNK_KEY_MIDDLE = "] = "
CHUNK_SUFFIX = ";\n"


def compact_text(value: object, limit: int) -> str | None:
    cleaned = build_game_data.text(value)
    if not cleaned:
        return None
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1].rstrip() + "..."


def sanitize_asset_strategy(asset_strategy: object) -> dict:
    payload = asset_strategy if isinstance(asset_strategy, dict) else {}
    return {
        "mode": payload.get("mode"),
        "boxArtBaseUrl": payload.get("boxArtBaseUrl"),
        "boxArtStagingDir": "./box-art" if payload.get("copiedAssetCount") else None,
        "publishedGameImageCount": payload.get("publishedGameImageCount", 0),
        "missingBoxArtCount": payload.get("missingBoxArtCount", 0),
        "copiedAssetCount": payload.get("copiedAssetCount", 0),
    }


def sanitize_source_list(sources: object) -> list[dict]:
    if not isinstance(sources, list):
        return []

    sanitized = []
    for source in sources:
        if not isinstance(source, dict):
            continue
        url = build_game_data.text(source.get("url"))
        if url and not url.startswith(("http://", "https://")):
            url = None
        sanitized.append(
            {
                "name": source.get("name"),
                "url": url,
                "role": source.get("role"),
            }
        )
    return sanitized


def compact_system(system: dict) -> dict:
    logo = system.get("logo") if isinstance(system.get("logo"), dict) else {}
    return {
        "id": system.get("id"),
        "key": system.get("key"),
        "name": system.get("name"),
        "shortName": system.get("shortName") or system.get("name"),
        "manufacturer": system.get("manufacturer"),
        "category": system.get("category"),
        "generation": system.get("generation"),
        "releaseYear": system.get("releaseYear"),
        "endYear": system.get("endYear"),
        "summary": compact_text(system.get("summary"), 320),
        "wikiUrl": system.get("wikiUrl"),
        "gameCount": system.get("gameCount"),
        "topGenres": system.get("topGenres") if isinstance(system.get("topGenres"), list) else [],
        "logo": {
            "url": logo.get("url"),
            "provider": logo.get("provider"),
            "kind": logo.get("kind"),
            "alt": logo.get("alt"),
        },
        "sourceAttribution": {
            "metadataProvider": ((system.get("sourceAttribution") or {}).get("metadataProvider")),
        },
    }


def compact_game(game: dict) -> dict:
    media = game.get("media") if isinstance(game.get("media"), dict) else {}
    box_front = media.get("boxFront") if isinstance(media.get("boxFront"), dict) else {}
    release_info = game.get("releaseInfo")
    if not isinstance(release_info, dict):
        release_info = game.get("batocera") if isinstance(game.get("batocera"), dict) else {}
    return {
        "id": game.get("id"),
        "title": game.get("title"),
        "systemId": game.get("systemId"),
        "releaseYear": game.get("releaseYear"),
        "developer": game.get("developer"),
        "publisher": game.get("publisher"),
        "genres": game.get("genres") if isinstance(game.get("genres"), list) else [],
        "summary": compact_text(game.get("summary"), 420),
        "media": {
            "boxFront": {
                "url": box_front.get("url"),
                "provider": box_front.get("provider"),
                "alt": box_front.get("alt"),
            }
        },
        "sourceAttribution": {
            "metadataProvider": ((game.get("sourceAttribution") or {}).get("metadataProvider")),
            "boxArtProvider": ((game.get("sourceAttribution") or {}).get("boxArtProvider")),
            "scraperGameId": ((game.get("sourceAttribution") or {}).get("scraperGameId")),
            "thegamesdbGameId": ((game.get("sourceAttribution") or {}).get("thegamesdbGameId")),
        },
        "releaseInfo": {
            "players": release_info.get("players"),
            "regionCode": release_info.get("regionCode") or release_info.get("region"),
            "language": release_info.get("language"),
            "family": release_info.get("family"),
            "rating": release_info.get("rating"),
            "regionId": release_info.get("regionId"),
            "countryId": release_info.get("countryId"),
        },
    }


def compact_metadata(metadata: dict) -> dict:
    return {
        "generatedAt": metadata.get("generatedAt"),
        "systemCount": metadata.get("systemCount"),
        "gameCount": metadata.get("gameCount"),
        "manufacturers": metadata.get("manufacturers") if isinstance(metadata.get("manufacturers"), list) else [],
        "topGenres": metadata.get("topGenres") if isinstance(metadata.get("topGenres"), list) else [],
        "providers": metadata.get("providers")
        if isinstance(metadata.get("providers"), list)
        else [],
        "atlasStrategy": metadata.get("atlasStrategy") if isinstance(metadata.get("atlasStrategy"), dict) else {},
        "providerUsage": metadata.get("providerUsage") if isinstance(metadata.get("providerUsage"), dict) else {},
        "catalogSource": metadata.get("catalogSource"),
        "notes": metadata.get("notes") if isinstance(metadata.get("notes"), list) else [],
        "sources": sanitize_source_list(metadata.get("sources")),
        "assetStrategy": sanitize_asset_strategy(metadata.get("assetStrategy")),
    }


def estimate_chunk_file_size_bytes(chunk_key: str, payload: dict) -> int:
    script = (
        CHUNK_PREFIX
        + CHUNK_KEY_PREFIX
        + json.dumps(chunk_key)
        + CHUNK_KEY_MIDDLE
        + json.dumps(payload, ensure_ascii=True)
        + CHUNK_SUFFIX
    )
    return len(script.encode("utf-8"))


def split_system_games_into_chunks(
    system_key: str,
    system_id: object,
    games: list[dict],
    max_chunk_bytes: int,
) -> list[dict]:
    if max_chunk_bytes <= 0:
        return [{"key": system_key, "systemId": system_id, "games": games}]

    game_parts: list[list[dict]] = [[]]
    for game in games:
        current_games = game_parts[-1]
        candidate_games = current_games + [game]
        candidate_payload = {
            "key": f"{system_key}-000",
            "systemKey": system_key,
            "systemId": system_id,
            "part": len(game_parts),
            "partCount": None,
            "games": candidate_games,
        }
        if current_games and estimate_chunk_file_size_bytes(candidate_payload["key"], candidate_payload) > max_chunk_bytes:
            game_parts.append([game])
        else:
            game_parts[-1] = candidate_games

    game_parts = [part for part in game_parts if part]
    part_count = len(game_parts)
    chunk_payloads = []
    for index, part_games in enumerate(game_parts, start=1):
        chunk_key = system_key if part_count == 1 else f"{system_key}-{index:03d}"
        chunk_payloads.append(
            {
                "key": chunk_key,
                "systemKey": system_key,
                "systemId": system_id,
                "part": index,
                "partCount": part_count,
                "games": part_games,
            }
        )
    return chunk_payloads


def compact_database_for_publish(database: dict, max_chunk_bytes: int) -> tuple[dict, dict[str, dict]]:
    metadata = database.get("metadata", {}) if isinstance(database.get("metadata"), dict) else {}
    systems = database.get("systems", []) if isinstance(database.get("systems"), list) else []
    games = database.get("games", []) if isinstance(database.get("games"), list) else []

    compact_systems = [compact_system(system) for system in systems if isinstance(system, dict)]
    system_key_by_id = {
        system.get("id"): (system.get("key") or str(system.get("id")))
        for system in compact_systems
    }

    games_by_system_key: dict[str, list[dict]] = {
        (system.get("key") or str(system.get("id"))): []
        for system in compact_systems
    }

    for game in games:
        if not isinstance(game, dict):
            continue
        system_key = system_key_by_id.get(game.get("systemId"))
        if not system_key or system_key not in games_by_system_key:
            continue
        games_by_system_key[system_key].append(compact_game(game))

    chunk_map: dict[str, dict] = {}
    chunk_manifest = []
    for system in compact_systems:
        system_key = system.get("key") or str(system.get("id"))
        split_chunks = split_system_games_into_chunks(
            system_key=system_key,
            system_id=system.get("id"),
            games=games_by_system_key.get(system_key, []),
            max_chunk_bytes=max_chunk_bytes,
        )
        for payload in split_chunks:
            chunk_key = payload["key"]
            chunk_map[chunk_key] = payload
            chunk_manifest.append(
                {
                    "key": chunk_key,
                    "systemId": payload["systemId"],
                    "systemKey": system_key,
                    "path": f"./data/chunks/{chunk_key}.js",
                    "gameCount": len(payload["games"]),
                    "part": payload["part"],
                    "partCount": payload["partCount"],
                    "estimatedBytes": estimate_chunk_file_size_bytes(chunk_key, payload),
                }
            )

    chunk_manifest.sort(key=lambda item: (item["systemKey"], item["part"], item["key"]))

    index_payload = {
        "metadata": {
            **compact_metadata(metadata),
            "chunkManifest": chunk_manifest,
            "chunked": True,
            "maxChunkBytes": max_chunk_bytes,
        },
        "systems": compact_systems,
        "games": [],
        "chunkManifest": chunk_manifest,
    }
    return index_payload, chunk_map


def write_database_bundle(output_dir: Path, index_payload: dict, chunk_map: dict[str, dict]) -> None:
    data_dir = output_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "game-database.json").write_text(
        json.dumps(index_payload, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    (data_dir / "game-database.js").write_text(
        "window.VIDEOGAME_ATLAS_DATA = " + json.dumps(index_payload, ensure_ascii=True) + ";\n",
        encoding="utf-8",
    )
    chunks_dir = data_dir / "chunks"
    if chunks_dir.exists():
        shutil.rmtree(chunks_dir)
    chunks_dir.mkdir(parents=True, exist_ok=True)
    for key, payload in chunk_map.items():
        (chunks_dir / f"{key}.js").write_text(
            CHUNK_PREFIX
            + CHUNK_KEY_PREFIX
            + json.dumps(key)
            + CHUNK_KEY_MIDDLE
            + json.dumps(payload, ensure_ascii=True)
            + CHUNK_SUFFIX,
            encoding="utf-8",
        )


def copy_static_shell(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename in STATIC_FILES:
        shutil.copyfile(ROOT / filename, output_dir / filename)
    (output_dir / ".nojekyll").write_text("\n", encoding="utf-8")


def write_build_info(output_dir: Path, database: dict) -> None:
    asset_strategy = database.get("metadata", {}).get("assetStrategy", {})
    build_info = {
        "catalogSource": database.get("metadata", {}).get("catalogSource"),
        "systemCount": database.get("metadata", {}).get("systemCount"),
        "gameCount": database.get("metadata", {}).get("gameCount"),
        "chunkCount": len(database.get("chunkManifest", []) if isinstance(database.get("chunkManifest"), list) else []),
        "assetStrategy": asset_strategy,
        "openFile": "index.html",
    }
    (output_dir / "build-info.json").write_text(
        json.dumps(build_info, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a portable videogame atlas bundle for local review or static hosting."
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where the publishable atlas bundle should be written.",
    )
    parser.add_argument(
        "--max-chunk-bytes",
        type=int,
        default=DEFAULT_MAX_CHUNK_BYTES,
        help="Maximum size target for each published chunk file, in bytes.",
    )
    parser.add_argument(
        "--catalog-source",
        choices=("auto", "retroachievements", "thegamesdb"),
        default="auto",
        help="Which normalized catalog source should drive the publish bundle when more than one is available.",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir).expanduser().resolve()
    database = build_game_data.build_database(catalog_source=args.catalog_source)
    publish_database, chunk_map = compact_database_for_publish(database, max_chunk_bytes=args.max_chunk_bytes)
    copy_static_shell(output_dir)
    write_database_bundle(output_dir, publish_database, chunk_map)
    write_build_info(output_dir, publish_database)

    asset_strategy = publish_database.get("metadata", {}).get("assetStrategy", {})
    print(f"Wrote publish bundle to {output_dir}")
    if asset_strategy.get("copiedAssetCount", 0):
        print(
            "Staged "
            f"{asset_strategy.get('copiedAssetCount', 0)} box-art files at "
            f"{asset_strategy.get('boxArtStagingDir')}"
        )
    else:
        print("Preserved remote box-art URLs; no local box-art directory was created.")
    print(f"Open {output_dir / 'index.html'}")


if __name__ == "__main__":
    main()
