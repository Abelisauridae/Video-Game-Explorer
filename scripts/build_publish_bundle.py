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


def compact_database_for_publish(database: dict) -> tuple[dict, dict[str, dict]]:
    metadata = database.get("metadata", {}) if isinstance(database.get("metadata"), dict) else {}
    systems = database.get("systems", []) if isinstance(database.get("systems"), list) else []
    games = database.get("games", []) if isinstance(database.get("games"), list) else []

    compact_systems = [compact_system(system) for system in systems if isinstance(system, dict)]
    system_key_by_id = {
        system.get("id"): (system.get("key") or str(system.get("id")))
        for system in compact_systems
    }

    chunk_map: dict[str, dict] = {}
    for system in compact_systems:
        key = system.get("key") or str(system.get("id"))
        chunk_map[key] = {
            "key": key,
            "systemId": system.get("id"),
            "games": [],
        }

    for game in games:
        if not isinstance(game, dict):
            continue
        system_key = system_key_by_id.get(game.get("systemId"))
        if not system_key or system_key not in chunk_map:
            continue
        chunk_map[system_key]["games"].append(compact_game(game))

    chunk_manifest = [
        {
            "key": key,
            "systemId": chunk["systemId"],
            "path": f"./data/chunks/{key}.js",
            "gameCount": len(chunk["games"]),
        }
        for key, chunk in chunk_map.items()
    ]

    index_payload = {
        "metadata": {
            **compact_metadata(metadata),
            "chunkManifest": chunk_manifest,
            "chunked": True,
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
            "window.VIDEOGAME_ATLAS_CHUNKS = window.VIDEOGAME_ATLAS_CHUNKS || {};\n"
            f"window.VIDEOGAME_ATLAS_CHUNKS[{json.dumps(key)}] = "
            + json.dumps(payload, ensure_ascii=True)
            + ";\n",
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
    args = parser.parse_args()

    output_dir = Path(args.output_dir).expanduser().resolve()
    database = build_game_data.build_database()
    publish_database, chunk_map = compact_database_for_publish(database)
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
