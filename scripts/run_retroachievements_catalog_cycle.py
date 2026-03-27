#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
ALLOWLIST_PATH = RAW_DIR / "thegamesdb-system-allowlist.json"
CATALOG_PATH = RAW_DIR / "retroachievements-catalog.json"
LOG_PATH = RAW_DIR / "retroachievements-batch-run-log.json"
FETCH_SCRIPT = ROOT / "scripts" / "fetch_retroachievements_data.py"
BUILD_SCRIPT = ROOT / "scripts" / "build_game_data.py"
PUBLISH_SCRIPT = ROOT / "scripts" / "build_publish_bundle.py"


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


def load_allowlist_keys(path: Path) -> list[str]:
    payload = load_json(path, default={})
    if isinstance(payload, dict):
        systems = payload.get("systems")
    elif isinstance(payload, list):
        systems = payload
    else:
        systems = []
    if not isinstance(systems, list):
        return []
    keys = [text(item.get("key")) for item in systems if isinstance(item, dict)]
    return [key for key in keys if key]


def load_existing_catalog_keys(path: Path) -> set[str]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return set()
    systems = payload.get("systems")
    if not isinstance(systems, list):
        return set()
    return {
        key
        for key in (text(system.get("key")) for system in systems if isinstance(system, dict))
        if key
    }


def determine_missing_keys(allowlist_path: Path, catalog_path: Path) -> list[str]:
    allowlist_keys = load_allowlist_keys(allowlist_path)
    existing_keys = load_existing_catalog_keys(catalog_path)
    return [key for key in allowlist_keys if key not in existing_keys]


def load_log(path: Path) -> list[dict]:
    payload = load_json(path, default=[])
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def write_log(path: Path, entries: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(entries, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def append_log(path: Path, event: dict) -> None:
    entries = load_log(path)
    entries.append(event)
    write_log(path, entries)


def make_event(kind: str, **extra: object) -> dict:
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "kind": kind,
    }
    payload.update(extra)
    return payload


def run_command(command: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(ROOT),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def summarize_result(result: subprocess.CompletedProcess[str]) -> str:
    parts = [result.stdout.strip(), result.stderr.strip()]
    return "\n".join(part for part in parts if part).strip()


def maybe_sleep(seconds: float, dry_run: bool, reason: str) -> None:
    if seconds <= 0:
        return
    print(reason)
    if not dry_run:
        time.sleep(seconds)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch missing RetroAchievements systems in batches over time and rebuild the atlas after each successful batch."
    )
    parser.add_argument("--batch-size", type=int, default=1, help="How many missing systems to fetch in each batch.")
    parser.add_argument("--pause-minutes", type=float, default=5.0, help="Cooldown between successful batches.")
    parser.add_argument("--max-hours", type=float, default=6.0, help="Maximum wall-clock runtime for the batch cycle.")
    parser.add_argument("--max-batches", type=int, default=None, help="Optional hard cap on the number of successful batches.")
    parser.add_argument("--page-size", type=int, default=500, help="Page size for the RetroAchievements game-list endpoint.")
    parser.add_argument("--list-delay-seconds", type=float, default=0.5, help="Pause between paginated system game-list requests.")
    parser.add_argument("--detail-delay-seconds", type=float, default=0.15, help="Pause between per-game summary requests.")
    parser.add_argument("--max-game-detail-requests", type=int, default=None, help="Optional cap on new per-game summary requests during each fetch batch.")
    parser.add_argument("--skip-dist", action="store_true", help="Only rebuild docs/, not the local dist/ preview bundle.")
    parser.add_argument("--log-path", default=str(LOG_PATH), help="Path to the local batch-run log JSON.")
    parser.add_argument("--dry-run", action="store_true", help="Print the planned batches and exit without network calls or file changes.")
    args = parser.parse_args()

    api_key = os.environ.get("RETROACHIEVEMENTS_WEB_API_KEY", "").strip()
    if not args.dry_run and not api_key:
        raise SystemExit("Missing required environment variable: RETROACHIEVEMENTS_WEB_API_KEY")

    batch_size = max(args.batch_size, 1)
    env = dict(os.environ)
    log_path = Path(args.log_path).expanduser()
    started_at = time.monotonic()
    successful_batches = 0

    if not args.dry_run:
        append_log(
            log_path,
            make_event(
                "cycle_started",
                batchSize=batch_size,
                pauseMinutes=args.pause_minutes,
                maxHours=args.max_hours,
                maxBatches=args.max_batches,
                skipDist=args.skip_dist,
                dryRun=args.dry_run,
            ),
        )

    while True:
        elapsed_hours = (time.monotonic() - started_at) / 3600
        if elapsed_hours >= args.max_hours:
            if not args.dry_run:
                append_log(log_path, make_event("cycle_stopped", reason="max_hours_reached"))
            print("Stopping because the configured max-hours window has been reached.")
            break

        if args.max_batches is not None and successful_batches >= args.max_batches:
            if not args.dry_run:
                append_log(log_path, make_event("cycle_stopped", reason="max_batches_reached"))
            print("Stopping because the configured max-batches limit has been reached.")
            break

        missing_keys_before = determine_missing_keys(ALLOWLIST_PATH, CATALOG_PATH)
        if not missing_keys_before:
            if not args.dry_run:
                append_log(log_path, make_event("cycle_completed", reason="all_systems_present"))
            print("All allowlist systems are already present in the local RetroAchievements catalog.")
            break

        batch_keys = missing_keys_before[:batch_size]
        if not args.dry_run:
            append_log(log_path, make_event("batch_planned", systems=batch_keys))
        print(f"Next batch: {', '.join(batch_keys)}")

        if args.dry_run:
            print("Dry run only. No network calls were made.")
            break

        fetch_command = [
            sys.executable,
            str(FETCH_SCRIPT),
            "--systems",
            *batch_keys,
            "--page-size",
            str(max(args.page_size, 1)),
            "--list-delay-seconds",
            str(max(args.list_delay_seconds, 0)),
            "--detail-delay-seconds",
            str(max(args.detail_delay_seconds, 0)),
        ]
        if args.max_game_detail_requests is not None:
            fetch_command.extend(["--max-game-detail-requests", str(args.max_game_detail_requests)])

        fetch_result = run_command(fetch_command, env)
        fetch_summary = summarize_result(fetch_result)
        append_log(
            log_path,
            make_event(
                "fetch_finished",
                systems=batch_keys,
                returnCode=fetch_result.returncode,
                summary=fetch_summary,
            ),
        )
        if fetch_summary:
            print(fetch_summary)

        if fetch_result.returncode != 0:
            print("Stopping because the fetch step failed.")
            append_log(log_path, make_event("cycle_stopped", reason="fetch_failed"))
            break

        missing_keys_after = determine_missing_keys(ALLOWLIST_PATH, CATALOG_PATH)
        fetched_keys = [key for key in batch_keys if key not in missing_keys_after]
        if not fetched_keys:
            print("The fetch step finished without adding any new systems. Stopping to avoid a tight loop.")
            append_log(log_path, make_event("cycle_stopped", reason="no_progress"))
            break

        for command, label in (
            ([sys.executable, str(BUILD_SCRIPT), "--catalog-source", "retroachievements"], "build_game_data"),
            ([sys.executable, str(PUBLISH_SCRIPT), "--catalog-source", "retroachievements"], "build_publish_bundle_docs"),
            (
                [
                    sys.executable,
                    str(PUBLISH_SCRIPT),
                    "--catalog-source",
                    "retroachievements",
                    "--output-dir",
                    str(ROOT / "dist"),
                ],
                "build_publish_bundle_dist",
            ),
        ):
            if args.skip_dist and label == "build_publish_bundle_dist":
                continue
            result = run_command(command, env)
            summary = summarize_result(result)
            append_log(
                log_path,
                make_event(
                    "command_finished",
                    label=label,
                    returnCode=result.returncode,
                    summary=summary,
                ),
            )
            if summary:
                print(summary)
            if result.returncode != 0:
                print(f"Stopping because {label} failed.")
                append_log(log_path, make_event("cycle_stopped", reason=f"{label}_failed"))
                return

        successful_batches += 1
        append_log(
            log_path,
            make_event(
                "batch_succeeded",
                systems=fetched_keys,
                requestedSystems=batch_keys,
                successfulBatches=successful_batches,
                remainingSystems=missing_keys_after,
            ),
        )

        maybe_sleep(
            args.pause_minutes * 60,
            dry_run=False,
            reason=f"Batch complete. Waiting {args.pause_minutes:.1f} minutes before the next batch.",
        )


if __name__ == "__main__":
    main()
