# Videogame Atlas

Static videogame atlas built from a curated system allowlist and a pluggable catalog source.

The current public-first workflow is intentionally simple:

- pick the systems you want in the atlas
- fetch their games and remote art from the active catalog provider
- build a lightweight static bundle for GitHub Pages

The atlas does not need local ROM folders or copied cover-art dumps.

## Open the app

Open `index.html` in a browser for the local build, or open `docs/index.html` after generating a publish bundle.

## Configure the atlas systems

Edit `data/raw/thegamesdb-system-allowlist.json`.

Each entry defines one system that should appear in the atlas:

- `platformId`: TheGamesDB platform id
- `key`: short stable slug used in local data
- `name` and `shortName`: display labels
- `manufacturer`
- `category`
- `generation`
- `releaseYear`
- `endYear`
- `summary`

That allowlist is the main size-control lever for the project.

## Credentials

Copy the env template:

```bash
cp videogame-atlas/.env.example videogame-atlas/.env.local
```

Then put the provider keys you plan to use in `.env.local`:

```bash
THEGAMESDB_API_KEY="your_thegamesdb_api_key"
RETROACHIEVEMENTS_WEB_API_KEY="your_retroachievements_web_api_key"
```

## Fetch TheGamesDB

```bash
set -a
source videogame-atlas/.env.local
set +a
python3 videogame-atlas/scripts/fetch_thegamesdb_data.py
```

That writes:

- `data/raw/thegamesdb-catalog.json`

The fetched catalog keeps remote TheGamesDB cover URLs instead of copying images into the repo.

For incremental growth, you can also fetch only specific systems and merge them into the existing local catalog:

```bash
python3 videogame-atlas/scripts/fetch_thegamesdb_data.py --systems nes gba
```

Or fetch only the allowlist systems that are not already present in the local catalog:

```bash
python3 videogame-atlas/scripts/fetch_thegamesdb_data.py --missing-only
```

The fetcher also keeps a local TheGamesDB lookup cache so repeated refreshes do not need to re-download genre, developer, and publisher tables every time.

For a longer unattended run, use the batch-cycle helper. It fetches a few missing systems at a time, rebuilds the atlas after each successful batch, and sleeps between batches so TheGamesDB is less likely to rate-limit the session:

```bash
python3 videogame-atlas/scripts/run_thegamesdb_catalog_cycle.py --batch-size 1 --pause-minutes 20 --rate-limit-pause-minutes 45 --max-hours 6
```

That command:

- works through the allowlist in order
- skips systems already present in the local catalog
- rebuilds `docs/` after each successful fetch batch
- also refreshes `dist/` unless you pass `--skip-dist`
- writes a local run log to `data/raw/thegamesdb-batch-run-log.json`

If you want a quick no-network preview of what the next batch would be, add `--dry-run`.

## Fetch RetroAchievements

```bash
set -a
source videogame-atlas/.env.local
set +a
python3 videogame-atlas/scripts/fetch_retroachievements_data.py --systems megadrive snes gb
```

That writes:

- `data/raw/retroachievements-catalog.json`
- `data/raw/retroachievements-game-detail-cache.json`

The RetroAchievements fetcher uses the same system allowlist, pages through each system's game list, and caches per-game summary responses locally so repeated runs only need to fill in missing details.

## Build the local data bundle

```bash
python3 videogame-atlas/scripts/build_game_data.py --catalog-source auto
```

That writes:

- `data/game-database.json`
- `data/game-database.js`

## Build the publish bundle

```bash
python3 videogame-atlas/scripts/build_publish_bundle.py --catalog-source auto
```

That writes a GitHub Pages-friendly bundle to `docs/`:

- `docs/index.html`
- `docs/app.js`
- `docs/styles.css`
- `docs/data/game-database.json`
- `docs/data/game-database.js`
- `docs/data/chunks/<system>.js`

The publish build now uses a small index file plus per-system chunk files, which keeps individual GitHub-tracked files comfortably smaller than a monolithic all-games bundle.

If a single system would still blow past GitHub's file-size comfort zone, the publish builder automatically splits it into numbered subchunks such as `snes-001.js`, `snes-002.js`, and so on.

No local `box-art/` directory is created in the publish build because the atlas uses remote TheGamesDB cover URLs directly.

## Atlas-specific release geography

Each game record keeps:

- remote cover art URL
- developer and publisher
- genre list
- release year
- release-region metadata derived from TheGamesDB `region_id` and `country_id`

The UI then uses that metadata to show:

- a release flag badge
- a release-region summary
- an approximate world-map marker

Current display rules:

- North America = USA flag
- Japan = Japanese flag
- Europe = EU flag
- UK = Union Jack
- Worldwide = UN flag

## Notes

- Games are modeled as system-scoped release entries so ports with the same title do not collapse into one record.
- The publish bundle is meant for static hosting and should stay much smaller than the old local art-dump approach.
- Local raw fetch artifacts and local full-build outputs are ignored by git so the publishable repo stays clean.
- The recommended way to expand the atlas is to add systems to the allowlist and fetch them incrementally instead of doing giant all-at-once refreshes.
- If you later want more systems, add them to the allowlist and refetch instead of importing an entire external library.
