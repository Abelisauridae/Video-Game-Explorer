# Videogame Atlas

Static videogame atlas built from a curated [TheGamesDB](https://thegamesdb.net/) system allowlist.

The current public-first workflow is intentionally simple:

- pick the systems you want in the atlas
- fetch their games and remote box art from TheGamesDB
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

Then put your TheGamesDB API key in `.env.local`:

```bash
THEGAMESDB_API_KEY="your_thegamesdb_api_key"
```

## Fetch the catalog

```bash
set -a
source videogame-atlas/.env.local
set +a
python3 videogame-atlas/scripts/fetch_thegamesdb_data.py
```

That writes:

- `data/raw/thegamesdb-catalog.json`

The fetched catalog keeps remote TheGamesDB cover URLs instead of copying images into the repo.

## Build the local data bundle

```bash
python3 videogame-atlas/scripts/build_game_data.py
```

That writes:

- `data/game-database.json`
- `data/game-database.js`

## Build the publish bundle

```bash
python3 videogame-atlas/scripts/build_publish_bundle.py
```

That writes a GitHub Pages-friendly bundle to `docs/`:

- `docs/index.html`
- `docs/app.js`
- `docs/styles.css`
- `docs/data/game-database.json`
- `docs/data/game-database.js`
- `docs/data/chunks/<system>.js`

The publish build now uses a small index file plus per-system chunk files, which keeps individual GitHub-tracked files comfortably smaller than a monolithic all-games bundle.

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
- If you later want more systems, add them to the allowlist and refetch instead of importing an entire external library.
