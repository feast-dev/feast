# Feast AI DJ

A personal AI DJ built from open-source parts:

- **Navidrome** plays the music in your browser.
- **Feast** stores the catalog (with CLaMP 3 and spectral vectors), every play, and your taste clusters.
- **CLaMP 3** is a music-text model. It reads a whole track (up to 640 s) as one sequence and puts it in the same space as text, so a mood written in words can find songs.
- **A small local LLM** (`granite4.2:3b` on Ollama) reads your taste and searches the catalog through the standalone `feast mcp` server, then saves a playlist in Navidrome.

```
Navidrome --scrobbles--> bridge --/push--> Feast (Milvus Lite)
                                              ^   |
                       taste job (k-means) ---'   |
                                                  v
                                       feast mcp (standalone, :6590/mcp)
                                                  |
                      Navidrome <--playlist-- AI DJ agent (Ollama)
```

## Quick start

You need macOS or Linux with [uv](https://docs.astral.sh/uv/), [Ollama](https://ollama.com), [Navidrome](https://www.navidrome.org), ffmpeg, git and curl. On macOS: `brew install uv ollama navidrome ffmpeg`.

From a clone of the Feast repo:

```bash
cd examples/ai-dj
./setup.sh   # one time: the LLM and CLaMP 3 models (~4 GB) and 12 songs
./start.sh   # starts every service and opens the UI at http://127.0.0.1:6600
```

`setup.sh` fetches 12 songs so the first run is quick. For the 1,000-song catalog from the blog post, run `TRACKS=1000 ./setup.sh` (about 1 GB of music and 10-20 more minutes to embed it). Both scripts are safe to run again. `start.sh` writes logs to `data/logs/`; press Ctrl+C to stop everything it started.

## Use the UI


1. **Give the DJ some history.** Press **Simulate a listener** (it plays the Hip-Hop and Electronic songs and skips Rock), or ask for any playlist and play a few songs.
2. **Press Update taste.** The clustering job reads your plays from Feast and writes taste clusters back. They show up on the left.
3. **Ask for music**, for example "hype me up for a workout", and press **Spin it**. The panel under the box shows every call the agent makes to Feast through `feast mcp`.
4. **Listen.** The playlist plays in the browser and is saved in Navidrome too. A song played past half way counts as a play. Skipping it earlier counts as a skip. Both go Navidrome -> bridge -> Feast, so press **Update taste** again and watch your taste move.

You can also play songs in Navidrome itself at http://127.0.0.1:4533 (the login is in `config/.env`). Those plays reach Feast the same way.

## What runs where

| Piece | File | Port |
|---|---|---|
| Web UI | `dj/ui.py`, `dj/static/index.html` | 6600 |
| CLaMP 3 text embeddings (own env: it pins transformers 4.40) | `clamp3/server.py` | 6580 |
| Feast feature server with CLaMP 3 query embedder | `dj/server.py` | 6566 |
| Standalone Feast MCP server (`features_*` tools), proxies to the feature server | `feast_mcp.yaml` | 6590 |
| ListenBrainz-compatible bridge (Navidrome plays to Feast) | `dj/bridge.py` | 6570 |
| Navidrome | `config/navidrome.toml` | 4533 |
| Taste clustering job | `dj/taste.py` | - |
| AI DJ agent | `dj/agent.py` | - |

## Step by step

This is what `setup.sh` and `start.sh` do, if you want to run the pieces yourself.

```bash
uv sync
ollama pull granite4.2:3b

# 1. Music: pick FMA small tracks spread over 8 genres and fetch only those MP3s
#    (range requests into the big FMA zips). Use --tracks 1000 for the full catalog.
uv run python scripts/prepare_catalog.py --tracks 12

# 2. Features: CLaMP 3 vectors (separate env, ~2 GB of models), then spectral vectors.
./clamp3/setup.sh
(cd clamp3 && .venv/bin/python embed_catalog.py)
uv run python scripts/build_features.py

# 3. Feast: register and load into the Milvus Lite online store.
(cd feature_repo && uv run feast apply && uv run feast materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%S))

# 4. Navidrome config + a local login saved in config/.env
uv run python scripts/setup_navidrome.py
```

Start the services, each in its own terminal (plus Ollama):

```bash
(cd clamp3 && .venv/bin/python server.py)
uv run python -m dj.server
uv run feast mcp --config feast_mcp.yaml
uv run python -m dj.bridge
navidrome --configfile config/navidrome.toml
```

Then the first-run Navidrome setup (creates the login, links ListenBrainz to the bridge, scans the music), and the DJ from the command line or the UI:

```bash
uv run python scripts/init_navidrome.py

uv run python scripts/simulate_plays.py --likes Hip-Hop Electronic --skips Rock
uv run python -m dj.taste
uv run python -m dj.agent "hype me up for a workout" -v

uv run python -m dj.ui   # http://127.0.0.1:6600
```

The playlist shows up in Navidrome as **AI DJ: ...**. To check every piece at once:

```bash
uv run python scripts/smoke_test.py
```

## Why CLaMP 3, not CLAP

We first used CLAP (`laion/clap-htsat-unfused`). It was trained mostly on general sounds, and it only sees 10 seconds at a time. We compared it on the 1,000 tracks with a simple genre test (8 balanced genres, chance = 12.5%):

| Method | Same-genre neighbors (top 10) | Genre text search (top 20) |
|---|---|---|
| CLAP, middle 10 s | 0.46 | 0.31 |
| CLAP, mean of 3 x 10 s | 0.49 | 0.31 |
| CLAP fused model, whole clip | 0.43 | 0.28 |
| **CLaMP 3, whole clip** | **0.51** | **0.53** |
| Spectral vector (Fourier stats) | 0.27 | - |

CLaMP 3 turns each 5-second window into a MERT vector, then a transformer reads all windows of the track together before pooling, so one embedding covers the whole song. MERT is CC BY-NC 4.0 (non-commercial); CLaMP 3 is MIT.

## Things we learned about Feast

- **One vector field per feature view**, and one search reads one feature view. So CLaMP 3 and spectral vectors live in two views, and the tags are copied into both so filters work.
- **The Milvus store has one `embedding_dim` for the whole project.** Both vectors are 768 numbers, to match CLaMP 3.
- **Numeric arrays become Milvus vectors**, so play history is stored as JSON strings.
- **`enable_openai_compatible_store: true`** stores numbers as numbers, so `year > 2012` filters work.
- **The standalone `feast mcp` server** (`feast[mcp-server]`) holds no data and proxies to the feature server. It exposes 9 `features_*` tools, including `features_materialize`, so lock Feast down with RBAC outside a demo.
- **Its search tools don't take `filters` yet**, so the demo searches by mood only. The feature server's endpoints do support filters (genre, `year > 2012`, ...).
- **Known issues (Feast 0.68.0, Milvus):** search results come back in the right `track_id` order, but titles, genres and distances can be attached to other rows. And the OpenAI-compatible search's `score` goes up as matches get worse. So the DJ uses only the result order and the track ids.

## Music credits

Audio from the [Free Music Archive dataset](https://github.com/mdeff/fma) (Defferrard et al., ISMIR 2017). Metadata is CC BY 4.0. Each track keeps the Creative Commons license its artist chose.
