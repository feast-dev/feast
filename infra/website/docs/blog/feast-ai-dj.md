---
title: "Build Your Own AI DJ with Feast and the Feast MCP Server"
description: "Store a music catalog, play history and listener taste in Feast, then let a small local LLM build playlists through the standalone Feast MCP server."
date: 2026-10-21
authors: ["Chaitanya Patel"]
---

# Build Your Own AI DJ with Feast and the Feast MCP Server

Spotify has an AI DJ. It knows what you played last night, it talks to you, and it picks what comes next.

Can you build one yourself, with only open-source parts, on a laptop?

Yes. In this post we build one where:

- **[Navidrome](https://www.navidrome.org)** plays the music in your browser.
- **Feast** stores the whole catalog with audio vectors, every play, and each listener's taste.
- **A 3B model on Ollama** reads your taste and searches the catalog through the new **standalone Feast MCP server** (`feast mcp`). Then it saves a playlist in Navidrome.

Everything runs locally. The LLM is `granite4.2:3b`, a 2.2 GB download.

## What we are building

![AI DJ architecture: Navidrome scrobbles through a bridge into Feast with Milvus Lite, a taste job feeds Feast, feast mcp serves an AI DJ agent on Ollama, and playlists return to Navidrome](/images/blog/feast-ai-dj-architecture.png)

One round of the DJ looks like this:

1. **Listen.** You play songs in Navidrome. Every play and every skip lands in Feast.
2. **Learn your taste.** A small job groups your recent songs into clusters and labels them, for example "energetic, electronic beats".
3. **Pick a vibe.** You ask for "something for a workout". The LLM reads your taste through MCP and writes what the music should sound like.
4. **Search.** The LLM searches the catalog by that description, through MCP.
5. **Play.** A playlist shows up in Navidrome.

## The stack

| Part | Tool | Why |
|---|---|---|
| Music | [FMA small](https://github.com/mdeff/fma) (1,000 of its 8,000 tracks) | Free, Creative Commons, has genre tags |
| Player | Navidrome | Web player, Subsonic API, ListenBrainz scrobbling |
| Music vectors | [CLaMP 3](https://github.com/sanderwood/clamp3) | Music and text in one space; reads the whole track |
| Sound vectors | Mel spectrogram (Fourier transform) stats | How a song *sounds* |
| Feature store | Feast with a Milvus Lite online store | Vector search, push, MCP |
| MCP server | `feast mcp` (standalone) | Gives the agent Feast tools |
| Agent | `granite4.2:3b` on Ollama | Small, local, good at tool calls |

## Step 1: Get the music

FMA small has 8,000 clips of 30 seconds in 8 genres. We take 125 per genre, so 1,000 tracks.

The zip is 7.2 GB, but you don't need all of it. A zip file keeps its file list at the end, and the server supports range requests. So [`remotezip`](https://pypi.org/project/remotezip/) can fetch just the files we want:

```python
from remotezip import RemoteZip

with RemoteZip("https://os.unil.cloud.switch.ch/fma/fma_small.zip") as z:
    for track_id in catalog["track_id"]:
        name = f"{track_id:06d}"
        (MUSIC / f"{name}.mp3").write_bytes(z.read(f"fma_small/{name[:3]}/{name}.mp3"))
```

That is about 1 GB instead of 7.2. Point Navidrome's music folder at it and let it scan.

## Step 2: Turn every song into vectors

We give each track two vectors.

### A music vector with CLaMP 3

We first tried CLAP (`laion/clap-htsat-unfused`). It is a good audio-text model, but it was trained mostly on general sounds (speech, animals, street noise), and it only looks at 10 seconds at a time.

[CLaMP 3](https://arxiv.org/abs/2502.10362) is built for music:

- It cuts the track into 5-second windows and turns each one into a vector with [MERT](https://huggingface.co/m-a-p/MERT-v1-95M), a music model.
- A transformer then reads **all the windows of the track together** (up to 640 seconds) and returns one 768-number embedding for the whole song.
- Text goes into the same space. So "calm acoustic guitar" lands near songs that sound like that.

We checked this instead of guessing. On our 1,000 tracks we ran a simple genre test (8 balanced genres, so chance is 12.5%):

| Method | Same-genre neighbors (top 10) | Genre text search (top 20) |
|---|---|---|
| CLAP, middle 10 s | 0.46 | 0.31 |
| CLAP, mean of three 10 s windows | 0.49 | 0.31 |
| CLAP fused model, whole clip | 0.43 | 0.28 |
| **CLaMP 3, whole clip** | **0.51** | **0.53** |

For text search, CLaMP 3 is a big step up.

CLaMP 3 pins `transformers==4.40`, so it lives in its own small Python environment. A tiny FastAPI service exposes `POST /embed` for text. Feast calls it when someone searches (Step 5).

> **License note:** MERT is CC BY-NC 4.0 (non-commercial). CLaMP 3 itself is MIT. That is fine for a demo; check it before you ship anything.

### A sound vector from the Fourier transform

The second vector describes how a song *sounds*: bright or dark, busy or calm.

A Fourier transform splits short frames of sound into frequencies. A mel spectrogram groups those into bands the way our ears hear them. We use 384 bands and keep the mean and the spread of each band over time:

```python
mel = librosa.feature.melspectrogram(y=y, sr=48_000, n_fft=8192, hop_length=2048, n_mels=384)
db = librosa.power_to_db(mel)
vector = np.concatenate([db.mean(axis=1), db.std(axis=1)])  # 384 + 384 = 768 numbers
```

Why 768 and not 512? See the box in the next step.

## Step 3: Define the features in Feast

Two entities, `track` and `user`, and four feature views:

| Feature view | Entity | What it holds | Fed by |
|---|---|---|---|
| `track_clamp3` | track | CLaMP 3 vector + title, artist, genre, year | batch (Parquet) |
| `track_spectral` | track | sound vector + the same tags | batch (Parquet) |
| `user_listening` | user | last track, recent tracks, skipped tracks | push |
| `user_taste` | user | taste summary, top genres | push |

Here is the music view:

```python
track_clamp3 = FeatureView(
    name="track_clamp3",
    entities=[track],
    schema=[
        Field(
            name="clamp3_vector",
            dtype=Array(Float32),
            vector_index=True,
            vector_search_metric="COSINE",
            vector_length=768,
        ),
        Field(name="title", dtype=String),
        Field(name="artist", dtype=String),
        Field(name="genre", dtype=String),
        Field(name="year", dtype=Int64),
    ],
    source=FileSource(path="data/track_clamp3.parquet", timestamp_field="event_timestamp"),
    ttl=timedelta(days=3650),
)
```

And the online store, [Milvus Lite](https://milvus.io/docs/milvus_lite.md), which is just a local file:

```yaml
online_store:
  type: milvus
  path: ../data/online_store.db
  vector_enabled: true
  embedding_dim: 768
  index_type: FLAT
  metric_type: COSINE
  enable_openai_compatible_store: true
```

> **Three things we learned the hard way**
>
> 1. **One vector per feature view.** Feast allows one `vector_index=True` field per feature view, and one search reads one feature view. So the CLaMP 3 vector and the sound vector live in two views.
> 2. **One `embedding_dim` per project in the Milvus store.** Every vector field gets the same size. That is why the sound vector is 768 numbers too.
> 3. **Numeric arrays become Milvus vectors.** In the Milvus store, even an `Array(Int64)` turns into a vector field. So the play history is stored as a JSON string, like `"[64537, 155066]"`.

Then:

```bash
feast apply
feast materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%S)
```

## Step 4: Capture every play

Navidrome can scrobble to ListenBrainz, and it lets you change the ListenBrainz address. So we point it at our own tiny service:

```toml
[ListenBrainz]
Enabled = true
BaseURL = "http://127.0.0.1:6570/1/"
```

The bridge answers two ListenBrainz calls:

- `GET /1/validate-token`: says yes, so Navidrome can link the account.
- `POST /1/submit-listens`: receives the plays.

Navidrome sends `playing_now` when a song starts and `single` when it was really listened to. A song that started but never got a `single` before the next one started is a **skip**.

One catch: Navidrome sends `single` events in batches, often after the next song has started. So when a late `single` arrives, the bridge takes that song back out of the skipped list.

Each change goes to Feast with one push:

```python
push(
    "user_listening_push",
    {
        "user_id": [user_id],
        "event_timestamp": [now],
        "last_track_id": [recent[-1]],
        "recent_track_ids": [json.dumps(recent[-50:])],
        "skipped_track_ids": [json.dumps(skipped[-50:])],
    },
    to="online_and_offline",
)
```

`online_and_offline` matters. The agent reads the online copy, and the offline copy is history you can train a real recommender on later.

## Step 5: Learn the listener's taste

A small job turns recent plays into taste clusters:

1. Read `recent_track_ids` from `user_listening`.
2. Read the CLaMP 3 vector and genre of each played song from `track_clamp3`.
3. Run k-means (up to 3 clusters).
4. Label each cluster.
5. Push the result to `user_taste`.

For the labels we use CLaMP 3 again, as a zero-shot tagger. We embed about 20 sound words ("chill", "energetic", "rap vocals", "electronic beats", "acoustic guitar", ...) and pick the closest ones to each cluster center:

```python
words = embed_text(SOUND_WORDS)  # CLaMP 3 text vectors

def describe(center, words, n=2):
    scores = words @ (center / np.linalg.norm(center))
    return ", ".join(SOUND_WORDS[i] for i in np.argsort(-scores)[:n])
```

For a listener who played Hip-Hop and Electronic and skipped Rock, the summary came out as:

```
energetic, electronic beats Electronic (71%), rap vocals, aggressive Hip-Hop (14%), rap vocals, energetic Hip-Hop (14%)
```

The agent reads this text, so it can search with words.

## Step 6: Give the agent Feast through the standalone MCP server

### Feast embeds the query with CLaMP 3

The feature server has an [OpenAI-compatible vector store search](https://docs.feast.dev/reference/alpha-vector-database): `POST /v1/vector_stores/{id}/search` takes a text query and turns it into a vector on the server.

The built-in embedder is Sentence Transformers. We swap in CLaMP 3 by passing our own `EmbeddingProvider` (it only needs `embed` and `aembed`):

```python
class Clamp3TextEmbedder:
    def embed(self, texts):
        return httpx.post(f"{CLAMP3_URL}/embed", json={"texts": texts}).json()["embeddings"]

    async def aembed(self, texts):
        async with httpx.AsyncClient() as client:
            r = await client.post(f"{CLAMP3_URL}/embed", json={"texts": texts})
        return r.json()["embeddings"]
```

`feast serve` builds its own `FeatureStore`, so we start the server from a few lines of Python instead:

```python
store = FeatureStore(repo_path="feature_repo", embedding_provider=Clamp3TextEmbedder())
uvicorn.run(get_app(store), host="127.0.0.1", port=6566)
```

Now "fast energetic beats with heavy drums" becomes a CLaMP 3 vector inside Feast. The agent never touches vectors.

### Start `feast mcp`

The standalone MCP server ships with Feast. Install the extra:

```bash
pip install "feast[milvus,mcp-server]"
```

It holds no data of its own. It turns MCP tool calls into calls to a running feature server:

```yaml
# feast_mcp.yaml
server:
  transport: http
  host: 127.0.0.1
  port: 6590

features:
  url: http://127.0.0.1:6566
```

```bash
feast mcp --config feast_mcp.yaml
```

With the additions developed for this demo, it exposes ten `features_*` tools:

| Tool | What it does |
|---|---|
| `features_get_online_features` | Read features for entities, optionally including feature view version metadata |
| `features_vector_store_search` | OpenAI-compatible text search with metadata filters and a choice of returned features (CLaMP 3 embeds the query) |
| `features_search` | Vector search with a raw query vector and metadata filters |
| `features_list_vector_stores`, `features_get_vector_store` | Find the vector store ids |
| `features_push` | Push rows |
| `features_write_to_online_store` | Write rows directly to a feature view without a push source |
| `features_materialize`, `features_materialize_incremental` | Load offline data online, optionally in the background with `async_mode` |
| `features_health` | Health check |

Add `registry.url` and you also get `registry_*` tools to browse projects, feature views and lineage.

Building this DJ exposed gaps between the feature server's API and its MCP tools. The code changes accompanying this post pass metadata filters through both search tools, let text search choose which features to return, expose direct online writes, and forward feature view version and materialization options. These additions require a Feast build that includes those changes.

The DJ gives the LLM only two of them, `features_get_online_features` and `features_vector_store_search`. The fewer the tools, the better a 3B model does. The other calls run in plain code over the same MCP session.

## Step 7: The DJ loop

The agent connects to `feast mcp`, hands the two tools to Ollama, and lets `granite4.2:3b` drive:

```python
async with streamablehttp_client("http://127.0.0.1:6590/mcp") as (read, write, _), \
        ClientSession(read, write) as mcp:
    await mcp.initialize()
    tools = {t.name: t for t in (await mcp.list_tools()).tools}
    llm_tools = [as_ollama_tool(tools["features_get_online_features"]),
                 as_ollama_tool(tools["features_vector_store_search"])]

    while True:
        reply = ollama.chat(model="granite4.2:3b", messages=messages, tools=llm_tools, think=False)
        if not reply.message.tool_calls:
            break
        for call in reply.message.tool_calls:
            result = await mcp.call_tool(call.function.name, dict(call.function.arguments))
            messages.append({"role": "tool", "tool_name": call.function.name,
                             "content": result_text(result)})
```

Here is a real run for "hype me up for a workout":

```
LLM -> features_get_online_features({"features": ["user_taste:taste_summary", "user_taste:top_genres",
        "user_listening:recent_track_ids"], "entities": {"user_id": ["demo"]}})
LLM -> features_vector_store_search({"vector_store_id": "vs_20b6...", "query": ["fast energetic beats
        with heavy drums", "hip hop tracks that match energetic genre"], "max_num_results": 20})

DJ: Let's crank the intensity—here's an electrifying workout playlist to fuel your session!
20 mood matches, 6 sound-alikes -> 12 new tracks
Saved playlist 'AI DJ: high energy, driving rhythm' (12 songs) in Navidrome
```

After the LLM is done, plain code finishes the job:

1. **Sound-alikes.** It reads the last song's sound vector and calls `features_search` with it, for songs that *sound like* it.
2. **Merge.** Three mood matches, then one sound-alike, and repeat.
3. **Clean up.** Drop songs the listener just played or skipped.
4. **Save.** Create the playlist with Navidrome's Subsonic `createPlaylist` call.

The whole round takes about 20 seconds on an M1 laptop.

### Tips for a 3B model

- **Small models copy examples.** Our first prompt had one example query, and the model reused it word for word for a *workout* request. Two contrasting examples fixed it:

  ```
  request "workout" -> "fast energetic beats with heavy drums"
  request "study"   -> "calm ambient textures without vocals"
  ```

- **Put the request first.** Tell the model the listener's request comes first and their taste is only a guide.
- **Nudge once.** If the model searches but forgets its final JSON, ask once more instead of failing.

## Why Feast for this?

You could glue a vector database, a key-value store and a few scripts together. Feast gives you one place for all of it:

- **One catalog.** Track vectors, tags, play history and taste all live in one store, behind one API.
- **Online and offline.** Every push lands in both. The online copy feeds the DJ in milliseconds; the offline copy, with correct timestamps, is ready for training.
- **Fresh taste.** TTLs on `user_listening` and `user_taste` let old listening fade out.
- **Your embedder.** A custom `EmbeddingProvider` let us use a music model instead of a text one.
- **MCP out of the box.** `feast mcp` gives any agent Feast tools without writing a single tool by hand.

## Limits and next steps

- **Playlist constraints.** The MCP search tools now accept metadata filters. The demo shown here searches by mood; a next step is to pass genre or year constraints through `filters`.
- **FMA small clips are 30 seconds.** CLaMP 3 can read up to 640 seconds, so full songs would help it more.
- **Cold start.** A new listener has no history. Start with a mood question.
- **Two searches.** One vector per feature view means one search per vector, merged in code.
- **Next:** train a ranking model on the play history in the offline store.

## Run it yourself

<!-- TODO: link the demo code once it is published -->

You need [uv](https://docs.astral.sh/uv/), [Ollama](https://ollama.com), [Navidrome](https://www.navidrome.org) and ffmpeg. Then:

```bash
ollama pull granite4.2:3b
uv sync

# Music, vectors, Feast
mkdir -p data/raw && curl -L -o data/raw/fma_metadata.zip https://os.unil.cloud.switch.ch/fma/fma_metadata.zip
uv run python scripts/prepare_catalog.py
./clamp3/setup.sh && (cd clamp3 && .venv/bin/python embed_catalog.py)
uv run python scripts/build_features.py
(cd feature_repo && uv run feast apply && uv run feast materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%S))

# Five services, each in its own terminal
# (first run: scripts/setup_navidrome.py, then the README's one-time Navidrome setup)
navidrome --configfile config/navidrome.toml
(cd clamp3 && .venv/bin/python server.py)
uv run python -m dj.server
uv run feast mcp --config feast_mcp.yaml
uv run python -m dj.bridge

# Listen, learn, DJ
uv run python scripts/simulate_plays.py
uv run python -m dj.taste
uv run python -m dj.agent "hype me up for a workout" -v
```

Open Navidrome at `http://127.0.0.1:4533` and press play on your new playlist.

## Credits

- Music from the [Free Music Archive dataset](https://github.com/mdeff/fma) (Defferrard et al., ISMIR 2017). The metadata is CC BY 4.0, and each track keeps the Creative Commons license its artist chose.
- [CLaMP 3](https://github.com/sanderwood/clamp3) (Wu et al., 2025) and [MERT](https://huggingface.co/m-a-p/MERT-v1-95M) (Li et al., 2023).
- [Navidrome](https://www.navidrome.org) and [IBM Granite 4.2](https://ollama.com/library/granite4.2).
