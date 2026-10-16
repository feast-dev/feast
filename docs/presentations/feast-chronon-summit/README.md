# Feast + Chronon: Feature Store Summit

Download [feast-chronon-summit.html](feast-chronon-summit.html) and open it in a modern desktop browser. GitHub displays the HTML source; use **Download raw file** to save the deck. The file contains all 22 slides, the video, the poster, styles, and speaker notes. It requires no CDN or internet connection for the deck itself. External source links require internet access.

The main talk has 20 slides, followed by two appendix slides, and is designed for approximately 20 minutes plus questions. [Speaker notes](speaker-notes.md) are a separate readable copy of the embedded notes.

## Controls

- Arrow keys, Page Up / Page Down, or Space: move between slides.
- Home / End: first / last slide.
- N: show or hide speaker notes and citations.
- O: slide overview. Escape closes it.
- F: request fullscreen where the browser permits it.
- B: blank the screen. B or Escape restores the slide.
- P / Presenter: open a separate notes window where popups are supported.
- The embedded video has its own playback and fullscreen controls. Advancing the deck pauses it.
- Browser Print uses one slide per page. Video appears as a poster in printed output.

Browser panels embedded in other applications may restrict fullscreen, popup windows, or local file access. The normal Notes panel works without a popup.

## Scope

The presentation discusses a proposed integration, Feast PR #6188, at commit `55873756b36d8b73c277c7971959aa8dc3fb9649`. The embedded recording remains pinned to the earlier demo commit `fc3c683d9`. PR status and public documentation were checked September 14, 2026. The embedded demo was recorded September 13, 2026. It shows real Chronon/MongoDB online retrieval and a generated Parquet sample for offline retrieval.

The 39 passing tests and 30 matched feature values are results of the focused local verification. They are not performance benchmarks or a claim of full production readiness. Slides and notes separate measured results from architectural benefits and production validation recommendations.

The checkout section includes annotated code excerpts and an actual Feast UI screenshot of the registered checkout FeatureService. The UI screenshot is embedded in the HTML and shows registry metadata, not live values or Chronon job status.

The current adapter adds explicit online service errors, opt-in HTTP retries, Parquet column selection, saved datasets, and local on-demand transforms. These changes passed 68 targeted unit tests and seven offline/HTTP-stub integration tests, plus lint/format and type checking. The recording’s 39-test result remains its original September 13 evidence.

## Edit and rebuild

Run from the repository root with Python 3.10+ (standard library only; no Feast installation or running Chronon service required):

```bash
python3 docs/presentations/feast-chronon-summit/build_deck.py
```

Edit `build_deck.py` to change the slide content, speaker notes, styles, or navigation. The build regenerates `feast-chronon-summit.html`, `speaker-notes.md`, and `deck-content.json` beside the script. Commit the regenerated files together with the source.

The `assets/` directory contains the original demo recording, video poster, and Feast UI screenshot. The build embeds these in the standalone HTML, so only the HTML is needed to present offline. The recording is silent; speaker notes provide narration context. The source media are retained separately to make rebuilding possible.

For a local HTTP preview, run from the repository root:

```bash
python3 -m http.server 8766 --bind 127.0.0.1 --directory docs/presentations/feast-chronon-summit
```

Then open <http://127.0.0.1:8766/feast-chronon-summit.html>.

The deck is a dated presentation snapshot, not the current compatibility reference. Its sources link to pinned revisions of [integration PR #6188](https://github.com/feast-dev/feast/pull/6188); the demo and implementation claims intentionally reference different revisions as described above.
