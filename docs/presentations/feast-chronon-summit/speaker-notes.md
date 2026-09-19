# Feast + Chronon

20 main slides and 2 appendix slides. Approximately 20 minutes plus questions.

## 1. Feast + Chronon

Open with the practical question: if a team already uses Feast and starts computing features in Chronon, what should change for the model developer? This talk examines a concrete read integration and when its benefits justify the extra operational surface. The implementation is an open pull request as of September 14, 2026. Avoid presenting it as a released, generally available connector. The demo runs the tested commit fc3c683d9. Suggested main talk duration: approximately 20 minutes, plus questions.

- [Proposed Feast integration](https://github.com/feast-dev/feast/pull/6188)

## 2. Where the combination earns its keep

Frame this as an architectural option for an existing platform, not a universal recommendation. A common API can reduce changes in model code, but the underlying feature definitions, storage choices, and operational dependencies still matter. We will distinguish the architectural benefit from the exact capability of the proposed adapter. An organization that is already happy with one system may have little to gain by adding the other.

- [Feast: feature retrieval](https://docs.feast.dev/getting-started/concepts/feature-retrieval)
- [Chronon: introduction](https://docs.chronon.ai/getting_started/Introduction.html)

## 3. Feast in this architecture

Keep the refresher short. Feast gives consumers logical feature definitions and APIs across supported backends. It is not just a registry, and Feast has other transformation and materialization capabilities. Here we deliberately use it as the consumer-facing layer over features that Chronon owns. The two retrieval calls are separate offline and online interfaces. A consistent surface does not mean all backends support every capability. The code assumes registered objects and a configured store.

- [Feast: feature views](https://docs.feast.dev/getting-started/concepts/feature-view)
- [Feast: feature retrieval](https://docs.feast.dev/getting-started/concepts/feature-retrieval)

## 4. Chronon in this architecture

Chronon is an end-to-end feature platform, not merely a batch preprocessing library. GroupBy defines aggregations, and Join combines groups and establishes the left-hand event timeline for training data. Chronon can serve those features without Feast. The reason to add Feast is the value of its interface in the surrounding platform. Accuracy depends on source type and configuration; we will return to snapshot versus temporal semantics later.

- [Chronon: introduction](https://docs.chronon.ai/getting_started/Introduction.html)
- [Chronon: GroupBy and accuracy](https://docs.chronon.ai/authoring_features/GroupBy.html)
- [Chronon: Join and backfill timelines](https://docs.chronon.ai/authoring_features/Join.html)
- [Chronon: runtime components](https://docs.chronon.ai/setup/Components.html)

## 5. An explicit division of responsibility

The adapter is a read boundary. Its online_write_batch raises NotImplementedError, and its infrastructure update and teardown methods do not manage Chronon infrastructure. The Chronon provider is a small passthrough wrapper, not a Chronon scheduler. Registering a FeatureView in Feast does not deploy the corresponding Chronon jobs. Treat aligned schemas, key types, feature names, timestamps, and versions as an explicit platform contract.

- [PR: online adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/online_stores/chronon_online_store/chronon.py)
- [PR: offline adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/offline_stores/contrib/chronon_offline_store/chronon.py)
- [PR: ChrononSource](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/offline_stores/contrib/chronon_offline_store/chronon_source.py)

## 6. Two read paths, one consumer interface

Walk from source data to Chronon, then split the online and offline paths. The online path calls Chronon’s service rather than duplicating values into another Feast-managed online database. Offline retrieval in this PR reads Parquet through pandas. The export/publication boundary matters: this adapter does not launch a Chronon backfill or provide an automatic warehouse-table export pipeline. Arrows show feature-value flow rather than request direction. Feast registry metadata configures both adapters; no registry RPC on every request is implied. The diagram describes the PR architecture, not every possible Feast or Chronon deployment.

- [PR: online adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/online_stores/chronon_online_store/chronon.py)
- [PR: offline adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/offline_stores/contrib/chronon_offline_store/chronon.py)
- [Chronon: runtime components](https://docs.chronon.ai/setup/Components.html)

## 7. The online request boundary

The actual route percent-encodes the Chronon object name. The source can override the store-level base URL. The adapter expects one result row per requested entity key and assumes response order matches request order. HTTP failures and non-success result rows raise errors. Successful responses preserve null/missing feature values. The model can distinguish expected absence from a failed retrieval. Optional connection_retries adds up to five retries through Feast’s HTTP session manager; retries default to zero. Requested feature filtering happens after Chronon responds, so it does not necessarily reduce upstream computation or response size.

- [PR: online adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/online_stores/chronon_online_store/chronon.py)

## 8. Historical retrieval uses materialized rows

The implementation loads materialized Parquet into pandas, groups by entity key, and performs backward merge_asof joins. A nonzero FeatureView TTL limits row age. This does not recompute a rolling window for every requested timestamp. A snapshot computed at 10:00 is still a 10:00 snapshot when selected for 10:05. Ensure the exported timeline and intended Chronon accuracy match the training objective. If duplicate entity/event-time rows exist, the latest configured created timestamp wins. There is no separate historical knowledge-time cutoff in that selection, so revision handling needs an explicit policy.

- [PR: offline adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/offline_stores/contrib/chronon_offline_store/chronon.py)
- [Chronon: GroupBy and accuracy](https://docs.chronon.ai/authoring_features/GroupBy.html)
- [Chronon: Join and backfill timelines](https://docs.chronon.ai/authoring_features/Join.html)

## 9. The integration surface is small

These are configuration excerpts, not a full standalone program. Source-level materialization_path identifies the offline data. Exactly one Chronon Join or GroupBy identifies the online object. The same logical definition can reference both, but keeping their versions aligned is the platform’s responsibility. The complete runnable example is linked in the sources. The local demo uses HTTP on loopback; production endpoint and authentication choices require separate validation. Do not suggest that feast apply starts or materializes Chronon jobs.

- [PR: ChrononSource](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/offline_stores/contrib/chronon_offline_store/chronon_source.py)
- [PR: runnable demo](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/examples/chronon/run_demo.py)
- [Proposed Feast integration](https://github.com/feast-dev/feast/pull/6188)

## 10. Checkout example: the code path

Walk through the highlighted lines before playing the video. First, the ChrononSource sits inside the checkout_risk_features FeatureView. QUICKSTART_JOIN is quickstart/training_set.v2. The complete FeatureView declares a string user_id key, ten purchase/refund fields, online=True, and offline=False. The demo includes placeholder Parquet metadata to define the source; it does not read that placeholder to serve online features. Second, the FeatureService named checkout_risk_v1 selects that FeatureView. store.apply registers the entity, view, and service in Feast; it does not run Chronon aggregation jobs. Finally, the SDK requests users 5, 7, and 999999, and converts the result to a dictionary. The entity-row comprehension is inlined here from the demo helper so the requested users are visible. All other displayed executable lines are taken from the example, with metadata and setup deliberately omitted. The ten-feature schema and direct Chronon comparison remain in the full example. These snippets explain the flow rather than form a standalone script.

- [PR: runnable demo](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/examples/chronon/run_demo.py)
- [PR: ChrononSource](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/offline_stores/contrib/chronon_offline_store/chronon_source.py)
- [PR: online adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/online_stores/chronon_online_store/chronon.py)

## 11. Feast UI: the checkout feature catalog

This is an actual screenshot of the local Feast UI with the checkout demo registry, captured September 14, 2026. Point to checkout_risk_v1 at the top: it is the FeatureService in the previous code slide. The service selects ten features from one FeatureView. The table exposes feature names and value types and links back to the view. Tags identify the Chronon checkout-risk scenario, while the entity link identifies user. The left navigation and search make the broader registry discoverable. This is a concrete reason a platform with existing Feast users might retain Feast while Chronon computes the values. The screenshot shows registry metadata, not live feature values, Chronon job execution, or a demonstrated monitoring integration. The screenshot uses locally built Feast UI assets with the PR backend and its real example definitions. Only the top portion of the feature table is visible; all ten features exist in the registry. Continue to the video to show retrieval from the actual Chronon service.

- [PR: runnable demo](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/examples/chronon/run_demo.py)
- [Feast: feature views](https://docs.feast.dev/getting-started/concepts/feature-view)
- [Proposed Feast integration](https://github.com/feast-dev/feast/pull/6188)

## 12. A working checkout example

Play the embedded video. It is packaged in this HTML and does not need a network connection. It has no audio, so narrate over it. First we read a small generated historical Parquet sample. Then we fetch from a real Chronon quickstart service backed by MongoDB. Finally, the checkout FeatureService fetches ten features for three users, and we compare those values to direct Chronon responses. This is a local integration demo, not a production deployment or a live-stream freshness demonstration. If video playback is unavailable, the next slide preserves the key results.

- [PR: runnable demo](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/examples/chronon/run_demo.py)

## 13. What the demo establishes

These are measured results from our September 13 run at fc3c683d9. Do not describe 39 tests as the entire repository suite. Seven are offline and HTTP-stub integration tests; one specifically exercises the real Chronon service. The thirty comparisons cover ten features for each of three users and allow Float32 rounding. Missing values for the unknown user are part of the comparison. This establishes a working read path and selected correctness cases, not comprehensive semantic equivalence across historical and online data at scale.

- [PR: runnable demo](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/examples/chronon/run_demo.py)

## 14. Benefits of the combination

These are architectural benefits inferred from the integration, not benchmark results. The native Chronon service remains the serving backend, so this path does not require a second copy in a Feast-managed online store. The incremental adoption argument is about keeping consumer interfaces familiar. It is not a promise of arbitrary multi-backend composition within this PR: backend configuration and unsupported combinations still need validation. There is still metadata to author in both systems.

- [PR: online adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/online_stores/chronon_online_store/chronon.py)
- [PR: ChrononSource](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/offline_stores/contrib/chronon_offline_store/chronon_source.py)
- [Feast: feature retrieval](https://docs.feast.dev/getting-started/concepts/feature-retrieval)

## 15. Costs of the combination

Be candid about the cons. The exact overhead depends on the alternative: native Chronon clients and HTTP clients have different baselines. We have not measured a latency penalty, so avoid attaching numbers. This Python adapter uses synchronous HTTP requests. Retries are opt-in and bounded by attempt count; timeout is per attempt, not a total deadline. Backoff and Retry-After can extend latency. Benchmark the entire model request, including fanout and cold starts. Metadata release coordination and incident ownership are real costs even if the adapter code is small. A shared interface can hide differences from callers without eliminating them.

- [PR: online adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/online_stores/chronon_online_store/chronon.py)
- [Chronon: runtime components](https://docs.chronon.ai/setup/Components.html)
- [PR: ChrononSource](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/offline_stores/contrib/chronon_offline_store/chronon_source.py)

## 16. Time semantics still need an agreement

Chronon SNAPSHOT and TEMPORAL accuracy encode different evaluation timelines. A Feast as-of selection over exports must preserve the chosen meaning. TTL here bounds how old a materialized row can be, not the aggregation window length. For instance, a 30-day purchase sum may have an hourly publication cadence and a separate freshness target. Do not treat those three intervals as equivalent. The current created-time rule chooses the latest revision for duplicate event times; it does not reconstruct when each revision became knowable. Validate late data, revisions, time zones, nulls, and timestamp precision for the intended use case.

- [Chronon: GroupBy and accuracy](https://docs.chronon.ai/authoring_features/GroupBy.html)
- [Chronon: Join and backfill timelines](https://docs.chronon.ai/authoring_features/Join.html)
- [PR: offline adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/offline_stores/contrib/chronon_offline_store/chronon.py)
- [Chronon: online/offline consistency](https://docs.chronon.ai/test_deploy_serve/Online_Offline_Consistency.html)

## 17. The current adapter has a narrow scope

Anchor limitations to the tested PR revision, not to the projects in general. The current revision reads only required Parquet columns before doing local joins. It does not push retrieval into Spark or a warehouse engine. The retrieval job supports Parquet saved datasets and local on-demand transforms; SQL export remains unsupported. SavedDataset round-trips use provider: chronon and preserve request data and custom timestamps. These additions passed 68 targeted unit tests plus seven offline/HTTP-stub integration tests on September 14. The embedded live demo remains the September 13 recording at fc3c683d9. The PR documentation lists Python SDK support and no direct Go or Java adapter support. Online infrastructure update and teardown are no-ops. Chronon online freshness remains Chronon-owned; the historical TTL filter is not a universal online TTL guarantee. Check upstream status before presenting because the PR remains open.

- [PR: offline adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/offline_stores/contrib/chronon_offline_store/chronon.py)
- [PR: online adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/online_stores/chronon_online_store/chronon.py)
- [Proposed Feast integration](https://github.com/feast-dev/feast/pull/6188)

## 18. Choosing the shape of the platform

This is a decision framework, not a ranking. It is reasonable to choose either system alone. Keep Feast with the pipelines you already have if those pipelines solve the problem. Use Chronon directly when its native workflow is enough. Combine them when stable Feast consumer interfaces and Chronon computation both deliver concrete value. Do not choose two platforms merely because the integration exists. Team expertise, backend compatibility, latency budget, and migration cost should drive the choice.

- [Chronon: introduction](https://docs.chronon.ai/getting_started/Introduction.html)
- [Feast: feature retrieval](https://docs.feast.dev/getting-started/concepts/feature-retrieval)
- [Proposed Feast integration](https://github.com/feast-dev/feast/pull/6188)

## 19. Production validation beyond the demo

These are next engineering steps, not claims that the adapter already provides them. Compare against an appropriate native Chronon baseline, and use realistic feature counts and entity batch sizes. Exercise non-success rows as well as HTTP failures: both raise, but only eligible HTTP/transport failures are retried when configured. Measure memory on realistic historical data volumes. Agree how model inference behaves on null or stale features. Validate authentication and endpoint deployment in the actual environment. Make schema and semantic compatibility checks a release gate before wider adoption.

- [PR: online adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/online_stores/chronon_online_store/chronon.py)
- [PR: offline adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/offline_stores/contrib/chronon_offline_store/chronon.py)
- [Chronon: online/offline consistency](https://docs.chronon.ai/test_deploy_serve/Online_Offline_Consistency.html)

## 20. A useful boundary for a mixed platform

Close on the decision, not on the number of tests. We have demonstrated a read integration that works for a concrete example. The harder platform design questions are semantic alignment and operational ownership. Invite questions about source-of-truth decisions, migration patterns, latency requirements, and historical correctness. The following two slides are backup material for deeper questions. Leave the main talk here unless the audience wants implementation detail.

- [Proposed Feast integration](https://github.com/feast-dev/feast/pull/6188)

## 21. Appendix: details worth testing

This table comes directly from the PR implementation. Row ordering is positional; there is no reconciliation by a returned entity key. Type declarations drive protobuf conversion, and field mapping applies in both request and response directions. connection_retries defaults to zero and accepts 0–5 retries through Feast’s shared HTTP session manager. It retries eligible transport failures and HTTP 429/5xx responses, including the read-only POST. HTTP 400 and per-row Chronon failures are not retried. Timeouts and TLS verification are configurable; timeout is per attempt, and Retry-After/backoff can extend total latency. These behaviors are testable boundaries, not necessarily bugs. The production design needs to decide the acceptable semantics for each one.

- [PR: online adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/online_stores/chronon_online_store/chronon.py)
- [PR: offline adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/offline_stores/contrib/chronon_offline_store/chronon.py)

## 22. Sources and further reading

Use these links for follow-up discussion. All links are to project documentation or the implementation under discussion. The artifact works offline, but following external citations requires an internet connection. The video, poster, styling, speaker notes, and navigation are included in the HTML itself. Keyboard shortcuts: arrows or Space to move, N for notes, O for overview, F for fullscreen, B to blank the screen, P for a separate presenter window, Home for the opening slide, and End for this source slide.

- [Feast: feature views](https://docs.feast.dev/getting-started/concepts/feature-view)
- [Feast: feature retrieval](https://docs.feast.dev/getting-started/concepts/feature-retrieval)
- [Chronon: introduction](https://docs.chronon.ai/getting_started/Introduction.html)
- [Chronon: GroupBy and accuracy](https://docs.chronon.ai/authoring_features/GroupBy.html)
- [Chronon: Join and backfill timelines](https://docs.chronon.ai/authoring_features/Join.html)
- [Chronon: runtime components](https://docs.chronon.ai/setup/Components.html)
- [Chronon: online/offline consistency](https://docs.chronon.ai/test_deploy_serve/Online_Offline_Consistency.html)
- [PR: online adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/online_stores/chronon_online_store/chronon.py)
- [PR: offline adapter](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/offline_stores/contrib/chronon_offline_store/chronon.py)
- [PR: ChrononSource](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/sdk/python/feast/infra/offline_stores/contrib/chronon_offline_store/chronon_source.py)
- [PR: runnable demo](https://github.com/feast-dev/feast/blob/55873756b36d8b73c277c7971959aa8dc3fb9649/examples/chronon/run_demo.py)
- [Proposed Feast integration](https://github.com/feast-dev/feast/pull/6188)