from pathlib import Path
from typing import Any, Sequence
import base64
import html
import json

ROOT = Path(__file__).resolve().parent
ASSETS = ROOT / "assets"
PR = "https://github.com/feast-dev/feast/pull/6188"
REV = "55873756b36d8b73c277c7971959aa8dc3fb9649"
GH = f"https://github.com/feast-dev/feast/blob/{REV}/"
S = {
    "feast": (
        "Feast: feature views",
        "https://docs.feast.dev/getting-started/concepts/feature-view",
    ),
    "retrieval": (
        "Feast: feature retrieval",
        "https://docs.feast.dev/getting-started/concepts/feature-retrieval",
    ),
    "chronon": (
        "Chronon: introduction",
        "https://docs.chronon.ai/getting_started/Introduction.html",
    ),
    "groupby": (
        "Chronon: GroupBy and accuracy",
        "https://docs.chronon.ai/authoring_features/GroupBy.html",
    ),
    "join": (
        "Chronon: Join and backfill timelines",
        "https://docs.chronon.ai/authoring_features/Join.html",
    ),
    "components": (
        "Chronon: runtime components",
        "https://docs.chronon.ai/setup/Components.html",
    ),
    "consistency": (
        "Chronon: online/offline consistency",
        "https://docs.chronon.ai/test_deploy_serve/Online_Offline_Consistency.html",
    ),
    "online": (
        "PR: online adapter",
        GH + "sdk/python/feast/infra/online_stores/chronon_online_store/chronon.py",
    ),
    "offline": (
        "PR: offline adapter",
        GH
        + "sdk/python/feast/infra/offline_stores/contrib/chronon_offline_store/chronon.py",
    ),
    "source": (
        "PR: ChrononSource",
        GH
        + "sdk/python/feast/infra/offline_stores/contrib/chronon_offline_store/chronon_source.py",
    ),
    "example": ("PR: runnable demo", GH + "examples/chronon/run_demo.py"),
    "pr": ("Proposed Feast integration", PR),
}
slides: list[dict[str, Any]] = []


def add(
    title: str,
    body: str,
    notes: str,
    refs: Sequence[str] = (),
    kind: str = "",
    foot: str = "",
) -> None:
    slides.append(
        dict(title=title, body=body, notes=notes, refs=list(refs), kind=kind, foot=foot)
    )


def code(s: str) -> str:
    return "<pre><code>" + html.escape(s.strip()) + "</code></pre>"


def rows(items: Sequence[tuple[str, str]]) -> str:
    return (
        '<div class="rows">'
        + "".join(f'<div class="row"><h3>{a}</h3><p>{b}</p></div>' for a, b in items)
        + "</div>"
    )


def table(headers: Sequence[str], data: Sequence[Sequence[str]], cl: str = "") -> str:
    return (
        f'<table class="{cl}"><thead><tr>'
        + "".join(f"<th>{c}</th>" for c in headers)
        + "</tr></thead><tbody>"
        + "".join(
            "<tr>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>" for row in data
        )
        + "</tbody></table>"
    )


add(
    "Feast + Chronon",
    """<div class="cover"><p class="event">Feature Store Summit</p><h1><span class="feast">Feast</span> <span class="plus">+</span><br><span class="chronon">Chronon</span></h1><p class="cover-sub">Using both in a feature platform</p><p class="cover-detail">Architecture, a working integration, and the tradeoffs</p><p class="status">Integration case study · PR #6188</p></div>""",
    """Open with the practical question: if a team already uses Feast and starts computing features in Chronon, what should change for the model developer? This talk examines a concrete read integration and when its benefits justify the extra operational surface. The implementation is an open pull request as of September 14, 2026. Avoid presenting it as a released, generally available connector. The demo runs the tested commit fc3c683d9. Suggested main talk duration: approximately 20 minutes, plus questions.""",
    ["pr"],
    "cover-slide",
)
add(
    "Where the combination earns its keep",
    """<p class="lead">Model teams already use Feast.<br>Some feature pipelines need Chronon.</p>"""
    + rows(
        [
            (
                "The application contract",
                "Keep feature selection and retrieval familiar to model developers.",
            ),
            (
                "The computation requirement",
                "Use Chronon’s aggregation and backfill machinery for the features that need it.",
            ),
            (
                "The architectural question",
                "Does a shared consumer interface justify operating the integration?",
            ),
        ]
    ),
    """Frame this as an architectural option for an existing platform, not a universal recommendation. A common API can reduce changes in model code, but the underlying feature definitions, storage choices, and operational dependencies still matter. We will distinguish the architectural benefit from the exact capability of the proposed adapter. An organization that is already happy with one system may have little to gain by adding the other.""",
    ["retrieval", "chronon"],
)
add(
    "Feast in this architecture",
    """<div class="split"><div><p class="lead feast">A feature interface<br>for model consumers</p><p>Entities identify the subject.<br>FeatureViews describe feature data.<br>FeatureServices select a model’s inputs.</p><p class="muted">The registry and pluggable stores connect those definitions to retrieval.</p></div><div class="code-block"><div class="code-title">A familiar consumer interface</div>"""
    + code("""features = store.get_online_features(
    features=checkout_risk_v1,
    entity_rows=[{"user_id": "5"}],
).to_dict()

training = store.get_historical_features(
    features=feature_refs,
    entity_df=training_entities,
).to_df()""")
    + """</div></div>""",
    """Keep the refresher short. Feast gives consumers logical feature definitions and APIs across supported backends. It is not just a registry, and Feast has other transformation and materialization capabilities. Here we deliberately use it as the consumer-facing layer over features that Chronon owns. The two retrieval calls are separate offline and online interfaces. A consistent surface does not mean all backends support every capability. The code assumes registered objects and a configured store.""",
    ["feast", "retrieval"],
)
add(
    "Chronon in this architecture",
    """<p class="lead chronon">Feature computation with batch and streaming paths</p><div class="two-col concepts"><div><h3>GroupBy</h3><p>Define entity-level aggregations, windows, and accuracy.</p><div class="example">Purchases per user<br>Sum and count over 14 and 30 days</div></div><div><h3>Join</h3><p>Combine feature groups and define the timeline for backfills.</p><div class="example">Purchase and refund features<br>Evaluated for checkout events</div></div></div><p class="bottom-line">Chronon also provides native serving and monitoring capabilities.</p>""",
    """Chronon is an end-to-end feature platform, not merely a batch preprocessing library. GroupBy defines aggregations, and Join combines groups and establishes the left-hand event timeline for training data. Chronon can serve those features without Feast. The reason to add Feast is the value of its interface in the surrounding platform. Accuracy depends on source type and configuration; we will return to snapshot versus temporal semantics later.""",
    ["chronon", "groupby", "join", "components"],
)
add(
    "An explicit division of responsibility",
    table(
        ["Responsibility", "Owner in this integration"],
        [
            (
                "Aggregations, windows, backfills",
                '<span class="chronon">Chronon</span>',
            ),
            (
                "Updates to the Chronon online store",
                '<span class="chronon">Chronon</span>',
            ),
            (
                "Consumer schema and feature selection",
                '<span class="feast">Feast definitions and registry</span>',
            ),
            ("Request routing and value conversion", "Feast’s Chronon adapters"),
            ("Version alignment and operating policy", "The platform team"),
        ],
    )
    + """<p class="bottom-line">One owner computes each feature. Both sides agree on its meaning.</p>""",
    """The adapter is a read boundary. Its online_write_batch raises NotImplementedError, and its infrastructure update and teardown methods do not manage Chronon infrastructure. The Chronon provider is a small passthrough wrapper, not a Chronon scheduler. Registering a FeatureView in Feast does not deploy the corresponding Chronon jobs. Treat aligned schemas, key types, feature names, timestamps, and versions as an explicit platform contract.""",
    ["online", "offline", "source"],
)
architecture = """<svg class="architecture" viewBox="0 0 1440 540" role="img" aria-label="Chronon computes features. Its HTTP service supplies online features to a Feast adapter. Exported Parquet supplies historical features to a separate Feast adapter. Model consumers use Feast APIs."><defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8" fill="currentColor"/></marker></defs>
<g class="flow-lines"><path d="M215 230 H275"/><path d="M520 200 H570 V120 H650"/><path d="M520 260 H570 V400 H650"/><path d="M895 120 H990"/><path d="M895 400 H990"/><path d="M1215 120 H1270 V230 H1330"/><path d="M1215 400 H1270 V275 H1330"/></g>
<g class="node"><rect x="0" y="168" width="215" height="124"/><text x="107" y="215">Source data</text><text x="107" y="255" class="detail">Tables / streams</text></g>
<g class="node coral"><rect x="275" y="145" width="245" height="180"/><text x="397" y="190">Chronon</text><text x="397" y="230" class="detail">GroupBy + Join</text><text x="397" y="277" class="detail">Batch / streaming</text></g>
<g class="node coral"><rect x="650" y="55" width="245" height="130"/><text x="772" y="104">Online KV</text><text x="772" y="145" class="detail">Chronon HTTP service</text></g>
<g class="node"><rect x="650" y="335" width="245" height="130"/><text x="772" y="385">Parquet output</text><text x="772" y="425" class="detail">Export / publish step</text></g>
<g class="node green"><rect x="990" y="55" width="225" height="130"/><text x="1102" y="104">Feast adapter</text><text x="1102" y="145" class="detail">Online retrieval</text></g>
<g class="node green"><rect x="990" y="335" width="225" height="130"/><text x="1102" y="385">Feast adapter</text><text x="1102" y="425" class="detail">Historical retrieval</text></g>
<text x="1380" y="233" class="consumer">Model</text><text x="1380" y="270" class="consumer">code</text>
<text x="920" y="85" class="edge-label">HTTP</text><text x="918" y="368" class="edge-label">Read</text>
<text x="660" y="515" class="diagram-note">Arrows show feature-value flow. Feast’s registry supplies retrieval metadata.</text>
</svg>"""
add(
    "Two read paths, one consumer interface",
    architecture,
    """Walk from source data to Chronon, then split the online and offline paths. The online path calls Chronon’s service rather than duplicating values into another Feast-managed online database. Offline retrieval in this PR reads Parquet through pandas. The export/publication boundary matters: this adapter does not launch a Chronon backfill or provide an automatic warehouse-table export pipeline. Arrows show feature-value flow rather than request direction. Feast registry metadata configures both adapters; no registry RPC on every request is implied. The diagram describes the PR architecture, not every possible Feast or Chronon deployment.""",
    ["online", "offline", "components"],
    foot="PR #6188 architecture · Offline export and job orchestration remain external",
)
add(
    "The online request boundary",
    rows(
        [
            (
                "1. Map the request",
                "A FeatureView points to a Chronon Join or GroupBy. The adapter maps entity keys.",
            ),
            (
                "2. Call Chronon",
                "The adapter posts entity rows to the selected HTTP endpoint.",
            ),
            (
                "3. Return Feast values",
                "Declared feature types guide conversion. The model receives typed feature values.",
            ),
        ]
    )
    + """<div class="endpoint">POST /v1/features/join/quickstart%2Ftraining_set.v2<br><span>[{"user_id": "5"}, {"user_id": "7"}]</span></div>""",
    """The actual route percent-encodes the Chronon object name. The source can override the store-level base URL. The adapter expects one result row per requested entity key and assumes response order matches request order. HTTP failures and non-success result rows raise errors. Successful responses preserve null/missing feature values. The model can distinguish expected absence from a failed retrieval. Optional connection_retries adds up to five retries through Feast’s HTTP session manager; retries default to zero. Requested feature filtering happens after Chronon responds, so it does not necessarily reduce upstream computation or response size.""",
    ["online"],
)
add(
    "Historical retrieval uses materialized rows",
    """<p class="lead">For each entity, choose the latest eligible row.</p><div class="time-line"><div><span class="time">10:00</span><strong class="feast">12 rides</strong><p>Eligible materialized row</p></div><div class="query"><span class="time">10:05</span><strong>Training event</strong><p>Retrieved value: <b class="feast">12</b></p></div><div><span class="time">10:10</span><strong class="muted">18 rides</strong><p>Later row excluded</p></div></div><p class="formula">feature time ≤ entity time<br><span>and row age within the configured FeatureView TTL</span></p><p class="caption">Illustrative values. The adapter selects existing rows; Chronon defines the aggregates within them.</p>""",
    """The implementation loads materialized Parquet into pandas, groups by entity key, and performs backward merge_asof joins. A nonzero FeatureView TTL limits row age. This does not recompute a rolling window for every requested timestamp. A snapshot computed at 10:00 is still a 10:00 snapshot when selected for 10:05. Ensure the exported timeline and intended Chronon accuracy match the training objective. If duplicate entity/event-time rows exist, the latest configured created timestamp wins. There is no separate historical knowledge-time cutoff in that selection, so revision handling needs an explicit policy.""",
    ["offline", "groupby", "join"],
)
add(
    "The integration surface is small",
    """<div class="split code-split"><div><div class="code-title">feature_store.yaml</div>"""
    + code("""project: checkout
registry: data/registry.db
provider: chronon

offline_store:
  type: chronon
online_store:
  type: chronon
  path: http://localhost:19000""")
    + """</div><div><div class="code-title">Source metadata in a FeatureView</div>"""
    + code("""source = ChrononSource(
    materialization_path=
        "data/checkout_features.parquet",
    chronon_join=
        "quickstart/training_set.v2",
    timestamp_field="event_timestamp",
)""")
    + """<p class="caption">The FeatureView declares keys and field types.<br>A FeatureService selects the model’s inputs.</p></div></div><p class="bottom-line">Chronon jobs and output publication have their own deployment lifecycle.</p>""",
    """These are configuration excerpts, not a full standalone program. Source-level materialization_path identifies the offline data. Exactly one Chronon Join or GroupBy identifies the online object. The same logical definition can reference both, but keeping their versions aligned is the platform’s responsibility. The complete runnable example is linked in the sources. The local demo uses HTTP on loopback; production endpoint and authentication choices require separate validation. Do not suggest that feast apply starts or materializes Chronon jobs.""",
    ["source", "example", "pr"],
)


def checkout_code(text: str, highlighted: Sequence[int], tone: str) -> str:
    lines = []
    for n, line in enumerate(text.strip().splitlines()):
        cls = f' class="code-emphasis {tone}"' if n in highlighted else ""
        lines.append(f"<span{cls}>" + html.escape(line) + "</span>")
    return "<pre><code>" + "".join(lines) + "</code></pre>"


checkout_blocks = [
    (
        """source=ChrononSource(
    chronon_join=QUICKSTART_JOIN,
    online_endpoint=service_url,
    # Parquet path and timestamp metadata omitted
)""",
        [1, 2],
        "chronon",
        "01",
        "Bind the Chronon join",
        "The FeatureView points to <code>quickstart/training_set.v2</code>. Chronon supplies the computed values.",
    ),
    (
        """feature_service = FeatureService(
    name=CHECKOUT_RISK_FEATURE_SERVICE,
    features=[checkout_risk_features],
)
store.apply([user, checkout_risk_features, feature_service])""",
        [2, 4],
        "feast",
        "02",
        "Register the model’s inputs",
        "<code>checkout_risk_v1</code> selects the declared features. <code>apply</code> registers the Feast objects.",
    ),
    (
        """feast_response = store.get_online_features(
    features=feature_service,
    entity_rows=[{"user_id": uid}
                 for uid in ["5", "7", "999999"]],
).to_dict()""",
        [0, 1, 2, 3],
        "feast",
        "03",
        "Read at checkout time",
        "Feast calls Chronon for these users and returns typed values. The unknown user receives missing features.",
    ),
]
add(
    "Checkout example: the code path",
    '<div class="checkout-walkthrough">'
    + "".join(
        '<div class="checkout-step">'
        + checkout_code(text, highlights, tone)
        + f'<div class="checkout-explanation"><span class="step-no {tone}">{number}</span><h3>{title}</h3><p>{explanation}</p></div></div>'
        for text, highlights, tone, number, title, explanation in checkout_blocks
    )
    + '</div><p class="caption">Relevant excerpts from examples/chronon/run_demo.py. Imports, schema, and setup omitted.</p>',
    """Walk through the highlighted lines before playing the video. First, the ChrononSource sits inside the checkout_risk_features FeatureView. QUICKSTART_JOIN is quickstart/training_set.v2. The complete FeatureView declares a string user_id key, ten purchase/refund fields, online=True, and offline=False. The demo includes placeholder Parquet metadata to define the source; it does not read that placeholder to serve online features. Second, the FeatureService named checkout_risk_v1 selects that FeatureView. store.apply registers the entity, view, and service in Feast; it does not run Chronon aggregation jobs. Finally, the SDK requests users 5, 7, and 999999, and converts the result to a dictionary. The entity-row comprehension is inlined here from the demo helper so the requested users are visible. All other displayed executable lines are taken from the example, with metadata and setup deliberately omitted. The ten-feature schema and direct Chronon comparison remain in the full example. These snippets explain the flow rather than form a standalone script.""",
    ["example", "source", "online"],
    "checkout-code-slide",
)


ui_image = base64.b64encode(
    (ASSETS / "feast-checkout-service.png").read_bytes()
).decode()
add(
    "Feast UI: the checkout feature catalog",
    f'<img class="feast-ui-capture" src="data:image/png;base64,{ui_image}" alt="Actual Feast UI showing checkout_risk_v1 with ten features from one FeatureView, feature types, checkout-risk tags, the user entity, project navigation, and registry search.">',
    """This is an actual screenshot of the local Feast UI with the checkout demo registry, captured September 14, 2026. Point to checkout_risk_v1 at the top: it is the FeatureService in the previous code slide. The service selects ten features from one FeatureView. The table exposes feature names and value types and links back to the view. Tags identify the Chronon checkout-risk scenario, while the entity link identifies user. The left navigation and search make the broader registry discoverable. This is a concrete reason a platform with existing Feast users might retain Feast while Chronon computes the values. The screenshot shows registry metadata, not live feature values, Chronon job execution, or a demonstrated monitoring integration. The screenshot uses locally built Feast UI assets with the PR backend and its real example definitions. Only the top portion of the feature table is visible; all ten features exist in the registry. Continue to the video to show retrieval from the actual Chronon service.""",
    ["example", "feast", "pr"],
    "feast-ui-slide",
    foot="Feast UI · Browse feature definitions, types, tags, and entity links · Local checkout demo",
)

video = base64.b64encode((ASSETS / "feast-chronon-demo.mp4").read_bytes()).decode()
poster = base64.b64encode((ASSETS / "poster.png").read_bytes()).decode()
add(
    "A working checkout example",
    f"""<div class="demo-layout"><video controls playsinline preload="metadata" poster="data:image/png;base64,{poster}" aria-label="41-second recording of real Feast and Chronon integration tests"><source src="data:video/mp4;base64,{video}" type="video/mp4"></video><div class="demo-copy"><h3>41 seconds</h3><p>Historical retrieval</p><p>Live online retrieval</p><p>Existing and missing users</p><p class="muted">Chronon quickstart<br>MongoDB<br>Feast Python SDK</p></div></div><p class="caption">Recorded September 13, 2026 · fc3c683d9 · Offline sample uses generated Parquet; online values come from real Chronon.</p>""",
    """Play the embedded video. It is packaged in this HTML and does not need a network connection. It has no audio, so narrate over it. First we read a small generated historical Parquet sample. Then we fetch from a real Chronon quickstart service backed by MongoDB. Finally, the checkout FeatureService fetches ten features for three users, and we compare those values to direct Chronon responses. This is a local integration demo, not a production deployment or a live-stream freshness demonstration. If video playback is unavailable, the next slide preserves the key results.""",
    ["example"],
    "demo-slide",
)
add(
    "What the demo establishes",
    """<div class="evidence"><div><strong class="big-number feast">39</strong><p>focused tests passed</p><span>31 unit + 7 offline/stub + 1 live-service</span></div><div><strong class="big-number chronon">30</strong><p>feature values matched</p><span>Feast versus direct Chronon responses</span></div></div>"""
    + table(
        ["User", "Purchases, 30d", "Refunds, 30d", "Feature status"],
        [
            ["5", "1253", "1269", "Found"],
            ["7", "1523", "1307", "Found"],
            ["999999", "null", "null", "Missing"],
        ],
        "results",
    )
    + """<p class="caption">Quickstart aggregate values, not currency-normalized business metrics. Latency, throughput, and production reliability were not benchmarked.</p>""",
    """These are measured results from our September 13 run at fc3c683d9. Do not describe 39 tests as the entire repository suite. Seven are offline and HTTP-stub integration tests; one specifically exercises the real Chronon service. The thirty comparisons cover ten features for each of three users and allow Float32 rounding. Missing values for the unknown user are part of the comparison. This establishes a working read path and selected correctness cases, not comprehensive semantic equivalence across historical and online data at scale.""",
    ["example"],
    foot="Evidence: the local demo and saved test logs, September 13, 2026",
)
add(
    "Benefits of the combination",
    rows(
        [
            (
                "A shared catalog and consumer API",
                "Model teams browse Chronon-backed definitions in Feast and retrieve features through familiar APIs.",
            ),
            (
                "Reuse of Chronon computation",
                "The adapter reads Chronon’s results without adding a second online materialization path.",
            ),
            (
                "A path for gradual adoption",
                "Teams can introduce Chronon-backed feature sets while retaining Feast definitions for model-facing use.",
            ),
        ]
    )
    + """<p class="bottom-line">The strongest benefit appears when the organization already values both layers.</p>""",
    """These are architectural benefits inferred from the integration, not benchmark results. The native Chronon service remains the serving backend, so this path does not require a second copy in a Feast-managed online store. The incremental adoption argument is about keeping consumer interfaces familiar. It is not a promise of arbitrary multi-backend composition within this PR: backend configuration and unsupported combinations still need validation. There is still metadata to author in both systems.""",
    ["online", "source", "retrieval"],
)
add(
    "Costs of the combination",
    rows(
        [
            (
                "Two definition lifecycles",
                "Chronon logic and Feast schemas can drift. Releases must align names, keys, types, and versions.",
            ),
            (
                "An additional serving boundary",
                "HTTP and serialization add work. Failures and missing results need explicit handling.",
            ),
            (
                "A larger operating surface",
                "Teams own batch jobs, streaming where used, a KV store, serving, the registry, and the adapter.",
            ),
        ]
    )
    + """<p class="bottom-line">A common API preserves the operational dependencies underneath it.</p>""",
    """Be candid about the cons. The exact overhead depends on the alternative: native Chronon clients and HTTP clients have different baselines. We have not measured a latency penalty, so avoid attaching numbers. This Python adapter uses synchronous HTTP requests. Retries are opt-in and bounded by attempt count; timeout is per attempt, not a total deadline. Backoff and Retry-After can extend latency. Benchmark the entire model request, including fanout and cold starts. Metadata release coordination and incident ownership are real costs even if the adapter code is small. A shared interface can hide differences from callers without eliminating them.""",
    ["online", "components", "source"],
)
add(
    "Time semantics still need an agreement",
    """<div class="two-col semantics"><div><h3 class="chronon">Chronon’s contract</h3><p>Aggregation window</p><p>Snapshot or temporal accuracy</p><p>Backfill and update behavior</p></div><div><h3 class="feast">The adapter’s contract</h3><p>Entity and timestamp mapping</p><p>Backward row selection and TTL</p><p>Created-time tie-breaking</p></div></div><p class="lead small-lead">Matching feature names does not establish matching historical meaning.</p><p class="caption">A snapshot row can pass an as-of join and still differ from a window evaluated at the exact training event.</p>""",
    """Chronon SNAPSHOT and TEMPORAL accuracy encode different evaluation timelines. A Feast as-of selection over exports must preserve the chosen meaning. TTL here bounds how old a materialized row can be, not the aggregation window length. For instance, a 30-day purchase sum may have an hourly publication cadence and a separate freshness target. Do not treat those three intervals as equivalent. The current created-time rule chooses the latest revision for duplicate event times; it does not reconstruct when each revision became knowable. Validate late data, revisions, time zones, nulls, and timestamp precision for the intended use case.""",
    ["groupby", "join", "offline", "consistency"],
)
add(
    "The current adapter has a narrow scope",
    """<div class="two-col limits"><div><h3 class="feast">Implemented in the PR</h3><ul><li>Python reads with explicit service errors</li><li>Opt-in HTTP retries (0–5)</li><li>Parquet reads and saved datasets</li><li>Local on-demand transforms</li></ul></div><div><h3 class="chronon">Outside the current scope</h3><ul><li>Chronon job orchestration</li><li>Feast-managed online writes</li><li>Distributed offline retrieval</li><li>Full backend feature parity</li></ul></div></div><p class="bottom-line">Only needed columns are loaded, but joins still run in pandas. Memory bounds matter.</p>""",
    """Anchor limitations to the tested PR revision, not to the projects in general. The current revision reads only required Parquet columns before doing local joins. It does not push retrieval into Spark or a warehouse engine. The retrieval job supports Parquet saved datasets and local on-demand transforms; SQL export remains unsupported. SavedDataset round-trips use provider: chronon and preserve request data and custom timestamps. These additions passed 68 targeted unit tests plus seven offline/HTTP-stub integration tests on September 14. The embedded live demo remains the September 13 recording at fc3c683d9. The PR documentation lists Python SDK support and no direct Go or Java adapter support. Online infrastructure update and teardown are no-ops. Chronon online freshness remains Chronon-owned; the historical TTL filter is not a universal online TTL guarantee. Check upstream status before presenting because the PR remains open.""",
    ["offline", "online", "pr"],
    foot="Proposed integration, not a released support commitment · Status checked September 14, 2026",
)
add(
    "Choosing the shape of the platform",
    table(
        ["Approach", "A good fit when", "Main tradeoff"],
        [
            (
                "Feast + existing pipelines",
                "Your computation stack already meets your needs.",
                "You continue owning that computation stack.",
            ),
            (
                "Chronon directly",
                "Chronon’s native APIs and workflows fit your consumers.",
                "Consumers integrate with Chronon’s interface.",
            ),
            (
                "Feast + Chronon",
                "Feast is an established consumer interface and Chronon adds needed computation capabilities.",
                "You own the integration and coordinated releases.",
            ),
        ],
        "decision",
    ),
    """This is a decision framework, not a ranking. It is reasonable to choose either system alone. Keep Feast with the pipelines you already have if those pipelines solve the problem. Use Chronon directly when its native workflow is enough. Combine them when stable Feast consumer interfaces and Chronon computation both deliver concrete value. Do not choose two platforms merely because the integration exists. Team expertise, backend compatibility, latency budget, and migration cost should drive the choice.""",
    ["chronon", "retrieval", "pr"],
)
add(
    "Production validation beyond the demo",
    rows(
        [
            (
                "Serving behavior",
                "Load-test p95/p99 latency, request fanout, timeouts, and failure behavior.",
            ),
            (
                "Historical correctness",
                "Test window boundaries, revisions, late data, and offline/online consistency.",
            ),
            (
                "Data and release contracts",
                "Check schemas, entity ordering, field mappings, and pinned Chronon versions.",
            ),
            (
                "Operating ownership",
                "Assign alerting, freshness targets, fallback policy, and backfill ownership.",
            ),
        ]
    ),
    """These are next engineering steps, not claims that the adapter already provides them. Compare against an appropriate native Chronon baseline, and use realistic feature counts and entity batch sizes. Exercise non-success rows as well as HTTP failures: both raise, but only eligible HTTP/transport failures are retried when configured. Measure memory on realistic historical data volumes. Agree how model inference behaves on null or stale features. Validate authentication and endpoint deployment in the actual environment. Make schema and semantic compatibility checks a release gate before wider adoption.""",
    ["online", "offline", "consistency"],
)
add(
    "A useful boundary for a mixed platform",
    """<div class="closing"><p class="lead">Chronon owns feature computation.<br>Feast offers a shared consumer interface.</p><p class="closing-thesis">The combination is useful when that interface<br>is worth the extra coordination.</p><p class="questions">Discussion</p><a href="https://github.com/feast-dev/feast/pull/6188" target="_blank" rel="noopener">github.com/feast-dev/feast/pull/6188</a></div>""",
    """Close on the decision, not on the number of tests. We have demonstrated a read integration that works for a concrete example. The harder platform design questions are semantic alignment and operational ownership. Invite questions about source-of-truth decisions, migration patterns, latency requirements, and historical correctness. The following two slides are backup material for deeper questions. Leave the main talk here unless the audience wants implementation detail.""",
    ["pr"],
    "closing-slide",
)
add(
    "Appendix: details worth testing",
    table(
        ["Boundary", "Current behavior", "Implication"],
        [
            (
                "HTTP request fails",
                "Opt-in retries; raises if unsuccessful",
                "Budget attempts, backoff, and fallback.",
            ),
            (
                "Chronon row is non-success",
                "Raises a runtime error",
                "Successful nulls remain missing values.",
            ),
            (
                "Result row count differs",
                "Raises a runtime error",
                "Validate the endpoint contract.",
            ),
            (
                "Entity / event-time duplicates",
                "Latest configured created timestamp wins",
                "Agree on historical revision policy.",
            ),
            (
                "Historical dataset read",
                "Required columns load into pandas",
                "Validate memory at realistic volumes.",
            ),
        ],
        "detail-table",
    ),
    """This table comes directly from the PR implementation. Row ordering is positional; there is no reconciliation by a returned entity key. Type declarations drive protobuf conversion, and field mapping applies in both request and response directions. connection_retries defaults to zero and accepts 0–5 retries through Feast’s shared HTTP session manager. It retries eligible transport failures and HTTP 429/5xx responses, including the read-only POST. HTTP 400 and per-row Chronon failures are not retried. Timeouts and TLS verification are configurable; timeout is per attempt, and Retry-After/backoff can extend total latency. These behaviors are testable boundaries, not necessarily bugs. The production design needs to decide the acceptable semantics for each one.""",
    ["online", "offline"],
    "appendix",
)
add(
    "Sources and further reading",
    """<div class="source-list">"""
    + "".join(
        f'<a href="{url}" target="_blank" rel="noopener"><span>{i:02d}</span>{label}</a>'
        for i, (label, url) in enumerate(
            [
                S[k]
                for k in [
                    "pr",
                    "example",
                    "feast",
                    "retrieval",
                    "chronon",
                    "groupby",
                    "join",
                    "consistency",
                ]
            ],
            1,
        )
    )
    + """</div><p class="caption">Implementation claims reference 55873756b. Demo evidence remains fc3c683d9, September 13, 2026.<br>Project documentation and PR status checked September 14, 2026. Slide-specific sources appear in speaker notes.</p>""",
    """Use these links for follow-up discussion. All links are to project documentation or the implementation under discussion. The artifact works offline, but following external citations requires an internet connection. The video, poster, styling, speaker notes, and navigation are included in the HTML itself. Keyboard shortcuts: arrows or Space to move, N for notes, O for overview, F for fullscreen, B to blank the screen, P for a separate presenter window, Home for the opening slide, and End for this source slide.""",
    list(S),
    "appendix",
)

css = """
:root{--bg:#0c1b2a;--fg:#f3f1e9;--muted:#aebdc8;--line:#344755;--feast:#83ddc5;--chronon:#ffb392;--scale:1}*{box-sizing:border-box}html,body{margin:0;width:100%;height:100%;overflow:hidden;background:#050c13;color:var(--fg);font-family:Arial,Helvetica,sans-serif}button,a{touch-action:manipulation}button{font:inherit;cursor:pointer}button:focus-visible,a:focus-visible,video:focus-visible{outline:3px solid var(--feast);outline-offset:5px}a{color:var(--feast);text-underline-offset:5px}#viewport{height:calc(100vh - 54px);width:100vw;position:relative}#stage{width:1600px;height:900px;position:absolute;left:50%;top:50%;transform:translate(-50%,-50%) scale(var(--scale));flex:none;transform-origin:center;box-shadow:0 20px 70px #0008}.slide{display:none;position:absolute;inset:0;background:var(--bg);padding:74px 86px 68px;overflow:hidden}.slide.active{display:block}.slide header{margin-bottom:46px}.slide h2{font-size:58px;letter-spacing:-1.8px;font-weight:600;line-height:1.08;margin:0;max-width:1380px}.slide p{font-size:29px;line-height:1.45;margin:18px 0}.slide h3{font-size:31px;line-height:1.2;margin:0 0 20px;letter-spacing:-.4px;font-weight:600}.slide footer{position:absolute;left:86px;right:86px;bottom:28px;display:flex;justify-content:space-between;align-items:center;gap:40px;font-size:18px;color:var(--muted);border-top:1px solid var(--line);padding-top:15px}.slide footer span:first-child{max-width:1260px}aside.notes{display:none}.feast{color:var(--feast)}.chronon{color:var(--chronon)}.muted{color:var(--muted)}.lead{font-size:43px!important;line-height:1.24!important;letter-spacing:-.8px;margin:0 0 38px!important}.small-lead{font-size:37px!important;margin-top:50px!important}.rows{margin-top:34px}.row{display:grid;grid-template-columns:380px 1fr;gap:58px;padding:24px 0;border-top:1px solid var(--line)}.row h3{margin:0;font-size:30px}.row p{margin:0;font-size:29px}.split{display:grid;grid-template-columns:1fr 1.05fr;gap:80px;align-items:start}.two-col{display:grid;grid-template-columns:1fr 1fr;gap:90px}.two-col>div+div{border-left:1px solid var(--line);padding-left:64px}.concepts{margin-top:65px}.concepts h3{font-size:49px}.concepts p{font-size:32px;max-width:510px}.example{color:var(--muted);font-size:26px;line-height:1.5;border-top:1px solid var(--line);padding-top:27px;margin-top:32px}.bottom-line{font-size:29px!important;margin-top:40px!important;padding-top:27px;border-top:2px solid var(--feast);line-height:1.35!important}.caption{font-size:22px!important;color:var(--muted);line-height:1.4!important;margin-top:25px!important}.code-title{font-size:22px;color:var(--muted);margin:0 0 22px}.code-block{padding-top:7px}pre{margin:0;white-space:pre-wrap;overflow-wrap:anywhere;font-family:'SFMono-Regular',Consolas,'Liberation Mono',monospace;font-size:24px;line-height:1.52;color:#dae9ef;padding:28px 0;border-top:1px solid var(--line);border-bottom:1px solid var(--line)}code{font-family:inherit}.code-split pre{font-size:25px}.code-split{gap:64px;grid-template-columns:.92fr 1.08fr}.endpoint{font:26px/1.6 'SFMono-Regular',Consolas,monospace;color:var(--feast);margin-top:38px;padding:22px 0;border-top:1px solid var(--line)}.endpoint span{color:var(--muted)}table{border-collapse:collapse;width:100%;font-size:29px;line-height:1.3}th{font-size:23px;font-weight:400;color:var(--muted);text-align:left;padding:0 25px 20px 0}td{padding:25px 26px 25px 0;border-top:1px solid var(--line);vertical-align:top}td:first-child{width:51%}.decision td{padding:31px 36px 31px 0;font-size:28px}.decision td:first-child{width:25%;color:var(--feast)}.decision td:nth-child(2){width:42%}.detail-table td{font-size:26px;padding:24px 30px 24px 0}.detail-table td:first-child{width:29%}.detail-table td:nth-child(2){width:37%}.cover{height:100%;position:relative}.cover-slide header{display:none}.cover-slide .slide-content{height:100%}.cover h1{font-size:151px;line-height:.98;letter-spacing:-8px;margin:65px 0 35px;font-weight:600}.cover .plus{color:#8da0ab;font-weight:300}.event{font-size:25px!important;letter-spacing:1px;margin:0!important}.cover-sub{font-size:41px!important;letter-spacing:-.7px}.cover-detail{color:var(--muted);font-size:26px!important}.cover-rule{position:absolute;right:24px;top:165px;width:330px;height:330px;border-top:3px solid var(--chronon);border-bottom:3px solid var(--feast);transform:rotate(-32deg)}.status{position:absolute;bottom:3px;font-size:21px!important;color:var(--muted)}.architecture{width:100%;height:560px;margin-top:30px;overflow:visible}.flow-lines{fill:none;stroke:#758d9e;stroke-width:3;color:#758d9e;marker-end:url(#arrow)}.node rect{fill:transparent;stroke:#677a88;stroke-width:2;rx:0}.node.coral rect{stroke:var(--chronon)}.node.green rect{stroke:var(--feast)}.node text{fill:var(--fg);font-size:29px;text-anchor:middle}.node text.detail{font-size:22px;fill:var(--muted)}.consumer{fill:var(--fg);font-size:27px;text-anchor:middle}.edge-label{fill:var(--muted);font-size:20px}.diagram-note{fill:var(--muted);font-size:21px}.time-line{display:grid;grid-template-columns:1fr 1fr 1fr;gap:45px;border-top:3px solid #566d7e;margin-top:75px;padding-top:30px}.time-line>div{position:relative}.time-line>div:before{content:'';position:absolute;left:0;top:-39px;width:15px;height:15px;background:var(--muted);border-radius:50%}.time-line .query:before{background:var(--feast)}.time{font-size:28px;color:var(--muted);display:block;margin-bottom:26px}.time-line strong{font-size:38px;font-weight:500}.time-line p{font-size:25px}.formula{margin-top:56px!important;font-size:38px!important;line-height:1.4!important}.formula span{font-size:27px;color:var(--muted)}.demo-layout{display:grid;grid-template-columns:1100px 1fr;gap:35px;align-items:center}.demo-layout video{width:1100px;height:595px;background:#000;object-fit:contain}.demo-slide header{margin-bottom:22px}.demo-slide .caption{font-size:20px!important;margin-top:15px!important}.demo-copy h3{color:var(--feast);font-size:38px}.demo-copy p{font-size:25px}.evidence{display:grid;grid-template-columns:1fr 1fr;gap:100px;margin-bottom:45px}.big-number{font-size:106px;letter-spacing:-5px;line-height:1}.evidence p{font-size:31px;margin:5px 0}.evidence span{font-size:21px;color:var(--muted)}.results td{padding:18px 20px 18px 0;font-size:28px}.results td:first-child{width:25%}.semantics{margin-top:63px}.semantics p{padding:12px 0;border-bottom:1px solid var(--line);font-size:30px}.limits{margin-top:67px}.limits h3{font-size:36px}.limits ul{padding-left:25px;margin:34px 0 0}.limits li{font-size:29px;line-height:1.42;margin:0 0 24px}.closing{margin-top:85px}.closing .lead{font-size:53px!important;line-height:1.3!important}.closing-thesis{font-size:34px!important;color:var(--muted);margin-top:45px!important}.questions{font-size:33px!important;margin-top:55px!important}.closing a{font-size:25px}.source-list{display:grid;grid-template-columns:1fr 1fr;gap:0 72px;margin-top:60px}.source-list a{padding:25px 0;border-top:1px solid var(--line);font-size:27px;text-decoration:none;display:flex;gap:24px;align-items:center}.source-list a span{font-size:20px;color:var(--muted)}.source-list a:hover{text-decoration:underline}#toolbar{height:54px;position:fixed;bottom:0;inset-inline:0;display:flex;justify-content:space-between;align-items:center;padding:0 24px;color:#b4c4cf;background:#050c13;font-size:13px;z-index:4}#toolbar button{background:transparent;color:#dae3e9;border:0;padding:10px 12px;border-radius:3px}#toolbar button:hover{background:#1e3243}#progress{position:fixed;bottom:54px;left:0;height:2px;background:var(--feast);z-index:5;transition:width .18s}#notes-panel{display:none;position:fixed;right:0;top:0;bottom:54px;width:400px;padding:27px;background:#162938;overflow:auto;z-index:3;border-left:1px solid #456}#notes-panel h2{font-size:23px;line-height:1.25}#notes-panel p,#notes-panel li{font-size:16px;line-height:1.6;color:#d7e1e8}#notes-panel li{margin:12px 0}body.show-notes #notes-panel{display:block}body.show-notes #viewport{width:calc(100vw - 400px)}#overview{display:none;position:fixed;inset:0 0 54px;background:#091722f5;z-index:10;overflow:auto;padding:35px}#overview.open{display:block}#overview h2{font-size:30px}#overview-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:15px}#overview button{min-height:105px;text-align:left;padding:20px;background:#142a3b;color:var(--fg);border:1px solid #3b5363;font-size:19px;line-height:1.35}#overview button span{display:block;color:var(--feast);font-size:13px;margin-bottom:10px}#blackout{display:none;position:fixed;inset:0;z-index:100;background:#000}#blackout.on{display:block}.help{font-size:12px;color:#90a6b5}:fullscreen #toolbar{opacity:.12}:fullscreen #toolbar:hover{opacity:1}@media(prefers-reduced-motion:reduce){*{transition:none!important}}@media(max-width:720px){#toolbar{padding:0 6px}#toolbar .help{display:none}#toolbar button{padding:10px 7px}#notes-panel{width:min(400px,100vw);z-index:8}body.show-notes #viewport{width:100vw}#overview-grid{grid-template-columns:repeat(2,1fr)}}@media print{@page{size:1600px 900px;margin:0}html,body{height:auto;overflow:visible;background:white}#viewport{display:block;height:auto;width:auto}#stage{transform:none!important;position:static;left:auto;top:auto;width:1600px;height:auto;box-shadow:none}.slide{display:block!important;position:relative;width:1600px;height:900px;break-after:page;print-color-adjust:exact;-webkit-print-color-adjust:exact}#toolbar,#progress,#notes-panel,#overview,#blackout{display:none!important}.slide:last-child{break-after:auto}}
"""

css += "\n.checkout-code-slide header{margin-bottom:30px}.checkout-walkthrough{display:grid;gap:18px}.checkout-step{display:grid;grid-template-columns:950px 1fr;gap:42px;align-items:center}.checkout-step pre{font-size:24px;line-height:1.27;background:#091522;border:0;padding:12px 16px;color:#9bb0be;white-space:pre;overflow-wrap:normal}.checkout-step code>span{display:block;min-height:1.27em}.checkout-step .code-emphasis.chronon{background:#ffb39210;color:var(--chronon)}.checkout-step .code-emphasis.feast{background:#83ddc510;color:var(--feast)}.checkout-explanation .step-no{font-size:19px;display:block;margin-bottom:9px}.checkout-explanation h3{font-size:28px;margin-bottom:11px;line-height:1.15}.checkout-explanation p{font-size:24px;line-height:1.35;margin:0}.checkout-explanation code{font-size:22px;overflow-wrap:anywhere}.checkout-code-slide .caption{font-size:20px!important;margin-top:20px!important}\n"

css += "\n.feast-ui-slide{padding:0;background:#f6f8fc}.feast-ui-slide header{display:none}.feast-ui-capture{display:block;width:1600px;height:900px;object-fit:contain}.feast-ui-slide footer{left:0;right:0;bottom:0;padding:12px 30px;border:0;background:#0c1b2a;color:#f3f1e9;font-size:20px}\n"

parts = []
for n, s in enumerate(slides, 1):
    links = "".join(
        f'<li><a href="{html.escape(S[k][1])}" target="_blank" rel="noopener">{html.escape(S[k][0])}</a></li>'
        for k in s["refs"]
    )
    foot = s["foot"] or ("Appendix" if "appendix" in s["kind"] else "Feast + Chronon")
    parts.append(
        f'<section class="slide {s["kind"]} {"active" if n == 1 else ""}" data-title="{html.escape(s["title"])}" aria-label="Slide {n}: {html.escape(s["title"])}" aria-hidden="{"false" if n == 1 else "true"}"><header><h2>{s["title"]}</h2></header><div class="slide-content">{s["body"]}</div><footer><span>{foot}</span><span>{n:02d} / {len(slides):02d}</span></footer><aside class="notes"><p>{html.escape(s["notes"])}</p><h3>Sources</h3><ul>{links}</ul></aside></section>'
    )
js = """
const slides=[...document.querySelectorAll('.slide')];let index=0,presenter=null;const stage=document.getElementById('stage'), viewport=document.getElementById('viewport');
function resize(){const r=viewport.getBoundingClientRect();document.documentElement.style.setProperty('--scale',Math.min(r.width/1600,r.height/900));}
function presenterUpdate(){if(presenter&&!presenter.closed){const d=presenter.document;d.getElementById('slide-num').textContent=`${index+1} / ${slides.length}`;d.getElementById('current').textContent=slides[index].dataset.title;d.getElementById('next').textContent=slides[index+1]?.dataset.title||'End';d.getElementById('script').innerHTML=slides[index].querySelector('.notes').innerHTML;}}
function go(n){const next=Math.max(0,Math.min(slides.length-1,n));if(next!==index)slides[index].querySelectorAll('video').forEach(v=>v.pause());index=next;slides.forEach((s,i)=>{s.classList.toggle('active',i===index);s.setAttribute('aria-hidden',String(i!==index));s.inert=i!==index;});document.getElementById('count').textContent=`${index+1} / ${slides.length}`;document.getElementById('progress').style.width=`${(index+1)/slides.length*100}%`;document.getElementById('notes-title').textContent=slides[index].dataset.title;document.getElementById('notes-content').innerHTML=slides[index].querySelector('.notes').innerHTML;document.getElementById('prev').disabled=index===0;document.getElementById('next-btn').disabled=index===slides.length-1;history.replaceState(null,'',`#slide-${index+1}`);document.title=`${index+1}. ${slides[index].dataset.title} | Feast + Chronon`;presenterUpdate();}
function notes(){document.body.classList.toggle('show-notes');document.getElementById('notes-btn').setAttribute('aria-pressed',String(document.body.classList.contains('show-notes')));resize();}
function overview(){const o=document.getElementById('overview');o.classList.toggle('open');if(o.classList.contains('open'))o.querySelectorAll('button')[index].focus();else document.getElementById('overview-btn').focus();}
function blank(){document.getElementById('blackout').classList.toggle('on');}
function fullscreen(){if(document.fullscreenElement)document.exitFullscreen?.();else document.documentElement.requestFullscreen?.().catch(()=>{});}
function openPresenter(){presenter=window.open('','feast-chronon-presenter','width=900,height=850');if(!presenter){notes();return;}presenter.document.write(`<!doctype html><html lang="en"><meta charset="utf-8"><title>Presenter — Feast + Chronon</title><style>body{background:#102232;color:#f3f1e9;font:19px/1.6 Arial;margin:36px}small{color:#8bdcc6}h1{font-size:34px;line-height:1.2}a{color:#83ddc5}button{font:inherit;background:#254458;color:white;border:1px solid #647a88;padding:9px 23px;cursor:pointer}#next{color:#aebdc8}#script{border-top:1px solid #405565;margin-top:30px;padding-top:10px}li{margin:8px 0}</style><small id="slide-num"></small><h1 id="current"></h1><p>Next: <span id="next"></span></p><button id="back">Previous</button> <button id="forward">Next</button><div id="script"></div></html>`);presenter.document.close();presenter.document.getElementById('back').onclick=()=>go(index-1);presenter.document.getElementById('forward').onclick=()=>go(index+1);presenter.onkeydown=e=>{if(e.key==='ArrowRight'||e.key===' '){e.preventDefault();go(index+1)}if(e.key==='ArrowLeft'){e.preventDefault();go(index-1)}};presenterUpdate();}
document.getElementById('prev').onclick=()=>go(index-1);document.getElementById('next-btn').onclick=()=>go(index+1);document.getElementById('notes-btn').onclick=notes;document.getElementById('overview-btn').onclick=overview;document.getElementById('full-btn').onclick=fullscreen;document.getElementById('presenter-btn').onclick=openPresenter;document.getElementById('blackout').onclick=blank;
slides.forEach((s,i)=>{const b=document.createElement('button');b.innerHTML=`<span>${String(i+1).padStart(2,'0')}</span>${s.dataset.title}`;b.onclick=()=>{go(i);overview();};document.getElementById('overview-grid').append(b);});
document.addEventListener('keydown',e=>{if(e.target.closest('video,input,textarea,select'))return;if(e.target.closest('button,a')&&(e.key===' '||e.key==='Enter'))return;if(e.ctrlKey||e.metaKey||e.altKey)return;const k=e.key.toLowerCase();if(k==='escape'){document.getElementById('overview').classList.remove('open');document.getElementById('blackout').classList.remove('on');return;}if(document.getElementById('overview').classList.contains('open')&&k!=='o')return;if(['arrowright','pagedown',' '].includes(k)){e.preventDefault();go(index+1)}else if(['arrowleft','pageup'].includes(k)){e.preventDefault();go(index-1)}else if(k==='home'){e.preventDefault();go(0)}else if(k==='end'){e.preventDefault();go(slides.length-1)}else if(k==='n')notes();else if(k==='o')overview();else if(k==='f')fullscreen();else if(k==='b')blank();else if(k==='p')openPresenter();});
let touchX=null;viewport.addEventListener('touchstart',e=>{if(!e.target.closest('video,a,button'))touchX=e.touches[0].clientX;},{passive:true});viewport.addEventListener('touchend',e=>{if(touchX!==null){const d=e.changedTouches[0].clientX-touchX;if(Math.abs(d)>60)go(index+(d<0?1:-1));touchX=null;}},{passive:true});window.addEventListener('resize',resize);window.addEventListener('hashchange',()=>{const n=parseInt(location.hash.replace('#slide-',''));if(Number.isFinite(n))go(n-1)});const initial=parseInt(location.hash.replace('#slide-',''));go(Number.isFinite(initial)?initial-1:0);resize();
"""
page = (
    '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta name="description" content="Feature Store Summit presentation on using Feast and Chronon together, including architecture, recorded demo, benefits and limitations."><title>Feast + Chronon</title><style>'
    + css
    + '</style></head><body><main id="viewport"><div id="stage">'
    + "".join(parts)
    + '</div></main><div id="progress"></div><nav id="toolbar" aria-label="Presentation controls"><div><button id="prev" aria-label="Previous slide">←</button><span id="count" aria-live="polite"></span><button id="next-btn" aria-label="Next slide">→</button></div><span class="help">Arrows: navigate · N: notes · O: overview · F: fullscreen · B: blank</span><div><button id="overview-btn">Overview</button><button id="notes-btn" aria-pressed="false">Notes</button><button id="presenter-btn">Presenter</button><button id="full-btn">Fullscreen</button></div></nav><aside id="notes-panel"><h2 id="notes-title"></h2><div id="notes-content"></div></aside><div id="overview" role="dialog" aria-label="Slide overview"><h2>Slide overview <small style="font-size:16px;color:#9fb4c2">Esc to close</small></h2><div id="overview-grid"></div></div><div id="blackout" aria-label="Blank screen"></div><script>'
    + js
    + "</script></body></html>"
)
(ROOT / "feast-chronon-summit.html").write_text(page, encoding="utf-8")
(ROOT / "speaker-notes.md").write_text(
    "# Feast + Chronon\n\n20 main slides and 2 appendix slides. Approximately 20 minutes plus questions.\n\n"
    + "\n\n".join(
        f"## {i}. {s['title']}\n\n{s['notes']}\n\n"
        + "\n".join(f"- [{S[k][0]}]({S[k][1]})" for k in s["refs"])
        for i, s in enumerate(slides, 1)
    ),
    encoding="utf-8",
)
(ROOT / "deck-content.json").write_text(
    json.dumps([{k: v for k, v in s.items() if k != "body"} for s in slides], indent=2),
    encoding="utf-8",
)
print(
    f"Built {len(slides)} slides: {(ROOT / 'feast-chronon-summit.html').stat().st_size:,} bytes"
)
