package onlinestore

// Bigtable online store for the Go feature server. It reads what the Python BigtableOnlineStore
// (sdk/python/feast/infra/online_stores/bigtable.py) writes, so table names, row keys and the event_ts
// encoding must match Python exactly.

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
	"github.com/spaolacci/murmur3"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/oauth2/google"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

const (
	// In benchmarks, a ReadRows of many row keys took longer than the same keys sent as several small
	// ReadRows in parallel, so the defaults favor many small requests in parallel. They wait on Bigtable
	// rather than CPU, so concurrency is not tied to GOMAXPROCS.
	bigtableDefaultKeysPerRequest      = 5
	bigtableDefaultMaxConcurrentChunks = 50
	bigtableMaxKeysPerRequest          = 100

	// bigtableDefaultReadTimeout bounds a whole read call when read_timeout_ms is unset. Without a deadline
	// the Go client retries some errors indefinitely. 60 s is a generous ceiling, not a latency target.
	bigtableDefaultReadTimeout = 60 * time.Second

	// Table name limits, as in Python's BigtableOnlineStore._get_table_name.
	bigtableTableNameMaxLength    = 50
	bigtableEntitiesPartMaxLength = 24
	bigtableProjectReadableLength = 10

	// bigtableSerializationVersion is the only entity key serialization version the Bigtable store reads.
	bigtableSerializationVersion = 3

	// naiveISOLayout is datetime.isoformat() of a datetime without a timezone, read as UTC.
	naiveISOLayout = "2006-01-02T15:04:05.999999999"
	// secondsOffsetISOLayout is datetime.isoformat() with a seconds UTC offset (+05:53:28), which RFC 3339
	// cannot express.
	secondsOffsetISOLayout = "2006-01-02T15:04:05.999999999-07:00:00"

	// bigtableFeatureFamily is the column family Python writes every cell to.
	bigtableFeatureFamily = "features"
	// bigtableEventTSQualifier is the qualifier of a row's event timestamp.
	bigtableEventTSQualifier = "event_ts"

	// bigtableStartupTimeout bounds the store's own PingAndWarm call after the client is built.
	bigtableStartupTimeout = 10 * time.Second
)

// bigtableReadRejectedError is the error of a read that max_inflight_reads turned away because the server
// was at its cap. Its gRPC status is RESOURCE_EXHAUSTED, so the gRPC feature server returns that code and
// a client can back off or retry on another replica.
type bigtableReadRejectedError struct{ msg string }

func (e *bigtableReadRejectedError) Error() string { return e.msg }

// GRPCStatus gives the error the RESOURCE_EXHAUSTED code; status.FromError finds it through wrapping too.
func (e *bigtableReadRejectedError) GRPCStatus() *status.Status {
	return status.New(codes.ResourceExhausted, e.msg)
}

// Prometheus metrics of online reads, served on the feature server's metrics port. A vector exports
// nothing until a read records into it, so other stores' deployments see no Bigtable series.
var (
	bigtableReadDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "feast",
		Subsystem: "bigtable",
		Name:      "online_read_duration_seconds",
		Help:      "Time taken by one Bigtable online read call, including waits for slots, by status (ok, error or rejected).",
		Buckets:   []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"status"})
	bigtableReadChunks = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "feast",
		Subsystem: "bigtable",
		Name:      "online_read_chunks_total",
		Help:      "ReadRows calls sent by Bigtable online reads.",
	}, []string{"table"})
	bigtableReadRowsFound = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "feast",
		Subsystem: "bigtable",
		Name:      "online_read_rows_found_total",
		Help:      "Rows returned by Bigtable online reads.",
	}, []string{"table"})
)

// BigtableOnlineStore reads features that the Python BigtableOnlineStore wrote. Tables are named after
// each feature view's entities, so reads go through OnlineReadWithEntities (EntityAwareOnlineStore).
type BigtableOnlineStore struct {
	project              string // Feast project, the first part of every table name
	cfg                  bigtableConfig
	serializationVersion int64
	client               *bigtable.Client
	tables               sync.Map // table name -> *bigtable.Table
	closeOnce            sync.Once
	// inflight holds the max_inflight_reads slots that admitted reads reserve; nil is unlimited.
	inflight *semaphore.Weighted
}

var (
	_ OnlineStore            = (*BigtableOnlineStore)(nil)
	_ EntityAwareOnlineStore = (*BigtableOnlineStore)(nil)
)

// NewBigtableOnlineStore builds the store from the online_store section of feature_store.yaml. An unset
// project_id falls back to GOOGLE_CLOUD_PROJECT, GCLOUD_PROJECT, then the default credentials' project.
//
// Outside the emulator the client primes its connections with PingAndWarm when it is created (three
// attempts each), so bad credentials or an unreachable instance fail client construction. The store then
// pings once more: that gives the emulator and pre-dialed connections, which skip priming, the same check,
// and it logs a warning for NotFound, which the client's priming treats as success and logs only at debug
// level (see tolerableStartupPingError).
func NewBigtableOnlineStore(project string, config *registry.RepoConfig, onlineStoreConfig map[string]interface{}) (*BigtableOnlineStore, error) {
	cfg, err := parseBigtableConfig(onlineStoreConfig)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	if cfg.ProjectID, err = resolveProjectID(ctx, cfg.ProjectID, defaultCredentialsProjectID); err != nil {
		return nil, err
	}
	return newBigtableOnlineStore(ctx, project, config, cfg, bigtableStartupTimeout)
}

// newBigtableOnlineStore validates cfg, builds the client for an already resolved project and warms it
// under startupTimeout. Tests pass opts to dial bttest.
func newBigtableOnlineStore(ctx context.Context, project string, config *registry.RepoConfig, cfg bigtableConfig,
	startupTimeout time.Duration, opts ...option.ClientOption) (*BigtableOnlineStore, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	version, err := normalizeSerializationVersion(config.EntityKeySerializationVersion)
	if err != nil {
		return nil, err
	}
	client, err := bigtable.NewClientWithConfig(ctx, cfg.ProjectID, cfg.Instance, bigtableClientConfig(cfg), opts...)
	if err != nil {
		return nil, fmt.Errorf("bigtable online store: create client for projects/%s/instances/%s: %w",
			cfg.ProjectID, cfg.Instance, err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, startupTimeout)
	defer cancel()
	if err := client.PingAndWarm(pingCtx); err != nil {
		reason, ok := tolerableStartupPingError(err, os.Getenv("BIGTABLE_EMULATOR_HOST") != "")
		if !ok {
			// The ping error is the one to return; a Close error on a client that never served adds nothing.
			_ = client.Close()
			return nil, fmt.Errorf("bigtable online store: PingAndWarm projects/%s/instances/%s: %w",
				cfg.ProjectID, cfg.Instance, err)
		}
		log.Warn().Err(err).
			Str("project_id", cfg.ProjectID).
			Str("instance", cfg.Instance).
			Msgf("Bigtable PingAndWarm failed, starting anyway: %s", reason)
	}
	log.Info().
		Str("project_id", cfg.ProjectID).
		Str("instance", cfg.Instance).
		Str("app_profile", appProfileForLog(cfg.AppProfile)).
		Int64("entity_key_serialization_version", version).
		Str("metrics", bigtableMetricsMode(cfg)).
		Int("keys_per_request", cfg.KeysPerRequest).
		Int("max_concurrent_chunks", cfg.MaxConcurrentChunks).
		Bool("adaptive_chunking", cfg.AdaptiveChunking).
		Int("max_inflight_reads", cfg.MaxInflightReads).
		Int64("max_inflight_wait_ms", cfg.MaxInflightWait.Milliseconds()).
		Int64("read_timeout_ms", cfg.ReadTimeout.Milliseconds()).
		Msg("Using Bigtable online store")
	store := &BigtableOnlineStore{project: project, cfg: cfg, serializationVersion: version, client: client}
	if cfg.MaxInflightReads > 0 {
		store.inflight = semaphore.NewWeighted(int64(cfg.MaxInflightReads))
	}
	return store, nil
}

// bigtableClientConfig keeps the client library's defaults apart from the app profile and, with
// disable_metrics, the no-op metrics provider (no Cloud Monitoring export, so no monitoring permission).
func bigtableClientConfig(cfg bigtableConfig) bigtable.ClientConfig {
	config := bigtable.ClientConfig{AppProfile: cfg.AppProfile}
	if cfg.DisableMetrics {
		config.MetricsProvider = bigtable.NoopMetricsProvider{}
	}
	return config
}

// tolerableStartupPingError reports whether the store may start despite err from the startup PingAndWarm,
// and why. Bigtable returns NotFound for an instance with no tables yet, so failing on it would crash-loop
// a server started before the first `feast apply`; the client library treats it as a successful prime too.
// Emulators older than cloud.google.com/go/bigtable v1.15.0 return Unimplemented. Anything else fails.
func tolerableStartupPingError(err error, emulator bool) (reason string, ok bool) {
	switch status.Code(err) {
	case codes.NotFound:
		return "the instance has no tables yet, or the instance or app profile does not exist; " +
			"reads fail until both exist", true
	case codes.Unimplemented:
		if emulator {
			return "the Bigtable emulator does not implement PingAndWarm", true
		}
	}
	return "", false
}

// bigtableProjectEnvVars are the environment variables Python's google.auth.default reads for the project,
// in its order, before the credentials' own project.
var bigtableProjectEnvVars = []string{"GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT"}

// resolveProjectID returns configured when set, then the first non-empty bigtableProjectEnvVars value, and
// otherwise the project that find reports (the default credentials' project in production). Tests pass
// their own find, so they never look up credentials.
func resolveProjectID(ctx context.Context, configured string, find func(context.Context) (string, error)) (string, error) {
	if configured != "" {
		return configured, nil
	}
	for _, name := range bigtableProjectEnvVars {
		if v := os.Getenv(name); v != "" {
			return v, nil
		}
	}
	found, err := find(ctx)
	if err != nil {
		return "", fmt.Errorf("bigtable online store: project_id is not set and default credentials failed: %w", err)
	}
	if found == "" {
		return "", errors.New("bigtable online store: project_id is not set, GOOGLE_CLOUD_PROJECT and " +
			"GCLOUD_PROJECT are empty, and the default credentials name no project")
	}
	return found, nil
}

func defaultCredentialsProjectID(ctx context.Context) (string, error) {
	creds, err := google.FindDefaultCredentials(ctx, bigtable.Scope)
	if err != nil {
		return "", err
	}
	return creds.ProjectID, nil
}

func appProfileForLog(profile string) string {
	if profile == "" {
		return "default"
	}
	return profile
}

// bigtableMetricsMode names the client's metrics setting for the startup log; the client turns metrics off
// itself for the emulator.
func bigtableMetricsMode(cfg bigtableConfig) string {
	if cfg.DisableMetrics {
		return "disabled (disable_metrics)"
	}
	if os.Getenv("BIGTABLE_EMULATOR_HOST") != "" {
		return "disabled (emulator)"
	}
	return "default"
}

// OnlineRead is unsupported: the store needs each feature view's entity names to find its table. The
// feature server calls OnlineReadWithEntities (EntityAwareOnlineStore) instead, so it never reaches this.
func (s *BigtableOnlineStore) OnlineRead(context.Context, []*types.EntityKey, []string, []string) ([][]FeatureData, error) {
	return nil, fmt.Errorf("bigtable online store: OnlineRead needs each feature view's entity names, "+
		"use OnlineReadWithEntities: %w", errors.ErrUnsupported)
}

// OnlineReadWithEntities reads featureNames[i] of featureViewNames[i] for every entity key and returns
// what the Python store returns for the same rows. entityNamesByView names each view's table.
//
// The result has one row per entity key and one slot per requested feature, both in input order. An
// entity with no row in any requested view gets a nil row. A view row absent beside a present one gives
// NullVal slots with a zero timestamp; a feature absent from a present row gives NullVal with the row's
// event_ts. A corrupt cell or a missing event_ts fails the whole read.
//
// read_timeout_ms bounds the whole call, including waits for slots; the caller's deadline still applies.
// A deadline or cancellation error wraps context.DeadlineExceeded or context.Canceled.
func (s *BigtableOnlineStore) OnlineReadWithEntities(ctx context.Context, entityKeys []*types.EntityKey,
	featureViewNames []string, featureNames []string, entityNamesByView map[string][]string) (rows [][]FeatureData, err error) {
	ctx, span := tracer.Start(ctx, "bigtable.OnlineRead")
	stats := &bigtableReadStats{entities: len(entityKeys), start: time.Now()}
	defer func() { stats.end(span, err) }()
	ctx, cancel := s.withReadTimeout(ctx)
	defer cancel()

	if len(featureViewNames) != len(featureNames) {
		return nil, fmt.Errorf("bigtable online store: featureViewNames has %d entries but featureNames has %d",
			len(featureViewNames), len(featureNames))
	}
	plan, err := planBigtableRead(s.project, featureViewNames, featureNames, entityNamesByView)
	if err != nil {
		return nil, err
	}
	stats.tables = plan.tables
	if len(entityKeys) == 0 || len(featureNames) == 0 {
		return make([][]FeatureData, len(entityKeys)), nil
	}
	hashes, err := s.entityHashes(entityKeys)
	if err != nil {
		return nil, err
	}
	result := newBigtableReadResult(len(entityKeys), len(featureNames), len(plan.views))
	chunks := s.planChunks(plan.tables, hashes, stats)
	if err := s.readChunks(ctx, chunks, result, stats); err != nil {
		return nil, err
	}
	return result.applyMissingData(plan.views), nil
}

// withReadTimeout bounds ctx by read_timeout_ms; when that budget fires, context.Cause names it.
func (s *BigtableOnlineStore) withReadTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeoutCause(ctx, s.cfg.ReadTimeout, fmt.Errorf("bigtable online store: read_timeout_ms %d exceeded: %w",
		s.cfg.ReadTimeout.Milliseconds(), context.DeadlineExceeded))
}

// bigtableReadStats feed a call's span and metrics; fields not reached before an error stay zero.
// inflightWait is the time the call waited for admission to max_inflight_reads; callSlotWait sums, over its
// chunks, the time spent waiting for one of the call's own slots. Together they separate queueing from
// Bigtable time. rounds is planned, not observed: the chunk count divided by the call's slots, rounded up.
type bigtableReadStats struct {
	start                                                   time.Time
	tables                                                  []*bigtableTableRead
	planned                                                 []*bigtableChunk
	entities, rowKeys, chunkSize, chunks, rounds, rowsFound int
	callSlotWait, inflightWait                              time.Duration
}

// end records the call's metrics, sets the span's attributes and ends the span.
func (st *bigtableReadStats) end(span trace.Span, err error) {
	outcome := "ok"
	switch {
	case errors.As(err, new(*bigtableReadRejectedError)):
		outcome = "rejected"
	case err != nil:
		outcome = "error"
	}
	bigtableReadDuration.WithLabelValues(outcome).Observe(time.Since(st.start).Seconds())
	st.recordTableCounters()
	if span.IsRecording() {
		names := make([]string, len(st.tables))
		for i, tbl := range st.tables {
			names[i] = tbl.name
		}
		span.SetAttributes(
			attribute.String("bigtable.table", strings.Join(names, ",")),
			attribute.Int("feast.entity_count", st.entities),
			attribute.Int("bigtable.row_keys", st.rowKeys),
			attribute.Int("bigtable.chunk_size", st.chunkSize),
			attribute.Int("bigtable.chunks", st.chunks),
			attribute.Int("bigtable.chunk_rounds", st.rounds),
			attribute.Float64("bigtable.call_slot_wait_ms", float64(st.callSlotWait.Microseconds())/1000),
			attribute.Float64("bigtable.inflight_wait_ms", float64(st.inflightWait.Microseconds())/1000),
			attribute.Int("bigtable.rows_found", st.rowsFound),
		)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, err.Error())
		}
	}
	span.End()
}

// recordTableCounters adds each table's sent chunks and found rows to the counters, one update per table.
// planChunks keeps a table's chunks together, and readChunks has waited for them all.
func (st *bigtableReadStats) recordTableCounters() {
	for i := 0; i < len(st.planned); {
		table := st.planned[i].tableName
		sent, rows := 0, 0
		for ; i < len(st.planned) && st.planned[i].tableName == table; i++ {
			if st.planned[i].dispatched {
				sent++
				rows += st.planned[i].rowsFound
			}
		}
		if sent > 0 {
			bigtableReadChunks.WithLabelValues(table).Add(float64(sent))
			bigtableReadRowsFound.WithLabelValues(table).Add(float64(rows))
		}
	}
}

// bigtableFeatureSlot is one requested feature of a view and its position in every output row.
type bigtableFeatureSlot struct {
	name  string
	index int
}

// bigtableViewRead is one requested feature view: its features in request order (slots), each name's
// output positions (two when requested twice), and its index into the found flags (order).
type bigtableViewRead struct {
	name      string
	order     int
	slots     []bigtableFeatureSlot
	positions map[string][]int
}

// bigtableTableRead is one Bigtable table and the requested views stored in it.
type bigtableTableRead struct {
	name  string
	views []*bigtableViewRead
}

// bigtableReadPlan is a request grouped by view (in first-seen order) and by table.
type bigtableReadPlan struct {
	views  []*bigtableViewRead
	tables []*bigtableTableRead
}

// planBigtableRead groups the requested features by view, then the views by table.
func planBigtableRead(project string, featureViewNames, featureNames []string,
	entityNamesByView map[string][]string) (*bigtableReadPlan, error) {
	plan := &bigtableReadPlan{views: groupBigtableFeaturesByView(featureViewNames, featureNames)}
	byName := make(map[string]*bigtableTableRead)
	for _, view := range plan.views {
		entityNames, ok := entityNamesByView[view.name]
		if !ok {
			return nil, fmt.Errorf("bigtable online store: no entity names for feature view %s", view.name)
		}
		name := bigtableTableName(project, entityNames)
		tbl, ok := byName[name]
		if !ok {
			tbl = &bigtableTableRead{name: name}
			byName[name] = tbl
			plan.tables = append(plan.tables, tbl)
		}
		tbl.views = append(tbl.views, view)
	}
	return plan, nil
}

// groupBigtableFeaturesByView pairs featureViewNames[i] with featureNames[i] and output position i.
func groupBigtableFeaturesByView(featureViewNames, featureNames []string) []*bigtableViewRead {
	byName := make(map[string]*bigtableViewRead)
	var views []*bigtableViewRead
	for i, viewName := range featureViewNames {
		view, ok := byName[viewName]
		if !ok {
			view = &bigtableViewRead{name: viewName, order: len(views), positions: make(map[string][]int)}
			byName[viewName] = view
			views = append(views, view)
		}
		feature := featureNames[i]
		view.slots = append(view.slots, bigtableFeatureSlot{name: feature, index: i})
		view.positions[feature] = append(view.positions[feature], i)
	}
	return views
}

// qualifierPattern matches the requested qualifiers of the table's views plus event_ts. Python builds the
// same pattern but does not escape names, so a name with regex metacharacters matches more there.
func (t *bigtableTableRead) qualifierPattern() string {
	seen := make(map[string]bool)
	var names []string
	for _, view := range t.views {
		for name := range view.positions {
			if !seen[name] {
				seen[name] = true
				names = append(names, regexp.QuoteMeta(name))
			}
		}
	}
	sort.Strings(names)
	return "^(" + strings.Join(append(names, bigtableEventTSQualifier), "|") + ")$"
}

// readFilter keeps the requested qualifiers and event_ts, newest cell only. Keep LatestNFilter(1): without
// it a read returns older cell versions whenever max_versions is above 1, and Bigtable's in-memory tier
// never serves those, so every read would go to storage (TestBigtableReadFilterKeepsNewestCellOnly).
func (t *bigtableTableRead) readFilter() bigtable.Filter {
	return bigtable.ChainFilters(bigtable.ColumnFilter(t.qualifierPattern()), bigtable.LatestNFilter(1))
}

// entityHashes returns the row-key hash of every entity key, in input order.
func (s *BigtableOnlineStore) entityHashes(entityKeys []*types.EntityKey) ([]string, error) {
	hashes := make([]string, len(entityKeys))
	for i, key := range entityKeys {
		hash, err := bigtableEntityHash(key, s.serializationVersion)
		if err != nil {
			return nil, fmt.Errorf("bigtable online store: serialize entity key %d: %w", i, err)
		}
		hashes[i] = hash
	}
	return hashes, nil
}

// bigtableRowTarget is the view of a row key and the entity positions that produced it (several when an
// entity key is repeated).
type bigtableRowTarget struct {
	view     *bigtableViewRead
	entities []int
}

// rowTargets builds one row key per unique entity per view of the table, in first-seen order.
func (t *bigtableTableRead) rowTargets(hashes []string) (map[string]*bigtableRowTarget, bigtable.RowList) {
	targets := make(map[string]*bigtableRowTarget, len(hashes)*len(t.views))
	keys := make(bigtable.RowList, 0, len(hashes)*len(t.views))
	for _, view := range t.views {
		for i, hash := range hashes {
			key := bigtableRowKeyFromHash(hash, view.name)
			if target, ok := targets[key]; ok {
				target.entities = append(target.entities, i)
				continue
			}
			targets[key] = &bigtableRowTarget{view: view, entities: []int{i}}
			keys = append(keys, key)
		}
	}
	return targets, keys
}

// bigtableReadResult is the preallocated output, plus a found flag per (entity, view) at
// entity*numViews + view.order.
type bigtableReadResult struct {
	rows     [][]FeatureData
	found    []bool
	numViews int
}

func newBigtableReadResult(numEntities, numFeatures, numViews int) *bigtableReadResult {
	rows := make([][]FeatureData, numEntities)
	for i := range rows {
		rows[i] = make([]FeatureData, numFeatures)
	}
	return &bigtableReadResult{rows: rows, found: make([]bool, numEntities*numViews), numViews: numViews}
}

// bigtableChunk is the row keys of one table read with one ReadRows. dispatched (set just before the
// ReadRows is sent) and rowsFound are written only by the chunk's goroutine, and read only after readChunks'
// group.Wait.
type bigtableChunk struct {
	tableName  string
	table      *bigtable.Table
	targets    map[string]*bigtableRowTarget // shared by the table's chunks, read-only once built
	keys       bigtable.RowList
	filter     bigtable.ReadOption
	dispatched bool
	rowsFound  int
}

// planChunks builds every table's row keys and cuts each table's keys into chunks of bigtableChunkSize.
func (s *BigtableOnlineStore) planChunks(tables []*bigtableTableRead, hashes []string, stats *bigtableReadStats) []*bigtableChunk {
	type tableKeys struct {
		tbl     *bigtableTableRead
		targets map[string]*bigtableRowTarget
		keys    bigtable.RowList
	}
	built := make([]tableKeys, len(tables))
	for i, tbl := range tables {
		targets, keys := tbl.rowTargets(hashes)
		built[i] = tableKeys{tbl: tbl, targets: targets, keys: keys}
		stats.rowKeys += len(keys)
	}
	size := bigtableChunkSize(s.cfg, stats.rowKeys)
	stats.chunkSize = size
	var chunks []*bigtableChunk
	for _, b := range built {
		handle, filter := s.table(b.tbl.name), bigtable.RowFilter(b.tbl.readFilter())
		for part := range slices.Chunk(b.keys, size) {
			chunks = append(chunks, &bigtableChunk{tableName: b.tbl.name, table: handle, targets: b.targets, keys: part, filter: filter})
		}
	}
	stats.chunks = len(chunks)
	stats.planned = chunks
	return chunks
}

// bigtableChunkSize is the row keys per ReadRows for a call of rowKeys keys: keys_per_request, or with
// adaptive_chunking the smallest size from keys_per_request up to 100 that sends the call in one round of
// max_concurrent_chunks. Each table is cut separately, so a call over several tables may plan a few
// chunks more than one round; those wait for a slot.
func bigtableChunkSize(cfg bigtableConfig, rowKeys int) int {
	if !cfg.AdaptiveChunking {
		return cfg.KeysPerRequest
	}
	// Ceiling division without rowKeys+MaxConcurrentChunks-1, which could overflow a 32-bit int.
	size := rowKeys / cfg.MaxConcurrentChunks
	if rowKeys%cfg.MaxConcurrentChunks != 0 {
		size++
	}
	return min(max(size, cfg.KeysPerRequest), bigtableMaxKeysPerRequest)
}

// readChunks reads the chunks with up to slots of them in flight, where slots is the smallest of the chunk
// count, max_concurrent_chunks and max_inflight_reads. With max_inflight_reads set, the call reserves all
// its slots before it sends any chunk and holds them until it ends, so a server at its cap rejects whole
// reads rather than cancelling chunks it has already sent. A slot is taken before a chunk's goroutine
// starts, so a chunk waiting for one holds no goroutine. The first error cancels the rest. Chunks write
// result without locks: each row key is in one chunk and writes only its own slots and found flags.
//
// The client reports a deadline or cancellation as a gRPC status that does not wrap the context error, so
// the returned error also wraps context.Cause(ctx).
func (s *BigtableOnlineStore) readChunks(ctx context.Context, chunks []*bigtableChunk, result *bigtableReadResult,
	stats *bigtableReadStats) error {
	slots := min(len(chunks), s.cfg.MaxConcurrentChunks)
	if s.inflight != nil {
		slots = min(slots, s.cfg.MaxInflightReads)
	}
	if slots == 0 {
		return nil
	}
	stats.rounds = (len(chunks) + slots - 1) / slots
	admitted := time.Now()
	releaseRead, err := s.admitRead(ctx, slots)
	stats.inflightWait = time.Since(admitted)
	if err != nil {
		return err
	}
	defer releaseRead()

	group, groupCtx := errgroup.WithContext(ctx)
	callSlots := make(chan struct{}, slots)
	var dispatchErr error
	for _, chunk := range chunks {
		release, err := acquireCallSlot(groupCtx, callSlots, chunk.tableName, stats)
		if err != nil {
			dispatchErr = err
			break
		}
		group.Go(func() error {
			defer release()
			return chunk.read(groupCtx, result)
		})
	}
	// A chunk's own error, which cancelled groupCtx, comes before the dispatch error it caused.
	err = group.Wait()
	if err == nil {
		err = dispatchErr
	}
	for _, chunk := range chunks {
		stats.rowsFound += chunk.rowsFound
	}
	return withContextCause(ctx, err)
}

// withContextCause appends context.Cause(ctx) to err when ctx has ended, even if err came from something
// else first (a corrupt cell, say); err still leads the message. It returns err unchanged otherwise.
func withContextCause(ctx context.Context, err error) error {
	if err != nil && ctx.Err() != nil && !errors.Is(err, context.Cause(ctx)) {
		return fmt.Errorf("%w (%w)", err, context.Cause(ctx))
	}
	return err
}

// admitRead reserves n max_inflight_reads slots for one read. When they are not free it waits up to
// max_inflight_wait_ms (0 does not wait) and then rejects the read with a *bigtableReadRejectedError, so
// an overloaded server turns work away before it reaches Bigtable instead of queueing it until
// read_timeout_ms. A read whose own deadline or cancellation ends the wait gets that error instead. With
// max_inflight_reads unset it returns at once. release returns the slots.
func (s *BigtableOnlineStore) admitRead(ctx context.Context, n int) (release func(), err error) {
	if s.inflight == nil {
		return func() {}, nil
	}
	release = func() { s.inflight.Release(int64(n)) }
	if s.inflight.TryAcquire(int64(n)) {
		return release, nil
	}
	if s.cfg.MaxInflightWait > 0 {
		waitCtx, cancel := context.WithTimeout(ctx, s.cfg.MaxInflightWait)
		err := s.inflight.Acquire(waitCtx, int64(n))
		cancel()
		if err == nil {
			return release, nil
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("bigtable online store: waiting for %d of max_inflight_reads %d: %w",
				n, s.cfg.MaxInflightReads, context.Cause(ctx))
		}
	}
	return nil, &bigtableReadRejectedError{msg: fmt.Sprintf("bigtable online store: read rejected: %d of "+
		"max_inflight_reads %d not free within max_inflight_wait_ms %d", n, s.cfg.MaxInflightReads,
		s.cfg.MaxInflightWait.Milliseconds())}
}

// acquireCallSlot waits for one of the call's slots and adds the wait to stats (only readChunks' dispatch
// loop calls it, so no lock). release returns the slot.
func acquireCallSlot(ctx context.Context, callSlots chan struct{}, table string,
	stats *bigtableReadStats) (release func(), err error) {
	start := time.Now()
	select {
	case callSlots <- struct{}{}:
		stats.callSlotWait += time.Since(start)
		return func() { <-callSlots }, nil
	case <-ctx.Done():
		stats.callSlotWait += time.Since(start)
		return nil, fmt.Errorf("bigtable online store: read table %s: %w", table, context.Cause(ctx))
	}
}

// read reads the chunk in one ReadRows and fills result. A decode error stops the stream from the callback,
// which cannot return it, so it is kept in fillErr.
func (c *bigtableChunk) read(ctx context.Context, result *bigtableReadResult) error {
	if ctx.Err() != nil {
		return fmt.Errorf("bigtable online store: read table %s: %w", c.tableName, context.Cause(ctx))
	}
	c.dispatched = true
	var fillErr error
	err := c.table.ReadRows(ctx, c.keys, func(row bigtable.Row) bool {
		if fillErr = result.fillRow(c.tableName, c.targets, row); fillErr != nil {
			return false
		}
		c.rowsFound++
		return true
	}, c.filter)
	if err != nil {
		return fmt.Errorf("bigtable online store: read table %s: %w", c.tableName, err)
	}
	return fillErr
}

// fillRow decodes a row into its first entity's slots, copies them to any repeats of that entity, and
// marks the view found.
func (r *bigtableReadResult) fillRow(table string, targets map[string]*bigtableRowTarget, row bigtable.Row) error {
	target, ok := targets[row.Key()]
	if !ok {
		return fmt.Errorf("bigtable online store: table %s returned unrequested row %q", table, row.Key())
	}
	first := r.rows[target.entities[0]]
	if err := decodeBigtableRow(row[bigtableFeatureFamily], target.view, first); err != nil {
		return fmt.Errorf("bigtable online store: table %s, view %s: %w", table, target.view.name, err)
	}
	for i, entity := range target.entities {
		if i > 0 {
			copyViewSlots(r.rows[entity], first, target.view)
		}
		r.found[entity*r.numViews+target.view.order] = true
	}
	return nil
}

// decodeBigtableRow writes the view's slots of one output row: each requested feature decoded straight
// into its slot, NullVal for one the row lacks, and the row's event_ts on all. A failed decode leaves the
// slots partly written, and the whole read fails.
func decodeBigtableRow(items []bigtable.ReadItem, view *bigtableViewRead, out []FeatureData) error {
	writeNullViewSlots(out, view)
	var eventTS *timestamppb.Timestamp
	for _, item := range items {
		qualifier := strings.TrimPrefix(item.Column, bigtableFeatureFamily+":")
		if qualifier == bigtableEventTSQualifier {
			ts, err := parseEventTS(item.Value)
			if err != nil {
				return fmt.Errorf("qualifier %s: %w", qualifier, err)
			}
			eventTS = ts
			continue
		}
		positions, requested := view.positions[qualifier]
		if !requested {
			continue
		}
		value := &out[positions[0]].Value
		if err := proto.Unmarshal(item.Value, value); err != nil {
			return fmt.Errorf("qualifier %s: decode feast.types.Value: %w", qualifier, err)
		}
		for _, p := range positions[1:] {
			out[p].Value.Val = value.Val
		}
	}
	if eventTS == nil {
		return fmt.Errorf("qualifier %s: missing from the row", bigtableEventTSQualifier)
	}
	for _, slot := range view.slots {
		out[slot.index].Timestamp = timestamppb.Timestamp{Seconds: eventTS.GetSeconds(), Nanos: eventTS.GetNanos()}
	}
	return nil
}

// writeNullViewSlots sets every slot of view in row to NullVal with a zero timestamp.
func writeNullViewSlots(row []FeatureData, view *bigtableViewRead) {
	for _, slot := range view.slots {
		row[slot.index] = FeatureData{
			Reference: serving.FeatureReferenceV2{FeatureViewName: view.name, FeatureName: slot.name},
			Value:     types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}},
		}
	}
}

// copyViewSlots copies the view's slots of src into dst, sharing each decoded value.
func copyViewSlots(dst, src []FeatureData, view *bigtableViewRead) {
	for _, slot := range view.slots {
		from := &src[slot.index]
		dst[slot.index] = FeatureData{
			Reference: serving.FeatureReferenceV2{FeatureViewName: view.name, FeatureName: slot.name},
			Timestamp: timestamppb.Timestamp{Seconds: from.Timestamp.GetSeconds(), Nanos: from.Timestamp.GetNanos()},
			Value:     types.Value{Val: from.Value.Val},
		}
	}
}

// applyMissingData nils the row of an entity found in no view and fills NullVal for its missing views.
func (r *bigtableReadResult) applyMissingData(views []*bigtableViewRead) [][]FeatureData {
	for entity, row := range r.rows {
		found := r.found[entity*r.numViews : (entity+1)*r.numViews]
		if !slices.Contains(found, true) {
			r.rows[entity] = nil
			continue
		}
		for _, view := range views {
			if !found[view.order] {
				writeNullViewSlots(row, view)
			}
		}
	}
	return r.rows
}

// Destruct closes the client. It is safe to call more than once.
func (s *BigtableOnlineStore) Destruct() {
	s.closeOnce.Do(func() {
		if err := s.client.Close(); err != nil {
			log.Warn().Err(err).Msg("bigtable online store: closing the client")
		}
	})
}

// table returns the cached handle for a table, opening it on first use. Safe for concurrent use.
func (s *BigtableOnlineStore) table(name string) *bigtable.Table {
	if t, ok := s.tables.Load(name); ok {
		return t.(*bigtable.Table)
	}
	t, _ := s.tables.LoadOrStore(name, s.client.Open(name))
	return t.(*bigtable.Table)
}

// bigtableTableName ports Python's BigtableOnlineStore._get_table_name, including its md5 shortening of
// long names. Lengths count runes, as Python's len and slicing do.
func bigtableTableName(project string, entityNames []string) string {
	entitiesPart := model.DUMMY_ENTITY_NAME
	if len(entityNames) > 0 {
		sorted := append([]string(nil), entityNames...)
		sort.Strings(sorted)
		entitiesPart = strings.Join(sorted, "-")
	}
	if runeLen(entitiesPart) > bigtableEntitiesPartMaxLength {
		entitiesPart = md5Hex(entitiesPart)[:bigtableEntitiesPartMaxLength]
	}
	remaining := bigtableTableNameMaxLength - runeLen(entitiesPart) - 1
	projectPart := project
	if runeLen(project) > remaining {
		projectHash := md5Hex(project)
		// Python's hexdigest()[:n] clamps at the 32 hex chars; a short entity part asks for more.
		hashLength := min(remaining-bigtableProjectReadableLength-1, len(projectHash))
		projectPart = string([]rune(project)[:bigtableProjectReadableLength]) + "_" + projectHash[:hashLength]
	}
	return projectPart + "." + entitiesPart
}

func runeLen(s string) int {
	return len([]rune(s))
}

func md5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

// bigtableRowKeyFromHash is Python's BigtableOnlineStore._compute_row_key from an entity hash: the hash,
// "#", then the feature view name.
func bigtableRowKeyFromHash(hash, featureViewName string) string {
	return hash + "#" + featureViewName
}

// bigtableEntityHash is the row key before "#": the hex MurmurHash3 x64 128-bit hash (seed 0) of the
// serialized entity key. It is the same for every view, so reads compute it once per entity.
func bigtableEntityHash(entityKey *types.EntityKey, serializationVersion int64) (string, error) {
	serKey, err := serializeEntityKey(entityKey, serializationVersion)
	if err != nil {
		return "", err
	}
	hash := bigtableMurmurHash(*serKey)
	return hex.EncodeToString(hash[:]), nil
}

// bigtableMurmurHash is MurmurHash3 x64 128-bit with seed 0 in the byte order of Python's
// mmh3.hash_bytes (h1 then h2, little-endian), using the pure Go github.com/spaolacci/murmur3.
func bigtableMurmurHash(data []byte) [16]byte {
	h1, h2 := murmur3.Sum128WithSeed(data, 0)
	var out [16]byte
	binary.LittleEndian.PutUint64(out[:8], h1)
	binary.LittleEndian.PutUint64(out[8:], h2)
	return out
}

// normalizeSerializationVersion maps an omitted entity_key_serialization_version (0) to 3, Python's
// default, and rejects every version other than 3.
func normalizeSerializationVersion(v int64) (int64, error) {
	if v == 0 {
		return bigtableSerializationVersion, nil
	}
	if v != bigtableSerializationVersion {
		return 0, fmt.Errorf("bigtable online store: entity_key_serialization_version must be %d, got %d",
			bigtableSerializationVersion, v)
	}
	return v, nil
}

// parseEventTS parses Python's datetime.isoformat() event_ts; one without an offset is UTC. An offset with
// fractional seconds (+05:53:28.123456, from a tzinfo built on a timedelta with microseconds) is not
// supported: no IANA zone has one, and Go's time layouts cannot express it.
func parseEventTS(b []byte) (*timestamppb.Timestamp, error) {
	s := string(b)
	t, err := time.Parse(time.RFC3339Nano, s)
	if err == nil {
		return timestamppb.New(t), nil
	}
	for _, layout := range []string{secondsOffsetISOLayout, naiveISOLayout} {
		if t, altErr := time.Parse(layout, s); altErr == nil {
			return timestamppb.New(t), nil
		}
	}
	return nil, fmt.Errorf("invalid event_ts %q: %w", s, err)
}
