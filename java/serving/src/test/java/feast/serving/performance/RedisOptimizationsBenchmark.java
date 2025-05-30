package feast.serving.performance;

import feast.proto.core.FeatureProto;
import feast.proto.serving.ServingAPIProto;
import feast.proto.storage.RedisProto;
import feast.proto.types.ValueProto;
import feast.serving.connectors.OnlineRetriever;
import feast.serving.connectors.redis.common.RedisHashDecoder;
import feast.serving.connectors.redis.common.RedisKeyGenerator;
import feast.serving.connectors.redis.retriever.*;
import feast.serving.registry.Registry;
import feast.serving.registry.RegistryRepository;
import feast.serving.service.FeatureCacheManager;
import feast.serving.service.config.ApplicationProperties;
import feast.serving.util.RedisPerformanceMonitor;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 10)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RedisOptimizationsBenchmark {

    private static final String TEST_PROJECT = "benchmark_project";
    private static final int REDIS_PORT = 6370; // Use a different port for embedded server
    private RedisServer redisServer;

    private ApplicationProperties applicationProperties;
    private RegistryRepository registryRepository;
    private EntityKeySerializer entityKeySerializer;

    private OnlineRetriever originalRedisRetriever; // Representing the baseline
    private OptimizedRedisOnlineRetriever optimizedRedisRetriever;

    private FeatureCacheManager featureCacheManager;
    private RedisPerformanceMonitor performanceMonitor;
    private OptimizedRedisClientAdapter optimizedRedisClientAdapter;
    private RedisClient standaloneClient; // For direct data setup

    // Test Data
    private List<Map<String, ValueProto.Value>> singleEntityRow;
    private List<Map<String, ValueProto.Value>> batchEntityRows_10;
    private List<Map<String, ValueProto.Value>> batchEntityRows_100;

    private List<ServingAPIProto.FeatureReferenceV2> fewFeatures_5;
    private List<ServingAPIProto.FeatureReferenceV2> manyFeatures_60;
    private List<String> entityNames;


    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        redisServer = RedisServer.builder().port(REDIS_PORT).setting("maxmemory 128M").build();
        redisServer.start();

        // Mock ApplicationProperties
        applicationProperties = new ApplicationProperties();
        ApplicationProperties.FeastProperties feastProperties = new ApplicationProperties.FeastProperties();
        feastProperties.setProject(TEST_PROJECT);
        feastProperties.setEntityKeySerializationVersion(2);
        feastProperties.setRetrieverThreadPoolSize(4); // Configurable thread pool

        ApplicationProperties.Store storeConfig = new ApplicationProperties.Store();
        storeConfig.setName("online_redis_benchmark");
        storeConfig.setType("REDIS");
        Map<String, String> redisConfigMap = new HashMap<>();
        redisConfigMap.put("host", "localhost");
        redisConfigMap.put("port", String.valueOf(REDIS_PORT));
        redisConfigMap.put("poolMaxTotal", "8"); // Default, can be tuned
        storeConfig.setConfig(redisConfigMap);

        feastProperties.setStores(Collections.singletonList(storeConfig));
        feastProperties.setActiveStore("online_redis_benchmark");
        applicationProperties.setFeast(feastProperties);

        // Mock Registry
        registryRepository = createMockRegistryRepository();
        entityKeySerializer = new EntityKeySerializerV2(); // Using V2 as per common config

        // Initialize components for Optimized Retriever
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        featureCacheManager = new FeatureCacheManager(); // Uses default Caffeine settings
        performanceMonitor = new RedisPerformanceMonitor(meterRegistry);
        optimizedRedisClientAdapter = new OptimizedRedisClientAdapter(applicationProperties);

        optimizedRedisRetriever = new OptimizedRedisOnlineRetriever(
                TEST_PROJECT,
                optimizedRedisClientAdapter,
                entityKeySerializer,
                featureCacheManager,
                performanceMonitor,
                feastProperties.getRetrieverThreadPoolSize());

        // Initialize components for a "Baseline" Retriever (simplified setup)
        // This baseline uses the new OptimizedRedisClientAdapter but not the OptimizedRedisOnlineRetriever logic
        // to simulate a less optimized path (e.g. no pipelining, no advanced caching if FeatureCacheManager is bypassed)
        // For a true "original" comparison, one would need to set up the older RedisClientAdapter.
        // This is a stand-in to show benefits of OptimizedRedisOnlineRetriever's internal logic.
        originalRedisRetriever = new RedisOnlineRetriever( // The "original" one from main codebase
            TEST_PROJECT,
            new RedisClientAdapter(applicationProperties), // Basic adapter
            entityKeySerializer
        );


        // Setup direct Redis client for populating data
        standaloneClient = RedisClient.create(String.format("redis://localhost:%d", REDIS_PORT));

        // Prepare test data
        entityNames = List.of("customer_id");
        singleEntityRow = generateEntityRows(1, entityNames);
        batchEntityRows_10 = generateEntityRows(10, entityNames);
        batchEntityRows_100 = generateEntityRows(100, entityNames);

        fewFeatures_5 = generateFeatureReferences("feature_view_1", 5);
        manyFeatures_60 = generateFeatureReferences("feature_view_1", 60); // To cross HGETALL threshold

        // Populate Redis with data
        populateRedisWithTestData(fewFeatures_5);
        populateRedisWithTestData(manyFeatures_60);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        if (optimizedRedisRetriever != null) {
            optimizedRedisRetriever.shutdown();
        }
        if (optimizedRedisClientAdapter != null) {
            optimizedRedisClientAdapter.shutdown();
        }
        if(standaloneClient != null) {
            standaloneClient.shutdown();
        }
        if (redisServer != null) {
            redisServer.stop();
        }
    }

    private RegistryRepository createMockRegistryRepository() {
        // Create a mock registry with one feature view and specified features
        // For simplicity, all features have STRING type
        List<FeatureProto.FeatureSpecV2> featureSpecs = new ArrayList<>();
        for (int i = 0; i < 100; i++) { // Max features we might request
            featureSpecs.add(FeatureProto.FeatureSpecV2.newBuilder()
                    .setName("feature_" + i)
                    .setValueType(ValueProto.ValueType.Enum.STRING)
                    .build());
        }

        FeatureProto.FeatureViewSpec fvSpec = FeatureProto.FeatureViewSpec.newBuilder()
                .setName("feature_view_1")
                .addAllEntities(entityNames)
                .addAllFeatures(featureSpecs)
                .build();
        
        FeatureProto.FeatureView fv = FeatureProto.FeatureView.newBuilder().setSpec(fvSpec).build();

        RegistryProto.Registry registryProto = RegistryProto.Registry.newBuilder()
            .addFeatureViews(fv)
            .addEntities(EntityProto.Entity.newBuilder().setSpec(
                EntityProto.EntitySpecV2.newBuilder().setName("customer_id").setJoinKey("customer_id").setValueType(ValueProto.ValueType.Enum.INT64)
            ).build())
            .build();
            
        Registry registry = new Registry(registryProto);
        return new RegistryRepository(registry, TEST_PROJECT);
    }

    private List<Map<String, ValueProto.Value>> generateEntityRows(int count, List<String> entityNames) {
        List<Map<String, ValueProto.Value>> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Map<String, ValueProto.Value> row = new HashMap<>();
            for (String entityName : entityNames) {
                row.put(entityName, ValueProto.Value.newBuilder().setInt64Val(1000 + i).build());
            }
            rows.add(row);
        }
        return rows;
    }

    private List<ServingAPIProto.FeatureReferenceV2> generateFeatureReferences(String fvName, int count) {
        List<ServingAPIProto.FeatureReferenceV2> refs = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            refs.add(ServingAPIProto.FeatureReferenceV2.newBuilder()
                    .setFeatureViewName(fvName)
                    .setFeatureName("feature_" + i)
                    .build());
        }
        return refs;
    }

    private void populateRedisWithTestData(List<ServingAPIProto.FeatureReferenceV2> featuresToPopulate) {
        try (StatefulRedisConnection<byte[], byte[]> connection = standaloneClient.connect(new ByteArrayCodec())) {
            RedisCommands<byte[], byte[]> sync = connection.sync();
            long currentTimestampMillis = System.currentTimeMillis();

            for (Map<String, ValueProto.Value> entityRow : batchEntityRows_100) { // Populate for all potential entities
                RedisProto.RedisKeyV2 redisKeyProto = RedisKeyGenerator.buildRedisKey(TEST_PROJECT, entityRow);
                byte[] redisKeyBytes = entityKeySerializer.serialize(redisKeyProto);

                Map<byte[], byte[]> hashData = new HashMap<>();
                for (ServingAPIProto.FeatureReferenceV2 ref : featuresToPopulate) {
                    byte[] fieldKey = RedisHashDecoder.getFeatureReferenceRedisHashKeyBytes(ref);
                    // Simple string value for benchmark
                    byte[] value = ValueProto.Value.newBuilder().setStringVal("value_for_" + ref.getFeatureName()).build().toByteArray();
                    hashData.put(fieldKey, value);

                    // Timestamp for the feature view
                    byte[] tsKey = RedisHashDecoder.getTimestampRedisHashKeyBytes(ref.getFeatureViewName(), "_ts");
                    ValueProto.Value tsValue = ValueProto.Value.newBuilder()
                        .setTimestampVal(com.google.protobuf.Timestamp.newBuilder().setSeconds(currentTimestampMillis / 1000).build())
                        .build();
                    hashData.put(tsKey, tsValue.toByteArray());
                }
                sync.hmset(redisKeyBytes, hashData);
            }
        }
    }
    
    // --- Benchmark Methods ---

    // Scenario 1: Single Entity, Few Features
    @Benchmark
    public void baseline_singleEntity_fewFeatures(Blackhole bh) {
        bh.consume(originalRedisRetriever.getOnlineFeatures(singleEntityRow, fewFeatures_5, entityNames));
    }

    @Benchmark
    public void optimized_singleEntity_fewFeatures(Blackhole bh) {
        bh.consume(optimizedRedisRetriever.getOnlineFeatures(singleEntityRow, fewFeatures_5, entityNames));
    }

    // Scenario 2: Single Entity, Many Features (to test HGETALL threshold effects)
    @Benchmark
    public void baseline_singleEntity_manyFeatures(Blackhole bh) {
        bh.consume(originalRedisRetriever.getOnlineFeatures(singleEntityRow, manyFeatures_60, entityNames));
    }

    @Benchmark
    public void optimized_singleEntity_manyFeatures(Blackhole bh) {
        bh.consume(optimizedRedisRetriever.getOnlineFeatures(singleEntityRow, manyFeatures_60, entityNames));
    }

    // Scenario 3: Batch Entities (10), Few Features (to test pipelining/batching)
    @Benchmark
    public void baseline_batch10Entities_fewFeatures(Blackhole bh) {
        bh.consume(originalRedisRetriever.getOnlineFeatures(batchEntityRows_10, fewFeatures_5, entityNames));
    }

    @Benchmark
    public void optimized_batch10Entities_fewFeatures(Blackhole bh) {
        bh.consume(optimizedRedisRetriever.getOnlineFeatures(batchEntityRows_10, fewFeatures_5, entityNames));
    }
    
    // Scenario 4: Batch Entities (100), Few Features
    @Benchmark
    public void baseline_batch100Entities_fewFeatures(Blackhole bh) {
        bh.consume(originalRedisRetriever.getOnlineFeatures(batchEntityRows_100, fewFeatures_5, entityNames));
    }

    @Benchmark
    public void optimized_batch100Entities_fewFeatures(Blackhole bh) {
        bh.consume(optimizedRedisRetriever.getOnlineFeatures(batchEntityRows_100, fewFeatures_5, entityNames));
    }

    // Scenario 5: Cache Hit (Optimized Retriever Only)
    // Run once to populate cache, then benchmark
    @Benchmark
    public void optimized_cached_singleEntity_fewFeatures(RedisBenchmarkState state, Blackhole bh) {
        // First call to populate cache (not measured directly in this specific invocation's timing)
        if (state.isFirstCacheRun) {
             optimizedRedisRetriever.getOnlineFeatures(singleEntityRow, fewFeatures_5, entityNames);
             state.isFirstCacheRun = false; // Ensure it only runs once per iteration for cache setup
        }
        // Subsequent calls should hit the cache
        bh.consume(optimizedRedisRetriever.getOnlineFeatures(singleEntityRow, fewFeatures_5, entityNames));
    }
    
    // Add a flag to state for cache priming, if JMH doesn't reset state perfectly for this.
    // For simplicity, a benchmark iteration usually calls the method multiple times.
    // The first call in an iteration might populate, subsequent ones hit.
    // To be more precise, one might need a @Setup(Level.Invocation) for cache priming,
    // but that can add overhead. The current approach is a common way.
    public boolean isFirstCacheRun = true; // This will be reset per JMH fork/iteration by default.
                                          // If we want it per *invocation* for priming, needs @Setup(Level.Invocation)

    @Setup(Level.Iteration)
    public void setupIteration() {
        // Clear cache before each iteration of the cache test to ensure consistent "first miss"
        if (featureCacheManager != null) {
            featureCacheManager.clearAll();
        }
        isFirstCacheRun = true; // Reset for the cache test
    }

    /*
     * To run this benchmark:
     * 1. Make sure you have JMH dependencies in your pom.xml (jmh-core, jmh-generator-annprocess)
     * 2. Build the project: mvn clean install
     * 3. Run the benchmark: java -jar target/benchmarks.jar feast.serving.performance.RedisOptimizationsBenchmark -prof gc
     *    (or run from IDE with JMH plugin)
     */
}
