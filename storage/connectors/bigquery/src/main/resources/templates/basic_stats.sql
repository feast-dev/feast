WITH subset AS (
SELECT * FROM `{{ projectId }}.{{ datasetId }}.{{ featureSet.project }}_{{ featureSet.name }}_v{{ featureSet.version }}`
{% if featureSet.datasetId == "" %}
WHERE event_timestamp >= '{{ featureSet.date }} 00:00:00 UTC' AND event_timestamp < DATETIME_ADD('{{ featureSet.date }}  00:00:00 UTC', INTERVAL 1 DAY)
{% else %}
WHERE dataset_id='{{ featureSet.datasetId }}'
{% endif %}
)
{% for feature in featureSet.features %}
SELECT
    "{{ feature.name }}" as feature_name,
    -- total count
    COUNT(*) AS total_count,
    -- count
    COUNT({{ feature.name }}) as feature_count,
    -- missing
    COUNT(*) - COUNT({{ feature.name }}) as missing_count,
    {% if feature.type equals "NUMERIC" %}
    -- mean
    AVG({{ feature.name }}) as mean,
    -- stdev
    STDDEV({{ feature.name }}) as stdev,
    -- zeroes
    COUNTIF({{ feature.name }} = 0) as zeroes,
    -- min
    MIN({{ feature.name }}) as min,
    -- max
    MAX({{ feature.name }}) as max,
    -- hist will have to be called separately
    -- quantiles
    APPROX_QUANTILES(CAST({{ feature.name }} AS FLOAT64), 10) AS quantiles,
    -- unique
    null as unique,
    -- top count
    ARRAY<STRUCT<value STRING, count INT64>>[] as top_count
    {% elseif feature.type equals "CATEGORICAL" %}
    -- mean
    null as mean,
    -- stdev
    null as stdev,
    -- zeroes
    null as zeroes,
    -- min
    null as min,
    -- max
    null as max,
    -- quantiles
    ARRAY<FLOAT64>[] AS quantiles,
    -- unique
    APPROX_COUNT_DISTINCT({{ feature.name }}) as unique,
    -- top count
    APPROX_TOP_COUNT({{ feature.name }}, 5) as top_count,
    {% elseif feature.type equals "BYTES" %}
    -- mean
    AVG(BIT_COUNT({{ feature.name }})) as mean,
    -- stdev
    null as stdev,
    -- zeroes
    null as zeroes,
    -- min
    MIN(BIT_COUNT({{ feature.name }})) as min,
    -- max
    MAX(BIT_COUNT({{ feature.name }})) as max,
    -- hist will have to be called separately
    -- quantiles
    ARRAY<FLOAT64>[] AS quantiles,
    -- unique
    APPROX_COUNT_DISTINCT({{ feature.name }}) as unique,
    -- top count
    ARRAY<STRUCT<value STRING, count INT64>>[] as top_count
    {% elseif feature.type equals "LIST" %}
    -- mean
    AVG(ARRAY_LENGTH({{ feature.name }})) as mean,
    -- stdev
    null as stdev,
    -- zeroes
    null as zeroes,
    -- min
    MIN(ARRAY_LENGTH({{ feature.name }})) as min,
    -- max
    MAX(ARRAY_LENGTH({{ feature.name }})) as max,
    -- hist will have to be called separately
    -- quantiles
    ARRAY<FLOAT64>[] AS quantiles,
    -- unique
    null as unique,
    -- top count
    ARRAY<STRUCT<value STRING, count INT64>>[] as top_count
    {% endif %}
FROM subset
{% if loop.last %}{% else %}UNION ALL {% endif %}
{% endfor %}

