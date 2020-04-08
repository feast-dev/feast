WITH subset AS (
SELECT * FROM `{{ projectId }}.{{ datasetId }}.{{ featureSet.project }}_{{ featureSet.name }}_v{{ featureSet.version }}`
{% if featureSet.datasetId == "" %}
WHERE event_timestamp >= '{{ featureSet.date }} 00:00:00 UTC' AND event_timestamp < DATETIME_ADD('{{ featureSet.date }}  00:00:00 UTC', INTERVAL 1 DAY)
{% else %}
WHERE dataset_id='{{ featureSet.datasetId }}'
{% endif %}
)
{% for feature in featureSet.features %}
, {{ feature.name }}_stats AS (
{% if feature.type == 'NUMERIC' %}
  WITH stats AS (
    SELECT min+step*i as min, min+step*(i+1) as max
    FROM (
      SELECT MIN({{ feature.name }}) as min, MAX({{ feature.name }}) as max, (MAX({{ feature.name }})-MIN({{ feature.name }}))/10 step, GENERATE_ARRAY(0, 10, 1) i
      FROM subset
    ), UNNEST(i) i
  ), counts as (
    SELECT COUNT(*) as count, min, max,
    FROM subset
    JOIN stats
    ON subset.{{ feature.name }} >= stats.min AND subset.{{ feature.name }}<stats.max
    GROUP BY min, max
  )
  SELECT '{{ feature.name }}' as feature, ARRAY_AGG(STRUCT(count as count, min as low_value, max as high_value)) as num_hist, ARRAY<STRUCT<value STRING, count INT64>>[] as cat_hist FROM counts
{% elseif feature.type == 'CATEGORICAL' %}
  WITH counts AS (
    SELECT {{ feature.name }}, COUNT({{ feature.name }}) AS count FROM subset GROUP BY {{ feature.name }}
  )
  SELECT '{{ feature.name }}' as feature, ARRAY<STRUCT<count INT64, low_value FLOAT64, high_value FLOAT64>>[] as num_hist, ARRAY_AGG(STRUCT({{ feature.name }} as value, count as count)) as cat_hist FROM counts
{% elseif feature.type == 'BYTES' %}
  SELECT '{{ feature.name }}' as feature, ARRAY<STRUCT<count INT64, low_value FLOAT64, high_value FLOAT64>>[] as num_hist, ARRAY<STRUCT<value STRING, count INT64>>[] as cat_hist
{% elseif feature.type == 'LIST' %}
  SELECT '{{ feature.name }}' as feature, ARRAY<STRUCT<count INT64, low_value FLOAT64, high_value FLOAT64>>[] as num_hist, ARRAY<STRUCT<value STRING, count INT64>>[] as cat_hist
{% endif %}
)
{% endfor %}
{% for feature in featureSet.features %}
SELECT * FROM {{ feature.name }}_stats
{% if loop.last %}{% else %}UNION ALL {% endif %}
{% endfor %}