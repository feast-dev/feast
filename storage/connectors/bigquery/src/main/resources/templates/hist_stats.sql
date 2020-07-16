WITH subset AS (
{{ dataset }}
)
{% for feature in features %}
, {{ feature.name }}_stats AS (
{% if feature.statsType == 'NUMERIC' %}
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
{% elseif feature.statsType == 'CATEGORICAL' %}
  WITH counts AS (
    SELECT {{ feature.name }}, COUNT({{ feature.name }}) AS count FROM subset GROUP BY {{ feature.name }}
  )
  SELECT '{{ feature.name }}' as feature, ARRAY<STRUCT<count INT64, low_value FLOAT64, high_value FLOAT64>>[] as num_hist, ARRAY_AGG(STRUCT({{ feature.name }} as value, count as count)) as cat_hist FROM counts
{% elseif feature.statsType == 'BYTES' %}
  SELECT '{{ feature.name }}' as feature, ARRAY<STRUCT<count INT64, low_value FLOAT64, high_value FLOAT64>>[] as num_hist, ARRAY<STRUCT<value STRING, count INT64>>[] as cat_hist
{% elseif feature.statsType == 'LIST' %}
  SELECT '{{ feature.name }}' as feature, ARRAY<STRUCT<count INT64, low_value FLOAT64, high_value FLOAT64>>[] as num_hist, ARRAY<STRUCT<value STRING, count INT64>>[] as cat_hist
{% endif %}
)
{% endfor %}
{% for feature in features %}
SELECT * FROM {{ feature.name }}_stats
{% if loop.last %}{% else %}UNION ALL {% endif %}
{% endfor %}