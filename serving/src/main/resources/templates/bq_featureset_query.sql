WITH union_features AS (SELECT
  event_timestamp,
  {% for featureSet in featureSets %}NULL as {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp,
  {% endfor %}{{ fullEntitiesList | join(', ')}},
  true AS is_entity_table
FROM `{{projectId}}.{{datasetId}}.{{leftTableName}}`
{% for featureSet in featureSets %}
UNION ALL
SELECT
  event_timestamp,
  {% for otherFeatureSet in featureSets %}
  {% if otherFeatureSet.id != featureSet.id %}
  NULL as {{ otherFeatureSet.name }}_v{{ otherFeatureSet.version }}_feature_timestamp,
  {% else %}
  event_timestamp as {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp,
  {% endif %}
  {% endfor %}
  {% for entityName in fullEntitiesList %}
  {% if featureSet.entities contains entityName %}
  {{entityName}},
  {% else %}
  NULL as {{entityName}},
  {% endif %}
  {% endfor %}
  false AS is_entity_table
FROM `{{projectId}}.{{datasetId}}.{{ featureSet.name }}_v{{ featureSet.version }}` WHERE event_timestamp <= '{{maxTimestamp}}' AND event_timestamp >= Timestamp_sub(TIMESTAMP '{{ minTimestamp }}', interval {{ featureSet.maxAge }} second)
{% endfor %}
), ts_union AS (
{% for featureSet in featureSets %}
  SELECT * FROM (
    SELECT
      event_timestamp,
      {{ fullEntitiesList | join(', ')}},
      {% for otherFeatureSet in featureSets %}
      {% if otherFeatureSet.id == featureSet.id %}
      LAST_VALUE({{ otherFeatureSet.name }}_v{{ otherFeatureSet.version }}_feature_timestamp IGNORE NULLS) over w AS {{ otherFeatureSet.name }}_v{{ otherFeatureSet.version }}_feature_timestamp,
      {% else %}
      {{ otherFeatureSet.name }}_v{{ otherFeatureSet.version }}_feature_timestamp,
      {% endif %}
      {% endfor %}
      is_entity_table
    FROM union_features
    WINDOW w AS (PARTITION BY {{ featureSet.entities }} ORDER BY event_timestamp, is_entity_table ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  ) WHERE is_entity_table
  {% if loop.last %}
  {% else %}
  UNION ALL
  {% endif %}
{% endfor %}
), ts_coalesce AS (
  SELECT
    event_timestamp,
    {{ fullEntitiesList | join(', ')}},
    {% for featureSet in featureSets %}
    LAST_VALUE({{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp IGNORE NULLS) over w AS {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp,
    {% endfor %}
    ROW_NUMBER() over w as rn
  FROM ts_union
  WINDOW w AS (PARTITION BY {{ fullEntitiesList | join(', ')}}, event_timestamp ORDER BY event_timestamp)
), ts_final AS (
SELECT
  event_timestamp,
  {% for featureSet in featureSets %}
  IF(event_timestamp >= {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp AND Timestamp_sub(event_timestamp, interval {{ featureSet.maxAge }} second) < {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp, {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp, NULL) as {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp,
  {% endfor %}
  {{ fullEntitiesList | join(', ')}}
 FROM ts_coalesce WHERE rn = 1
)
SELECT * FROM ts_final
{% for featureSet in featureSets %}
LEFT JOIN
(SELECT {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp,
  {% for featureName in featureSet.features %}{{ featureSet.name }}_v{{ featureSet.version }}_{{ featureName }},
  {% endfor %}{{ featureSet.entities | join(', ') }}
  FROM (SELECT
    event_timestamp as {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp,
    {{ featureSet.entities | join(', ') }},
    {% for featureName in featureSet.features %}
    {{ featureName }} as {{ featureSet.name }}_v{{ featureSet.version }}_{{ featureName }},
    {% endfor %}ROW_NUMBER() OVER(PARTITION BY event_timestamp, {{ featureSet.entities | join(', ') }} ORDER BY created_timestamp DESC) as {{ featureSet.name }}_v{{ featureSet.version }}_rown
    FROM `{{ projectId }}.{{ datasetId }}.{{ featureSet.name }}_v{{ featureSet.version }}` WHERE event_timestamp <= '{{maxTimestamp}}' AND event_timestamp >= Timestamp_sub(TIMESTAMP '{{ minTimestamp }}', interval {{ featureSet.maxAge }} second)
) WHERE {{ featureSet.name }}_v{{ featureSet.version }}_rown = 1
) USING ({{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp, {{ featureSet.entities | join(', ') }})
{% endfor %}