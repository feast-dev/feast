WITH union_features AS (SELECT
  uuid,
  event_timestamp,
  NULL as {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp,
  NULL as created_timestamp,
  {{ featureSet.entities | join(', ')}},
  true AS is_entity_table
FROM `{{leftTableName}}`
UNION ALL
SELECT
  NULL as uuid,
  event_timestamp,
  event_timestamp as {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp,
  created_timestamp,
  {{ featureSet.entities | join(', ')}},
  false AS is_entity_table
FROM `{{projectId}}.{{datasetId}}.{{ featureSet.name }}_v{{ featureSet.version }}` WHERE event_timestamp <= '{{maxTimestamp}}' AND event_timestamp >= Timestamp_sub(TIMESTAMP '{{ minTimestamp }}', interval {{ featureSet.maxAge }} second)
)
SELECT
  uuid,
  event_timestamp,
  {{ featureSet.entities | join(', ')}},
  {% for featureName in featureSet.features %}
  IF(event_timestamp >= {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp AND Timestamp_sub(event_timestamp, interval {{ featureSet.maxAge }} second) < {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp, {{ featureSet.name }}_v{{ featureSet.version }}_{{ featureName }}, NULL) as {{ featureSet.name }}_v{{ featureSet.version }}_{{ featureName }}{% if loop.last %}{% else %}, {% endif %}
  {% endfor %}
FROM (
SELECT
  uuid,
  event_timestamp,
  {{ featureSet.entities | join(', ')}},
  FIRST_VALUE(created_timestamp IGNORE NULLS) over w AS created_timestamp,
  FIRST_VALUE({{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp IGNORE NULLS) over w AS {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp,
  is_entity_table
FROM union_features
WINDOW w AS (PARTITION BY {{ featureSet.entities | join(', ') }} ORDER BY event_timestamp DESC, is_entity_table DESC, created_timestamp DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
)
LEFT JOIN (
SELECT
  event_timestamp as {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp,
  created_timestamp,
  {{ featureSet.entities | join(', ')}},
  {% for featureName in featureSet.features %}
  {{ featureName }} as {{ featureSet.name }}_v{{ featureSet.version }}_{{ featureName }}{% if loop.last %}{% else %}, {% endif %}
  {% endfor %}
FROM `{{projectId}}.{{datasetId}}.{{ featureSet.name }}_v{{ featureSet.version }}` WHERE event_timestamp <= '{{maxTimestamp}}' AND event_timestamp >= Timestamp_sub(TIMESTAMP '{{ minTimestamp }}', interval {{ featureSet.maxAge }} second)
) USING ({{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp, created_timestamp, {{ featureSet.entities | join(', ')}})
WHERE is_entity_table