SELECT {{ fullEntitiesList | join(', ')}}, {% for featureSet in featureSets %}{% for featureName in featureSet.features %}{{ featureSet.name }}_v{{ featureSet.version }}_{{ featureName }}, {% endfor %}{% endfor %}event_timestamp FROM (WITH union_features AS (
SELECT
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
FROM `{{projectId}}.{{datasetId}}.{{ featureSet.name }}_v{{ featureSet.version }}` WHERE event_timestamp <= '{{maxTimestamp}}' AND event_timestamp >= '{{minTimestamp}}'
{% endfor %}
)
SELECT 
  event_timestamp,
  {% for featureSet in featureSets %}
  IF(event_timestamp >= {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp AND Timestamp_sub(event_timestamp, interval {{ featureSet.maxAge }} second) < {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp, {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp, NULL) as {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp, 
  {% endfor %}
  {{ fullEntitiesList | join(', ')}}
  FROM (
  SELECT
    event_timestamp,
    {{ fullEntitiesList | join(', ')}},
    {% for featureSet in featureSets %}
    LAST_VALUE({{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp IGNORE NULLS) over w AS {{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp,
    {% endfor %}
    is_entity_table
  FROM union_features 
  WINDOW w AS (PARTITION BY source ORDER BY event_timestamp ASC) 
) WHERE is_entity_table)
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
    FROM `{{ projectId }}.{{ datasetId }}.{{ featureSet.name }}_v{{ featureSet.version }}` WHERE event_timestamp <= '{{maxTimestamp}}' AND event_timestamp >= '{{minTimestamp}}' 
) WHERE {{ featureSet.name }}_v{{ featureSet.version }}_rown = 1
) USING ({{ featureSet.name }}_v{{ featureSet.version }}_feature_timestamp, source)
{% endfor %}