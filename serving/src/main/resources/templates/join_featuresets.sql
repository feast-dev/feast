WITH joined as (
SELECT * FROM `{{ leftTableName }}`
{% for featureSet in featureSets %}
LEFT JOIN (
    SELECT
    uuid,
    {% for featureName in featureSet.features %}
    {{ featureSet.name }}_v{{ featureSet.version }}_{{ featureName }}{% if loop.last %}{% else %}, {% endif %}
    {% endfor %}
    FROM `{{ featureSet.table }}`
) USING (uuid)
{% endfor %}
) SELECT
    event_timestamp,
    {{ entities | join(', ') }}
    {% for featureSet in featureSets %}
    {% for featureName in featureSet.features %}
    ,{{ featureSet.name }}_v{{ featureSet.version }}_{{ featureName }}
    {% endfor %}
    {% endfor %}
FROM joined