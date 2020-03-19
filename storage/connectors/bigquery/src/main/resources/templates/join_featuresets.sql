/*
 Joins the outputs of multiple point-in-time-correctness joins to a single table.
 */
WITH joined as (
SELECT * FROM `{{ leftTableName }}`
{% for featureSet in featureSets %}
LEFT JOIN (
    SELECT
    uuid,
    {% for featureName in featureSet.features %}
    {{ featureSet.project }}_{{ featureName }}_v{{ featureSet.version }}{% if loop.last %}{% else %}, {% endif %}
    {% endfor %}
    FROM `{{ featureSet.table }}`
) USING (uuid)
{% endfor %}
) SELECT
    event_timestamp,
    {{ entities | join(', ') }}
    {% for featureSet in featureSets %}
    {% for featureName in featureSet.features %}
    ,{{ featureSet.project }}_{{ featureName }}_v{{ featureSet.version }} as {{ featureName }}
    {% endfor %}
    {% endfor %}
FROM joined