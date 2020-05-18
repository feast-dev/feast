/*
 Joins the outputs of multiple point-in-time-correctness joins to a single table.
 */
WITH joined as (
SELECT * FROM `{{ leftTableName }}`
{% for featureSet in featureSets %}
LEFT JOIN (
    SELECT
    uuid,
    {% for feature in featureSet.features %}
    {{ featureSet.project }}__{{ featureSet.name }}__{{ feature.name }}{% if loop.last %}{% else %}, {% endif %}
    {% endfor %}
    FROM `{{ featureSet.table }}`
) USING (uuid)
{% endfor %}
) SELECT
    event_timestamp,
    {{ entities | join(', ') }}
    {% for featureSet in featureSets %}
    {% for feature in featureSet.features %}
	,{{ featureSet.project }}__{{ featureSet.name }}__{{ feature.name }} as {% if feature.featureSetName != "" %}{{ featureSet.name }}__{% endif %}{{ feature.name }}
    {% endfor %}
    {% endfor %}
FROM joined