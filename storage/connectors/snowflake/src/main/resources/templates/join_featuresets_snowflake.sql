/*
 Joins the outputs of multiple point-in-time-correctness joins to a single table.
 */
WITH joined as (
SELECT * FROM {{ leftTableName }}

{% for featureSet in featureSets %}
LEFT JOIN (
    SELECT
    row_number,
    {% for feature in featureSet.features %}
    {{ featureSet.project }}__{{ featureSet.name }}__{{ feature.name }}{% if loop.last %}{% else %}, {% endif %}
    {% endfor %}
    FROM {{ featureSet.joinedTable }}
) as {{ featureSet.joinedTable }} USING (row_number)
{% endfor %}
) SELECT
    {{ entities | join(', ') }}
    {% for featureSet in featureSets %}
    {% for feature in featureSet.features %}
	,{{ featureSet.project }}__{{ featureSet.name }}__{{ feature.name }} as {% if feature.featureSet != "" %}{{ featureSet.name }}__{% endif %}{{ feature.name }}
    {% endfor %}
    {% endfor %}
FROM joined
