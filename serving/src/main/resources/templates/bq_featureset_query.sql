SELECT
   reduce.event_timestamp{% for entityName in entityNames %}, reduce.{{ entityName }}{% endfor %}{% for featureName in featureSet.featureNamesList %}, reduce.{{featureSet.name}}_v{{featureSet.version}}_{{ featureName }}{% endfor %} FROM
   (
      SELECT
         joined.event_timestamp,
         {% for entityName in entityNames %}joined.{{ entityName }},
         {% endfor %}{% for featureName in featureSet.featureNamesList %}joined.{{featureSet.name}}_v{{featureSet.version}}_{{featureName}},
         {% endfor %}ROW_NUMBER() OVER ( PARTITION BY {% for entityName in entityNames %}joined.{{ entityName }}, {% endfor %} joined.event_timestamp
      ORDER BY
         joined.r_event_timestamp DESC) rank
      FROM
         (
            SELECT
               {% for entityName in entityNames %}l.{{ entityName }},
               {% endfor %}l.event_timestamp,
               {% for featureName in featureSet.featureNamesList %}r.{{featureName}} as {{featureSet.name}}_v{{featureSet.version}}_{{featureName}},
               {% endfor %}r.event_timestamp AS r_event_timestamp
            FROM
               `{{ leftTableName }}` AS l
               LEFT OUTER JOIN
                  (
                     SELECT
                        *
                     FROM
                        `{{ rightTableName }}`
                     WHERE
                        event_timestamp >= "{{ minTimestamp }}"
                        AND event_timestamp <= "{{ maxTimestamp }}"
                  )
                  AS r
                  ON l.event_timestamp >= r.event_timestamp
                  AND Timestamp_sub(l.event_timestamp, interval {{ maxAge }} second) < r.event_timestamp
                  {% for entity in featureSetSpec.entitiesList %}AND l.{{entity.name}} = r.{{entity.name}}
                  {% endfor %})
         AS joined
   )
   AS reduce
WHERE
   reduce.rank = 1