SELECT * FROM `{{ table }}`
{% if ingestionId is not empty %}
WHERE ingestion_id='{{ ingestionId }}'
{% endif %}
{% if date is not empty %}
WHERE DATE(event_timestamp)='{{ date }}'
{% endif %}