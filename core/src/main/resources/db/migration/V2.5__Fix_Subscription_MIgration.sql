WITH updates as (
    select name,
           array_to_string(array_agg(b.array_to_string), ',') as subscriptions
    from (
             select name, array_to_string(string_to_array(subscriptions, ':') || '{false}', ':')
             from (
                      select name, unnest(string_to_array(subscriptions, ',')) as subscriptions from stores) a
             WHERE array_length(string_to_array(subscriptions, ':'), 1) = 2) b
    group by name
)
UPDATE stores
SET subscriptions = updates.subscriptions
FROM updates
WHERE stores.name = updates.name