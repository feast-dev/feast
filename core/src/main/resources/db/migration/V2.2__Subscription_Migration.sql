UPDATE stores
    SET subscriptions = array_to_string(string_to_array(subscriptions, ':') || '{false}', ':')
    WHERE array_length(string_to_array(subscriptions, ':'), 1) = 2;