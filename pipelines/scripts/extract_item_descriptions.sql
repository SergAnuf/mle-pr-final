
SELECT 
    TO_TIMESTAMP(timestamp / 1000) AS timestamp,  -- Convert Unix timestamp (in milliseconds) to proper timestamp
    itemid,
    REPLACE(TRIM(value), E'\n', '') AS value  -- Remove new line characters from value
FROM (
    SELECT 
        timestamp,
        itemid,
        TRIM(value) AS value
    FROM 
        item_properties_part1
    WHERE 
        TRIM(property) = '283'
        AND TRIM(value) !~ '^n'  -- Exclude values that start with 'n'

    UNION ALL

    SELECT 
        timestamp,
        itemid,
        TRIM(value) AS value
    FROM 
        item_properties_part2
    WHERE 
        TRIM(property) = '283'
        AND TRIM(value) !~ '^n'  -- Exclude values that start with 'n'
) AS combined