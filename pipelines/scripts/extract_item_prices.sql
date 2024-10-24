SELECT * FROM (
    SELECT 
        TO_TIMESTAMP(timestamp / 1000) AS timestamp,
        itemid,
        CAST(SUBSTRING(value, 2) AS NUMERIC)::INTEGER AS value
    FROM 
        item_properties_part1
    WHERE 
        TRIM(property) = '790'
) AS part1

UNION ALL

SELECT * FROM (
    SELECT 
        TO_TIMESTAMP(timestamp / 1000) AS timestamp,
        itemid,
        CAST(SUBSTRING(value, 2) AS NUMERIC)::INTEGER AS value
    FROM 
        item_properties_part2
    WHERE 
        TRIM(property) = '790'
) AS part2

ORDER BY 
    timestamp;

