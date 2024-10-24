WITH category_data AS (
    SELECT DISTINCT
        itemid,
        TRIM(value)::bigint AS categoryid  -- Cast to bigint
    FROM 
        item_properties_part1
    WHERE 
        TRIM(property) = 'categoryid'

    UNION ALL

    SELECT DISTINCT
        itemid,
        TRIM(value)::bigint AS categoryid  -- Cast to bigint
    FROM 
        item_properties_part2
    WHERE 
        TRIM(property) = 'categoryid'
)

SELECT 
    cd.itemid,
    cd.categoryid,
    ct.parentid  -- Include parentid from category_tree
FROM 
    category_data cd
LEFT JOIN 
    category_tree ct ON cd.categoryid = ct.categoryid;