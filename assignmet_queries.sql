-- q1
SELECT 
    ca.name,
    COALESCE(i.total_impressions, 0) AS total_impressions,
    COALESCE(c.total_clicks, 0) AS total_clicks,
    CASE 
        WHEN COALESCE(i.total_impressions, 0) > 0 THEN 
            ROUND(COALESCE(c.total_clicks, 0) / i.total_impressions, 3)
        ELSE NULL 
    END AS total_ctr
FROM latest_campaign ca
LEFT JOIN (
    SELECT campaign_id, COUNT(DISTINCT id) AS total_impressions
    FROM latest_impressions
    GROUP BY campaign_id
) i ON ca.id = i.campaign_id
LEFT JOIN (
    SELECT campaign_id, COUNT(DISTINCT id) AS total_clicks
    FROM latest_clicks
    GROUP BY campaign_id
) c ON ca.id = c.campaign_id
ORDER BY total_ctr DESC;

-- Q2
SELECT
    ii.date AS date,
    ii.daily_impressions AS daily_impressions,
    cc.daily_clicks AS daily_clicks,
FROM (
    SELECT 
        toDate(i.created_at) AS date, 
        COUNT(*) AS daily_impressions
    FROM latest_impressions i
    WHERE i.created_at IS NOT NULL
    GROUP BY toDate(i.created_at)
) ii
LEFT JOIN (
    SELECT 
        toDate(c.created_at) AS date, 
        COUNT(*) AS daily_clicks
    FROM latest_clicks c
    WHERE c.created_at IS NOT NULL
    GROUP BY toDate(c.created_at)
) cc 
ON ii.date = cc.date
ORDER BY ii.date;