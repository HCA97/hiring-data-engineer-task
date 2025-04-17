CREATE TABLE IF NOT EXISTS default.advertiser
(
    id UInt32,
    name String,
    updated_at DateTime64 NULL,
    created_at DateTime64 DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (id);

CREATE TABLE IF NOT EXISTS default.campaign
(
    id UInt32,
    name String,
    bid Decimal(10, 2),
    budget Decimal(10, 2),
    start_date Date NULL,
    end_date Date NULL, 
    advertiser_id UInt32 NULL,
    updated_at DateTime64 NULL,
    created_at DateTime64 DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (id);

CREATE TABLE IF NOT EXISTS default.impressions_dump
(
    id UInt32,
    campaign_id UInt32 NULL,
    created_at DateTime64 DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (id)
PARTITION BY toDate(created_at);

CREATE TABLE IF NOT EXISTS default.clicks_dump
(
    id UInt32,
    campaign_id UInt32 NULL,
    created_at DateTime64 DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (id)
PARTITION BY toDate(created_at);


-- impression view

CREATE OR REPLACE VIEW default.impressions AS
SELECT
    i.id as id,
    i.campaign_id as campaign_id,
    i.created_at as created_at
FROM default.campaign ca
JOIN impressions_dump i ON ca.id = i.campaign_id
ORDER BY i.id;

-- clicks materizalied veiw

CREATE OR REPLACE VIEW default.clicks AS
SELECT
    c.id as id,
    c.campaign_id as campaign_id,
    c.created_at as created_at
FROM default.campaign ca
JOIN clicks_dump c ON ca.id = c.campaign_id
ORDER BY c.id;

-- assignment views

-- q1
CREATE OR REPLACE VIEW default.campaigns_ctr AS
SELECT 
    ca.id,
    ca.name,
    ca.budget,
    i.total_impressions AS total_impressions,
    c.total_clicks AS total_clicks,
    CASE 
        WHEN i.total_impressions > 0 THEN 
            ROUND(c.total_clicks / i.total_impressions, 3)
        ELSE NULL 
    END AS total_ctr,
    -- additional column from me (checking the efficiency of the campaign)
    CASE 
        WHEN c.total_clicks > 0 THEN
            ROUND(ca.budget / c.total_clicks, 3)
        ELSE ca.budget
    END AS cost_of_click
FROM campaign ca
LEFT JOIN (
    SELECT campaign_id, COUNT(DISTINCT id) AS total_impressions
    FROM impressions
    GROUP BY campaign_id
) i ON ca.id = i.campaign_id
LEFT JOIN (
    SELECT campaign_id, COUNT(DISTINCT id) AS total_clicks
    FROM clicks
    GROUP BY campaign_id
) c ON ca.id = c.campaign_id
ORDER BY cost_of_click DESC;


-- q2
CREATE OR REPLACE VIEW default.daily_impressions_clicks AS
SELECT
    ii.date AS date,
    ii.daily_impressions AS daily_impressions,
    cc.daily_clicks AS daily_clicks
FROM (
    SELECT 
        toDate(i.created_at) AS date, 
        COUNT(*) AS daily_impressions
    FROM impressions i
    WHERE i.created_at IS NOT NULL
    GROUP BY toDate(i.created_at)
) ii
LEFT JOIN (
    SELECT 
        toDate(c.created_at) AS date, 
        COUNT(*) AS daily_clicks
    FROM clicks c
    WHERE c.created_at IS NOT NULL
    GROUP BY toDate(c.created_at)
) cc 
ON ii.date = cc.date
ORDER BY ii.date;


