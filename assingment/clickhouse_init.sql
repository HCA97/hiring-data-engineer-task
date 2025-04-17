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

CREATE TABLE IF NOT EXISTS default.impressions
(
    id UInt32,
    campaign_id UInt32 NULL,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (id, created_at)
PARTITION BY toDate(created_at);

CREATE TABLE IF NOT EXISTS default.clicks
(
    id UInt32,
    campaign_id UInt32 NULL,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (id, created_at)
PARTITION BY toDate(created_at);


CREATE TABLE IF NOT EXISTS default.daily_overview
(
    `on_date` Date,
    `campaign_id` UInt32,
    `impressions` SimpleAggregateFunction(sum, UInt64),
    `clicks` SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree 
ORDER BY (on_date, campaign_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.daily_impressions_mv
TO default.daily_overview
AS
SELECT
    toDate(created_at) AS on_date,
    campaign_id,
    count() AS impressions,
    0 clicks         ---<<<--- if you omit this, it will be the same 0
FROM
    default.impressions
GROUP BY
    toDate(created_at) AS on_date,
    campaign_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS default.daily_clicks_mv
TO default.daily_overview
AS
SELECT
    toDate(created_at) AS on_date,
    campaign_id,
    count() AS clicks,
    0 impressions    ---<<<--- if you omit this, it will be the same 0
FROM
    default.clicks
GROUP BY
    toDate(created_at) AS on_date,
    campaign_id;

-- assignment views

-- q1
CREATE OR REPLACE VIEW default.campaigns_ctr AS
SELECT 
    ca.id,
    ca.name,
    ca.budget,
    do.total_impressions AS total_impressions,
    do.total_clicks AS total_clicks,
    CASE 
        WHEN do.total_impressions > 0 THEN 
            ROUND(do.total_clicks / do.total_impressions, 3)
        ELSE NULL 
    END AS total_ctr,
    -- additional column from me (checking the efficiency of the campaign)
    CASE 
        WHEN do.total_clicks > 0 THEN
            ROUND(ca.budget / do.total_clicks, 3)
        ELSE ca.budget
    END AS cost_of_click
FROM campaign ca
LEFT JOIN (
    SELECT  campaign_id,
            SUM(impressions) AS total_impressions,
            SUM(clicks) AS total_clicks
    FROM daily_overview
    GROUP BY campaign_id
) do ON ca.id = do.campaign_id
ORDER BY cost_of_click DESC;


-- q2
CREATE OR REPLACE VIEW default.daily_impressions_clicks AS
SELECT
    on_date,
    sum(impressions) AS daily_impressions,
    sum(clicks) AS daily_clicks
FROM
    default.daily_overview
GROUP BY
    on_date;