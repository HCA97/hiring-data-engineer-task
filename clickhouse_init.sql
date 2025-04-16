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

-- clicks view

CREATE OR REPLACE VIEW default.clicks AS
SELECT
    c.id as id,
    c.campaign_id as campaign_id,
    c.created_at as created_at
FROM default.campaign ca
JOIN clicks_dump c ON ca.id = c.campaign_id
ORDER BY c.id;