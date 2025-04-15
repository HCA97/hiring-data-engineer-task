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
    created_at DateTime64 DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (id)
PARTITION BY toDate(created_at);

CREATE TABLE IF NOT EXISTS default.clicks
(
    id UInt32,
    campaign_id UInt32 NULL,
    created_at DateTime64 DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (id)
PARTITION BY toDate(created_at);


-- advertiser view
CREATE VIEW IF NOT EXISTS default.latest_advertiser AS
SELECT
    id,
    name,
    updated_at,
    created_at
FROM (
    SELECT
        id,
        name,
        updated_at,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at, updated_at DESC) AS rn
    FROM default.advertiser
) AS sub
WHERE rn = 1;

-- campaing view
CREATE VIEW IF NOT EXISTS default.latest_campaign AS
SELECT
    id,
    name,
    bid,
    budget,
    start_date,
    end_date,
    advertiser_id,
    updated_at,
    created_at
FROM (
    SELECT
	    id,
	    name,
	    bid,
	    budget,
	    start_date,
	    end_date,
	    advertiser_id,
	    updated_at,
	    created_at,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at, updated_at DESC) AS rn
    FROM default.campaign
) AS sub
WHERE rn = 1;


-- impression view

CREATE VIEW IF NOT EXISTS default.latest_impressions AS
SELECT
    id,
	campaign_id,
	created_at
FROM (
    SELECT
	    id,
	    campaign_id,
	    created_at,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn
    FROM default.impressions
) AS sub
WHERE rn = 1;

-- clicks view

CREATE VIEW IF NOT EXISTS default.latest_clicks AS
SELECT
    id,
	campaign_id,
	created_at
FROM (
    SELECT
	    id,
	    campaign_id,
	    created_at,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn
    FROM default.clicks
) AS sub
WHERE rn = 1;