CREATE TABLE IF NOT EXISTS advertiser (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS campaign (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    bid NUMERIC(10,2) NOT NULL,
    budget NUMERIC(10,2) NOT NULL,
    start_date DATE,
    end_date DATE,
    advertiser_id INTEGER REFERENCES advertiser(id),
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS impressions (
    id SERIAL PRIMARY KEY,
    campaign_id INTEGER REFERENCES campaign(id),
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS clicks (
    id SERIAL PRIMARY KEY,
    campaign_id INTEGER REFERENCES campaign(id),
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Materialized view to flatten the campaign data for clickhouse
CREATE MATERIALIZED VIEW IF NOT EXISTS campaign_flat AS
SELECT
    c.id AS campaign_id,
    c.name AS campaign_name,
    c.bid,
    c.budget,
    c.start_date,
    c.end_date,
    c.created_at AS campaign_created_at,
    c.updated_at AS campaign_updated_at,
    a.id AS advertiser_id,
    a.name AS advertiser_name,
    a.created_at AS advertiser_created_at,
    a.updated_at AS advertiser_updated_at,
    i.id AS impression_id,
    i.created_at AS impression_created_at,
    k.id AS click_id,
    k.created_at AS click_created_at
FROM campaign c
LEFT JOIN advertiser a ON c.advertiser_id = a.id
LEFT JOIN impressions i ON i.campaign_id = c.id
LEFT JOIN clicks k ON k.campaign_id = c.id;

-- Add indexes for the key IDs
CREATE INDEX IF NOT EXISTS idx_campaign_id ON campaign_flat(campaign_id);
CREATE INDEX IF NOT EXISTS idx_advertiser_id ON campaign_flat(advertiser_id);
CREATE INDEX IF NOT EXISTS idx_impression_id ON campaign_flat(impression_id);
CREATE INDEX IF NOT EXISTS idx_click_id ON campaign_flat(click_id);

-- Install pg_cron extension
CREATE EXTENSION IF NOT EXISTS pg_cron;
-- Schedule the materialized view refresh every 5 minutes
SELECT cron.schedule('*/5 * * * *', 'REFRESH MATERIALIZED VIEW campaign_flat');
