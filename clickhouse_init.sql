CREATE TABLE IF NOT EXISTS default.campaign_flat
(
    `campaign_id` Nullable(Int32),
    `campaign_name` Nullable(String),
    `bid` Nullable(Decimal(10,
 2)),
    `budget` Nullable(Decimal(10,
 2)),
    `start_date` Nullable(Date),
    `end_date` Nullable(Date),
    `campaign_created_at` Nullable(DateTime64(6)),
    `campaign_updated_at` Nullable(DateTime64(6)),
    `advertiser_id` Nullable(Int32),
    `advertiser_name` Nullable(String),
    `advertiser_created_at` Nullable(DateTime64(6)),
    `advertiser_updated_at` Nullable(DateTime64(6)),
    `impression_id` Nullable(Int32),
    `impression_created_at` Nullable(DateTime64(6)),
    `click_id` Nullable(Int32),
    `click_created_at` Nullable(DateTime64(6))
) ENGINE = PostgreSQL('postgres:5432', 'postgres', 'campaign_flat', 'postgres', 'postgres');


-- CREATE TABLE IF NOT EXISTS default.impressions
-- ENGINE = PostgreSQL('postgres:5432', 'postgres', 'impressions', 'postgres', 'postgres');


-- CREATE TABLE IF NOT EXISTS default.clicks
-- ENGINE = PostgreSQL('postgres:5432', 'postgres', 'clicks', 'postgres', 'postgres');

-- CREATE TABLE IF NOT EXISTS default.advertiser
-- ENGINE = PostgreSQL('postgres:5432', 'postgres', 'advertiser', 'postgres', 'postgres');


-- CREATE TABLE IF NOT EXISTS default.campaign
-- ENGINE = PostgreSQL('postgres:5432', 'postgres', 'campaign', 'postgres', 'postgres');