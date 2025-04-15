-- Ref: https://clickhouse.com/docs/integrations/postgresql

CREATE TABLE IF NOT EXISTS default.campaign
ENGINE = PostgreSQL('postgres:5432', 'postgres', 'campaign', 'postgres', 'postgres');

CREATE TABLE IF NOT EXISTS default.clicks
ENGINE = PostgreSQL('postgres:5432', 'postgres', 'clicks', 'postgres', 'postgres');

CREATE TABLE IF NOT EXISTS default.impressions
ENGINE = PostgreSQL('postgres:5432', 'postgres', 'impressions', 'postgres', 'postgres');

CREATE TABLE IF NOT EXISTS default.advertiser
ENGINE = PostgreSQL('postgres:5432', 'postgres', 'advertiser', 'postgres', 'postgres');

