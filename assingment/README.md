# Sync data from Postgresql to Clickhouse

End-to-end pipeline to sync data from PostgreSQL into ClickHouse.

## Arhitecture

```
PostgreSQL ──(Python Sync Job)──> ClickHouse
```

* **PostgreSQL:** Stores the source data for `advertisers`, `campaigns`, `clicks`, and `impressions`
* **Python Sync Job:** Periodically dumps data to Clickhouse from PostgreSQL using `postgresql` Table Function
* **ClickHouse:** Calculate KPI Queries (CTR, dailiy impressions)

## Data Transfer Flow

**Data Dump via SQL:** The Python syncer triggers ClickHouse SQL commands to load data from PostgreSQL. For small tables (like advertiser and campaign), data is fully replaced, while for large tables (clicks and impressions), data is loaded incrementally.

**Handling CRUD operations:** 
* For small tables like `advertiser` and `campaign`, we perform full loads, so handling inserts, updates and deletes is not a problem.
* For larger tables like `clicks` and `impressions`, we only handle inserts (there are no updates for these tables). To handle deletes (e.g., when a campaign is removed from the source database), we join with the `campaign` table in our views. This ensures we only retain impressions and clicks associated with valid campaign IDs. 

## Simple Testing

### 1. Test Large Syncs (1M impressions)

1. Delete all the data and populate the postgresql and sync to clikchouse.
    ```bash
    export INIT_SQL_FILE=assingment/clickhouse_init.sql
    docker compose -f 'docker-compose.yaml' down -v
    docker compose -f 'docker-compose.yaml' up --build clickhouse -d
    uv run python main.py batch --advertisers 10 --campaigns 10 --impressions 10000 --ctr 0.08
    uv run python assingment/data_sync.py --run-once
    ```
    <details> <summary>Logs</summary>

    ```bash
    [+] Running 6/6
    ✔ Container ch_analytics                            Removed                                                                                                  0.0s 
    ✔ Container db_migrations                           Removed                                                                                                  0.0s 
    ✔ Container psql_source                             Removed                                                                                                  0.5s 
    ✔ Volume hiring-data-engineer-task_clickhouse_data  Removed                                                                                                  0.1s 
    ✔ Volume hiring-data-engineer-task_postgres_data    Removed                                                                                                  0.3s 
    ✔ Network hiring-data-engineer-task_default         Removed                                                                                                  0.2s 
    [+] Running 6/6
    ✔ Network hiring-data-engineer-task_default           Created                                                                                                0.0s 
    ✔ Volume "hiring-data-engineer-task_clickhouse_data"  Created                                                                                                0.0s 
    ✔ Volume "hiring-data-engineer-task_postgres_data"    Created                                                                                                0.0s 
    ✔ Container psql_source                               Healthy                                                                                                5.8s 
    ✔ Container db_migrations                             Exited                                                                                                 6.9s 
    ✔ Container ch_analytics                              Started                                                                                                7.1s 
    Creating 10 advertisers...
    Creating 10 campaigns per advertiser...
    Creating ~10000 impressions per campaign...
    Creating clicks with approximately 8.0% CTR...
    Seeding complete!
    Created 10 advertisers with 100 campaigns total.
    Generated approximately 1000000 impressions and 80000 clicks.
    2025-04-17 17:37:23,965 - root - INFO - Initializing ClickHouse sync scheduler
    2025-04-17 17:37:23,974 - root - INFO - Connected to ClickHouse successfully
    2025-04-17 17:37:23,986 - root - INFO - Starting scheduled sync job
    2025-04-17 17:37:23,993 - root - INFO - [ADVERTISER ] Data sync completed successfully |        10 rows inserted | Time taken:   0.01 seconds
    2025-04-17 17:37:24,000 - root - INFO - [CAMPAIGN   ] Data sync completed successfully |       100 rows inserted | Time taken:   0.01 seconds
    2025-04-17 17:37:24,040 - root - INFO - [CLICKS     ] Data sync completed successfully |     80000 rows inserted | Time taken:   0.04 seconds
    2025-04-17 17:37:24,377 - root - INFO - [IMPRESSIONS] Data sync completed successfully |   1000000 rows inserted | Time taken:   0.34 seconds
    2025-04-17 17:37:24,378 - root - INFO - Finished scheduled sync job
    ```
    </details>
2. Check if all the data is there;

    ```bash
    docker exec -it ch_analytics clickhouse-client "SELECT
    COUNT(distinct ca.id) AS total_campaign,
    COUNT(distinct a.id) AS total_advertiser,
    COUNT(distinct i.id) AS total_impressions,
    COUNT(distinct c.id) AS total_clicks
    FROM campaign ca
    JOIN advertiser a on a.id = ca.advertiser_id
    JOIN impressions i ON ca.id = i.campaign_id
    JOIN clicks c ON ca.id = c.campaign_id 
    FORMAT PrettyCompact;"
    ```

    **Result:**
    ```bash
           ┌─total_campaign─┬─total_advertiser─┬─total_impressions─┬─total_clicks─┐
        1. │            100 │               10 │           1000000 │        80000 │
           └────────────────┴──────────────────┴───────────────────┴──────────────┘
    ```

### 2. Check KPI Queries If impelemented Correct or Not

1. Remove all volumes, populate the data, create campaigns with no clicks but with impressions, and campaigns with no impressions. Finally, sync to ClickHouse.
    ```bash
    export INIT_SQL_FILE=assingment/clickhouse_init.sql
    docker compose -f 'docker-compose.yaml' down -v
    docker compose -f 'docker-compose.yaml' up --build clickhouse -d
    uv run python main.py batch --advertisers 1 --campaigns 10 --impressions 1000 --ctr 0.08
    # New camapaigns [11, 12]
    uv run python main.py campaigns --advertiser-id 1 --count 2
    # Add impressions to the camapaign 11 but no clicks
    uv run python main.py impressions --campaign-id 11 --count 500
    uv run python assingment/data_sync.py --run-once
    ```
2. Check ctr by campaing, all should be `0.08` and last two should be `0` and `NULL`.
    ```bash
    docker exec -it ch_analytics clickhouse-client "SELECT * FROM campaigns_ctr FORMAT PrettyCompact;"
    ```

    **Result:**
    ```bash
        ┌─id─┬─name──────────┬─budget─┬─total_impressions─┬─total_clicks─┬─total_ctr─┬─cost_of_click─┐
     1. │ 11 │ Campaign_1_1  │ 352.33 │               500 │            0 │         0 │        352.33 │
     2. │ 12 │ Campaign_1_2  │ 250.44 │                 0 │            0 │      ᴺᵁᴸᴸ │        250.44 │
     3. │ 10 │ Campaign_1_10 │ 340.77 │              1000 │           80 │      0.08 │          4.25 │
     4. │  7 │ Campaign_1_7  │ 297.35 │              1000 │           80 │      0.08 │          3.71 │
     5. │  2 │ Campaign_1_2  │ 287.48 │              1000 │           80 │      0.08 │          3.59 │
     6. │  8 │ Campaign_1_8  │  263.8 │              1000 │           80 │      0.08 │          3.29 │
     7. │  9 │ Campaign_1_9  │ 205.07 │              1000 │           80 │      0.08 │          2.56 │
     8. │  1 │ Campaign_1_1  │ 130.35 │              1000 │           80 │      0.08 │          1.62 │
     9. │  4 │ Campaign_1_4  │ 100.23 │              1000 │           80 │      0.08 │          1.25 │
    10. │  5 │ Campaign_1_5  │  96.13 │              1000 │           80 │      0.08 │           1.2 │
    11. │  6 │ Campaign_1_6  │  93.87 │              1000 │           80 │      0.08 │          1.17 │
    12. │  3 │ Campaign_1_3  │   89.4 │              1000 │           80 │      0.08 │          1.11 │
        └────┴───────────────┴────────┴───────────────────┴──────────────┴───────────┴───────────────┘
    ```
3. Check if total sum of daily impressions and clicks equivals to total impressions and clicks 
    ```bash
    docker exec -it ch_analytics clickhouse-client "SELECT 
        SUM(daily_impressions) AS total_impressions, 
        SUM(daily_clicks) AS total_clicks 
    FROM daily_impressions_clicks FORMAT PrettyCompact;"
    ```

    **Result:**
    ```bash
       ┌─total_impressions─┬─total_clicks─┐
    1. │            10500 │         800 │
       └───────────────────┴──────────────┘
   ```

## Future Work

1. Replace Python scripts with user-defined functions scheduled via cron jobs.
2. Improve data architecture by using materialized views instead of regular views.
   * Example: [Combining multiple source tables into a single target table](https://clickhouse.com/docs/guides/developer/cascading-materialized-views#combining-multiple-source-tables-to-single-target-table)
3. Handle updates on the `campaign` table efficiently. This table can grow, if we count historical data.
4. Implement automated integration tests using Python. These should replicate our current manual tests, but in a more automated and repeatable way.
