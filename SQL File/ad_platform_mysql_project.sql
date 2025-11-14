
-- =====================================================================
-- DIGITAL ADS ANALYTICS PROJECT - MySQL END-TO-END SCRIPT
-- Dataset: users, campaigns, ads, ad_events
--
-- COVERS:
--   1. DATA PREPARATION  (tables, constraints, validation checks, cleaning)
--   2. DATA MODELLING    (FK relationships, analytic views)
--   3. DATA ANALYSIS     (KPI queries, churn, revenue, segmentation)
--   4. PRESENTATION      (BI-friendly views, summary table, export example)
--   5. IMPROVEMENTS      (indexes, stored procedures, events, risk flags)
--   6. DATA GOVERNANCE   (roles & permissions examples)
--   7. OPTIONAL STEP     (audit / logging structure)
--
-- HOW TO USE:
--   - Run this script in MySQL Workbench / phpMyAdmin / CLI.
--   - Then import the CSV data (users, campaigns, ads, ad_events)
--     into the corresponding tables using LOAD DATA / import wizard.
-- =====================================================================

-- *********************************************************************
-- 0. PROJECT SETUP
-- *********************************************************************

DROP DATABASE IF EXISTS ad_platform;
CREATE DATABASE ad_platform;
USE ad_platform;

SET NAMES utf8mb4;
SET sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION';

-- *********************************************************************
-- 1. DATA PREPARATION (tables, constraints, validation checks, cleaning)
-- *********************************************************************

-- =========================
-- 1.1 CORE TABLES (DDL)
-- =========================

-- 1.1.1 users
-- NOTE: In the CSV user_id can be numeric-looking or alphanumeric,
-- so we model it as VARCHAR.
DROP TABLE IF EXISTS users;
CREATE TABLE users (
    user_id      VARCHAR(50) PRIMARY KEY,
    user_gender  ENUM('Male','Female','Other') NULL,
    user_age     INT,
    age_group    VARCHAR(20),
    country      VARCHAR(50),
    location     VARCHAR(100),
    interests    VARCHAR(255)
) ENGINE=InnoDB;

-- 1.1.2 campaigns
DROP TABLE IF EXISTS campaigns;
CREATE TABLE campaigns (
    campaign_id   INT PRIMARY KEY,
    name          VARCHAR(150)           NOT NULL,
    start_date    DATE,
    end_date      DATE,
    duration_days INT,
    total_budget  DECIMAL(14,2),
    INDEX idx_campaign_dates (start_date, end_date)
) ENGINE=InnoDB;

-- 1.1.3 ads
DROP TABLE IF EXISTS ads;
CREATE TABLE ads (
    ad_id             INT PRIMARY KEY,
    campaign_id       INT                        NOT NULL,
    ad_platform       VARCHAR(50),  -- e.g. Facebook, Instagram, Google
    ad_type           VARCHAR(50),  -- e.g. Video, Image, Text
    target_gender     VARCHAR(50),
    target_age_group  VARCHAR(50),
    target_interests  VARCHAR(255),
    CONSTRAINT fk_ads_campaign
        FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    INDEX idx_ads_campaign      (campaign_id),
    INDEX idx_ads_platform_type (ad_platform, ad_type)
) ENGINE=InnoDB;

-- 1.1.4 ad_events
DROP TABLE IF EXISTS ad_events;
CREATE TABLE ad_events (
    event_id     BIGINT PRIMARY KEY,
    ad_id        INT                        NOT NULL,
    user_id      VARCHAR(50),
    timestamp    DATETIME,
    day_of_week  VARCHAR(20),
    time_of_day  VARCHAR(20),
    event_type   VARCHAR(30),  -- e.g. Impression, Click, Like, Share, Conversion
    CONSTRAINT fk_events_ad
        FOREIGN KEY (ad_id) REFERENCES ads(ad_id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_events_user
        FOREIGN KEY (user_id) REFERENCES users(user_id)
        ON UPDATE CASCADE ON DELETE SET NULL,
    INDEX idx_events_ad        (ad_id),
    INDEX idx_events_user      (user_id),
    INDEX idx_events_timestamp (timestamp),
    INDEX idx_events_type      (event_type)
) ENGINE=InnoDB;

-- NOTE:
--  AFTER CREATING TABLES, IMPORT YOUR CSV FILES:
--   - users.csv      -> users
--   - campaigns.csv  -> campaigns
--   - ads.csv        -> ads
--   - ad_events.csv  -> ad_events
--
-- Example LOAD DATA (adjust path & options as per your server).
-- For campaigns with DD-MM-YYYY date format:
-- LOAD DATA INFILE '/path/to/campaigns.csv'
-- INTO TABLE campaigns
-- FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
-- IGNORE 1 LINES
-- (@campaign_id, @name, @start_date, @end_date, @duration_days, @total_budget)
-- SET
--   campaign_id   = @campaign_id,
--   name          = @name,
--   start_date    = STR_TO_DATE(@start_date,'%d-%m-%Y'),
--   end_date      = STR_TO_DATE(@end_date,'%d-%m-%Y'),
--   duration_days = @duration_days,
--   total_budget  = @total_budget;

-- =========================
-- 1.2 DATA QUALITY / VALIDATION CHECKS
-- =========================

-- 1.2.1 Record counts per table
SELECT 'users' AS table_name, COUNT(*) AS row_count FROM users
UNION ALL
SELECT 'campaigns', COUNT(*) FROM campaigns
UNION ALL
SELECT 'ads', COUNT(*) FROM ads
UNION ALL
SELECT 'ad_events', COUNT(*) FROM ad_events;

-- 1.2.2 Duplicate checks

-- Duplicate users by user_id (should be unique)
SELECT user_id, COUNT(*) AS cnt
FROM users
GROUP BY user_id
HAVING cnt > 1;

-- Duplicate campaigns by campaign_id
SELECT campaign_id, COUNT(*) AS cnt
FROM campaigns
GROUP BY campaign_id
HAVING cnt > 1;

-- Duplicate ads by ad_id
SELECT ad_id, COUNT(*) AS cnt
FROM ads
GROUP BY ad_id
HAVING cnt > 1;

-- 1.2.3 NULL / Missing critical data

-- Users missing gender or age (profiling completeness)
SELECT *
FROM users
WHERE user_gender IS NULL
   OR user_age IS NULL;

-- Ads with missing campaign_id
SELECT *
FROM ads
WHERE campaign_id IS NULL;

-- Events with missing ad_id, user_id, timestamp or event_type
SELECT *
FROM ad_events
WHERE ad_id IS NULL
   OR timestamp IS NULL
   OR event_type IS NULL;

-- 1.2.4 Date sanity checks

-- Campaign end date before start date
SELECT *
FROM campaigns
WHERE end_date IS NOT NULL
  AND start_date IS NOT NULL
  AND end_date < start_date;

-- Events outside campaign period (join ad->campaign)
SELECT e.*
FROM ad_events e
JOIN ads a       ON a.ad_id = e.ad_id
JOIN campaigns c ON c.campaign_id = a.campaign_id
WHERE e.timestamp IS NOT NULL
  AND c.start_date IS NOT NULL
  AND c.end_date IS NOT NULL
  AND (DATE(e.timestamp) < c.start_date OR DATE(e.timestamp) > c.end_date);

-- 1.2.5 Orphaned foreign keys (after import, if FKs disabled)

-- Ads referencing non-existing campaign
SELECT a.*
FROM ads a
LEFT JOIN campaigns c ON c.campaign_id = a.campaign_id
WHERE c.campaign_id IS NULL;

-- Events referencing non-existing ad
SELECT e.*
FROM ad_events e
LEFT JOIN ads a ON a.ad_id = e.ad_id
WHERE a.ad_id IS NULL;

-- Events referencing non-existing user
SELECT e.*
FROM ad_events e
LEFT JOIN users u ON u.user_id = e.user_id
WHERE e.user_id IS NOT NULL
  AND u.user_id IS NULL;

-- =========================
-- 1.3 SAMPLE DATA CLEANING ACTIONS
-- =========================

-- 1.3.1 Trim whitespace in text columns
UPDATE users
SET user_gender = TRIM(user_gender),
    age_group   = TRIM(age_group),
    country     = TRIM(country),
    location    = TRIM(location),
    interests   = TRIM(interests);

UPDATE campaigns
SET name = TRIM(name);

UPDATE ads
SET ad_platform      = TRIM(ad_platform),
    ad_type          = TRIM(ad_type),
    target_gender    = TRIM(target_gender),
    target_age_group = TRIM(target_age_group),
    target_interests = TRIM(target_interests);

UPDATE ad_events
SET day_of_week = TRIM(day_of_week),
    time_of_day = TRIM(time_of_day),
    event_type  = TRIM(event_type);

-- 1.3.2 Standardize user_gender values (if any free-text values exist)
UPDATE users
SET user_gender = 'Male'
WHERE user_gender IS NOT NULL AND LOWER(user_gender) IN ('m','male');

UPDATE users
SET user_gender = 'Female'
WHERE user_gender IS NOT NULL AND LOWER(user_gender) IN ('f','female');

-- 1.3.3 Fix negative or NULL budget (set to 0 if negative)
UPDATE campaigns
SET total_budget = 0
WHERE total_budget IS NULL OR total_budget < 0;

-- 1.3.4 Cap unrealistic ages (e.g. age > 100 or < 0)
UPDATE users
SET user_age = NULL
WHERE user_age < 0 OR user_age > 110;

-- *********************************************************************
-- 2. DATA MODELLING (FK relationships, analytic views)
-- *********************************************************************

-- Relationships:
--   campaigns (1) --- (N) ads
--   ads       (1) --- (N) ad_events
--   users     (1) --- (N) ad_events

-- =========================
-- 2.1 ANALYTIC VIEWS
-- =========================

-- 2.1.1 Campaign-level performance aggregation
DROP VIEW IF EXISTS vw_campaign_performance;
CREATE VIEW vw_campaign_performance AS
SELECT
    c.campaign_id,
    c.name AS campaign_name,
    c.start_date,
    c.end_date,
    c.total_budget,
    COUNT(DISTINCT a.ad_id) AS total_ads,
    COUNT(*) FILTER (WHERE e.event_type = 'Impression') AS impressions_dummy -- placeholder (MySQL doesn't support FILTER)
FROM campaigns c
LEFT JOIN ads a       ON a.campaign_id = c.campaign_id
LEFT JOIN ad_events e ON e.ad_id = a.ad_id
GROUP BY
    c.campaign_id,
    c.name,
    c.start_date,
    c.end_date,
    c.total_budget;

-- NOTE: MySQL doesn't support FILTER syntax; recreate properly below.

DROP VIEW IF EXISTS vw_campaign_performance;
CREATE VIEW vw_campaign_performance AS
SELECT
    c.campaign_id,
    c.name AS campaign_name,
    c.start_date,
    c.end_date,
    c.total_budget,
    COUNT(DISTINCT a.ad_id) AS total_ads,
    SUM(CASE WHEN e.event_type = 'Impression' THEN 1 ELSE 0 END) AS total_impressions,
    SUM(CASE WHEN e.event_type = 'Click'      THEN 1 ELSE 0 END) AS total_clicks,
    SUM(CASE WHEN e.event_type = 'Like'       THEN 1 ELSE 0 END) AS total_likes,
    SUM(CASE WHEN e.event_type = 'Share'      THEN 1 ELSE 0 END) AS total_shares,
    SUM(CASE WHEN e.event_type = 'Conversion' THEN 1 ELSE 0 END) AS total_conversions
FROM campaigns c
LEFT JOIN ads a       ON a.campaign_id = c.campaign_id
LEFT JOIN ad_events e ON e.ad_id = a.ad_id
GROUP BY
    c.campaign_id,
    c.name,
    c.start_date,
    c.end_date,
    c.total_budget;

-- 2.1.2 Ad-level performance view
DROP VIEW IF EXISTS vw_ad_performance;
CREATE VIEW vw_ad_performance AS
SELECT
    a.ad_id,
    a.campaign_id,
    c.name AS campaign_name,
    a.ad_platform,
    a.ad_type,
    a.target_gender,
    a.target_age_group,
    a.target_interests,
    SUM(CASE WHEN e.event_type = 'Impression' THEN 1 ELSE 0 END) AS impressions,
    SUM(CASE WHEN e.event_type = 'Click'      THEN 1 ELSE 0 END) AS clicks,
    SUM(CASE WHEN e.event_type = 'Like'       THEN 1 ELSE 0 END) AS likes,
    SUM(CASE WHEN e.event_type = 'Share'      THEN 1 ELSE 0 END) AS shares,
    SUM(CASE WHEN e.event_type = 'Conversion' THEN 1 ELSE 0 END) AS conversions
FROM ads a
LEFT JOIN campaigns c ON c.campaign_id = a.campaign_id
LEFT JOIN ad_events e ON e.ad_id = a.ad_id
GROUP BY
    a.ad_id,
    a.campaign_id,
    c.name,
    a.ad_platform,
    a.ad_type,
    a.target_gender,
    a.target_age_group,
    a.target_interests;

-- 2.1.3 User engagement view
DROP VIEW IF EXISTS vw_user_engagement;
CREATE VIEW vw_user_engagement AS
SELECT
    u.user_id,
    u.user_gender,
    u.user_age,
    u.age_group,
    u.country,
    u.location,
    u.interests,
    COUNT(DISTINCT e.ad_id) AS ads_seen,
    SUM(CASE WHEN e.event_type = 'Impression' THEN 1 ELSE 0 END) AS impressions,
    SUM(CASE WHEN e.event_type = 'Click'      THEN 1 ELSE 0 END) AS clicks,
    SUM(CASE WHEN e.event_type = 'Like'       THEN 1 ELSE 0 END) AS likes,
    SUM(CASE WHEN e.event_type = 'Share'      THEN 1 ELSE 0 END) AS shares,
    SUM(CASE WHEN e.event_type = 'Conversion' THEN 1 ELSE 0 END) AS conversions
FROM users u
LEFT JOIN ad_events e ON e.user_id = u.user_id
GROUP BY
    u.user_id,
    u.user_gender,
    u.user_age,
    u.age_group,
    u.country,
    u.location,
    u.interests;

-- *********************************************************************
-- 3. DATA ANALYSIS (KPI queries, churn, revenue, segmentation)
-- *********************************************************************

-- =========================
-- 3.1 OVERALL KPIs
-- =========================

-- 3.1.1 Total users and events
SELECT
    (SELECT COUNT(*) FROM users)      AS total_users,
    (SELECT COUNT(*) FROM ad_events)  AS total_events;

-- 3.1.2 Overall event mix
SELECT
    event_type,
    COUNT(*) AS event_count
FROM ad_events
GROUP BY event_type
ORDER BY event_count DESC;

-- 3.1.3 Impressions, clicks, CTR overall
SELECT
    SUM(CASE WHEN event_type = 'Impression' THEN 1 ELSE 0 END) AS impressions,
    SUM(CASE WHEN event_type = 'Click'      THEN 1 ELSE 0 END) AS clicks,
    ROUND(
        SUM(CASE WHEN event_type = 'Click' THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN event_type = 'Impression' THEN 1 ELSE 0 END),0) * 100,
        2
    ) AS ctr_percent
FROM ad_events;

-- 3.1.4 Engagement by platform
SELECT
    ap.ad_platform,
    SUM(CASE WHEN e.event_type = 'Impression' THEN 1 ELSE 0 END) AS impressions,
    SUM(CASE WHEN e.event_type = 'Click'      THEN 1 ELSE 0 END) AS clicks,
    SUM(CASE WHEN e.event_type = 'Like'       THEN 1 ELSE 0 END) AS likes,
    SUM(CASE WHEN e.event_type = 'Share'      THEN 1 ELSE 0 END) AS shares,
    ROUND(
        SUM(CASE WHEN e.event_type = 'Click' THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN e.event_type = 'Impression' THEN 1 ELSE 0 END),0) * 100,
        2
    ) AS ctr_percent
FROM ads ap
LEFT JOIN ad_events e ON e.ad_id = ap.ad_id
GROUP BY ap.ad_platform
ORDER BY impressions DESC;

-- =========================
-- 3.2 CAMPAIGN ANALYSIS
-- =========================

-- 3.2.1 Top 5 campaigns by impressions
SELECT *
FROM vw_campaign_performance
ORDER BY total_impressions DESC
LIMIT 5;

-- 3.2.2 Top 5 campaigns by clicks
SELECT *
FROM vw_campaign_performance
ORDER BY total_clicks DESC
LIMIT 5;

-- 3.2.3 Campaign CTR & cost metrics (using total_budget)
SELECT
    campaign_id,
    campaign_name,
    total_budget,
    total_impressions,
    total_clicks,
    ROUND(total_clicks / NULLIF(total_impressions,0) * 100, 2) AS ctr_percent,
    CASE
        WHEN total_clicks > 0 THEN ROUND(total_budget / total_clicks, 4)
        ELSE NULL
    END AS cost_per_click,
    CASE
        WHEN total_impressions > 0 THEN ROUND(total_budget / total_impressions, 6)
        ELSE NULL
    END AS cost_per_impression
FROM vw_campaign_performance
ORDER BY ctr_percent DESC;

-- =========================
-- 3.3 USER SEGMENTATION
-- =========================

-- 3.3.1 Engagement by country
SELECT
    u.country,
    COUNT(DISTINCT u.user_id) AS users_count,
    SUM(CASE WHEN e.event_type = 'Impression' THEN 1 ELSE 0 END) AS impressions,
    SUM(CASE WHEN e.event_type = 'Click'      THEN 1 ELSE 0 END) AS clicks,
    ROUND(
        SUM(CASE WHEN e.event_type = 'Click' THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN e.event_type = 'Impression' THEN 1 ELSE 0 END),0) * 100,
        2
    ) AS ctr_percent
FROM users u
LEFT JOIN ad_events e ON e.user_id = u.user_id
GROUP BY u.country
ORDER BY impressions DESC;

-- 3.3.2 Engagement by age_group
SELECT
    u.age_group,
    COUNT(DISTINCT u.user_id) AS users_count,
    SUM(CASE WHEN e.event_type = 'Impression' THEN 1 ELSE 0 END) AS impressions,
    SUM(CASE WHEN e.event_type = 'Click'      THEN 1 ELSE 0 END) AS clicks,
    ROUND(
        SUM(CASE WHEN e.event_type = 'Click' THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN e.event_type = 'Impression' THEN 1 ELSE 0 END),0) * 100,
        2
    ) AS ctr_percent
FROM users u
LEFT JOIN ad_events e ON e.user_id = u.user_id
GROUP BY u.age_group
ORDER BY ctr_percent DESC;

-- =========================
-- 3.4 BASIC "CHURN" STYLE ANALYSIS
--      (Users not seen in events recently)
-- =========================

-- Users with no events in last 30 days (potentially churned)
SELECT
    u.user_id,
    u.user_gender,
    u.age_group,
    u.country,
    MAX(e.timestamp) AS last_event_time
FROM users u
LEFT JOIN ad_events e ON e.user_id = u.user_id
GROUP BY
    u.user_id,
    u.user_gender,
    u.age_group,
    u.country
HAVING last_event_time IS NULL
    OR last_event_time < DATE_SUB(NOW(), INTERVAL 30 DAY)
ORDER BY last_event_time;

-- *********************************************************************
-- 4. PRESENTATION (BI-friendly views, summary table, export example)
-- *********************************************************************

-- =========================
-- 4.1 DIMENSION & FACT VIEWS
-- =========================

-- User dimension
DROP VIEW IF EXISTS vw_dim_user;
CREATE VIEW vw_dim_user AS
SELECT
    user_id,
    user_gender,
    user_age,
    age_group,
    country,
    location,
    interests
FROM users;

-- Campaign dimension
DROP VIEW IF EXISTS vw_dim_campaign;
CREATE VIEW vw_dim_campaign AS
SELECT
    campaign_id,
    name AS campaign_name,
    start_date,
    end_date,
    duration_days,
    total_budget
FROM campaigns;

-- Ad dimension
DROP VIEW IF EXISTS vw_dim_ad;
CREATE VIEW vw_dim_ad AS
SELECT
    ad_id,
    campaign_id,
    ad_platform,
    ad_type,
    target_gender,
    target_age_group,
    target_interests
FROM ads;

-- Event fact
DROP VIEW IF EXISTS vw_fact_ad_event;
CREATE VIEW vw_fact_ad_event AS
SELECT
    event_id,
    ad_id,
    user_id,
    timestamp,
    DATE(timestamp) AS event_date,
    day_of_week,
    time_of_day,
    event_type
FROM ad_events;

-- =========================
-- 4.2 DAILY CAMPAIGN PERFORMANCE SUMMARY TABLE
-- =========================

DROP TABLE IF EXISTS fact_daily_campaign_performance;
CREATE TABLE fact_daily_campaign_performance (
    campaign_id        INT,
    event_date         DATE,
    impressions        BIGINT,
    clicks             BIGINT,
    likes              BIGINT,
    shares             BIGINT,
    conversions        BIGINT,
    PRIMARY KEY (campaign_id, event_date)
) ENGINE=InnoDB;

-- Initial load
INSERT INTO fact_daily_campaign_performance (
    campaign_id,
    event_date,
    impressions,
    clicks,
    likes,
    shares,
    conversions
)
SELECT
    c.campaign_id,
    DATE(e.timestamp) AS event_date,
    SUM(CASE WHEN e.event_type = 'Impression' THEN 1 ELSE 0 END) AS impressions,
    SUM(CASE WHEN e.event_type = 'Click'      THEN 1 ELSE 0 END) AS clicks,
    SUM(CASE WHEN e.event_type = 'Like'       THEN 1 ELSE 0 END) AS likes,
    SUM(CASE WHEN e.event_type = 'Share'      THEN 1 ELSE 0 END) AS shares,
    SUM(CASE WHEN e.event_type = 'Conversion' THEN 1 ELSE 0 END) AS conversions
FROM campaigns c
JOIN ads a       ON a.campaign_id = c.campaign_id
JOIN ad_events e ON e.ad_id = a.ad_id
GROUP BY c.campaign_id, DATE(e.timestamp);

-- =========================
-- 4.3 EXPORT EXAMPLE (manual)
-- =========================
-- NOTE: Requires FILE privilege and valid path on MySQL server.
-- Example:
-- SELECT * FROM fact_daily_campaign_performance
-- INTO OUTFILE '/var/lib/mysql-files/fact_daily_campaign_performance.csv'
-- FIELDS TERMINATED BY ','
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n';

-- *********************************************************************
-- 5. IMPROVEMENTS (indexes, stored procedures, events, risk flags)
-- *********************************************************************

-- 5.1 ADDITIONAL INDEXES FOR PERFORMANCE

-- Fast filter events by date and type
CREATE INDEX idx_events_date_type
    ON ad_events (DATE(timestamp), event_type);

-- Fast user segmentation by country and age_group
CREATE INDEX idx_users_country_age
    ON users (country, age_group);

-- 5.2 STORED PROCEDURE TO REFRESH DAILY CAMPAIGN PERFORMANCE

DROP PROCEDURE IF EXISTS sp_refresh_daily_campaign_performance;
DELIMITER $$
CREATE PROCEDURE sp_refresh_daily_campaign_performance()
BEGIN
    TRUNCATE TABLE fact_daily_campaign_performance;

    INSERT INTO fact_daily_campaign_performance (
        campaign_id,
        event_date,
        impressions,
        clicks,
        likes,
        shares,
        conversions
    )
    SELECT
        c.campaign_id,
        DATE(e.timestamp) AS event_date,
        SUM(CASE WHEN e.event_type = 'Impression' THEN 1 ELSE 0 END) AS impressions,
        SUM(CASE WHEN e.event_type = 'Click'      THEN 1 ELSE 0 END) AS clicks,
        SUM(CASE WHEN e.event_type = 'Like'       THEN 1 ELSE 0 END) AS likes,
        SUM(CASE WHEN e.event_type = 'Share'      THEN 1 ELSE 0 END) AS shares,
        SUM(CASE WHEN e.event_type = 'Conversion' THEN 1 ELSE 0 END) AS conversions
    FROM campaigns c
    JOIN ads a       ON a.campaign_id = c.campaign_id
    JOIN ad_events e ON e.ad_id = a.ad_id
    GROUP BY c.campaign_id, DATE(e.timestamp);
END$$
DELIMITER ;

-- 5.3 OPTIONAL EVENT TO AUTO-REFRESH DAILY (if event_scheduler=ON)

DROP EVENT IF EXISTS ev_refresh_daily_campaign_performance;
DELIMITER $$
CREATE EVENT ev_refresh_daily_campaign_performance
ON SCHEDULE EVERY 1 DAY
STARTS CURRENT_DATE + INTERVAL 1 DAY
DO
    CALL sp_refresh_daily_campaign_performance();
$$
DELIMITER ;

-- 5.4 CAMPAIGN RISK FLAGS (low CTR or low engagement)

DROP TABLE IF EXISTS campaign_risk_flags;
CREATE TABLE campaign_risk_flags (
    campaign_id INT PRIMARY KEY,
    risk_level  ENUM('Low','Medium','High'),
    reason      VARCHAR(255),
    updated_at  DATETIME
) ENGINE=InnoDB;

DROP PROCEDURE IF EXISTS sp_refresh_campaign_risk;
DELIMITER $$
CREATE PROCEDURE sp_refresh_campaign_risk()
BEGIN
    TRUNCATE TABLE campaign_risk_flags;

    INSERT INTO campaign_risk_flags (campaign_id, risk_level, reason, updated_at)
    SELECT
        cp.campaign_id,
        CASE
            WHEN cp.total_impressions = 0 THEN 'High'
            WHEN cp.total_clicks / cp.total_impressions < 0.005 THEN 'High'   -- CTR < 0.5%
            WHEN cp.total_clicks / cp.total_impressions < 0.01  THEN 'Medium' -- CTR < 1%
            ELSE 'Low'
        END AS risk_level,
        CASE
            WHEN cp.total_impressions = 0 THEN 'No impressions'
            WHEN cp.total_clicks / cp.total_impressions < 0.005 THEN 'Very low CTR (<0.5%)'
            WHEN cp.total_clicks / cp.total_impressions < 0.01  THEN 'Low CTR (<1%)'
            ELSE 'Healthy performance'
        END AS reason,
        NOW() AS updated_at
    FROM vw_campaign_performance cp;
END$$
DELIMITER ;

-- *********************************************************************
-- 6. DATA GOVERNANCE (roles & permissions examples)
-- *********************************************************************

-- These commands require admin privileges; adjust usernames/passwords.

-- 6.1 Read-only BI / Analyst user
-- CREATE USER 'ad_analyst'@'%' IDENTIFIED BY 'StrongPassword123!';
-- GRANT SELECT ON ad_platform.* TO 'ad_analyst'@'%';

-- 6.2 ETL user (can load / update data but not drop database)
-- CREATE USER 'ad_etl'@'%' IDENTIFIED BY 'AnotherStrongPassword!';
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ad_platform.* TO 'ad_etl'@'%';

-- 6.3 Dashboard user (read-only on fact & dim views only)
-- GRANT SELECT ON ad_platform.vw_dim_user       TO 'ad_analyst'@'%';
-- GRANT SELECT ON ad_platform.vw_dim_campaign   TO 'ad_analyst'@'%';
-- GRANT SELECT ON ad_platform.vw_dim_ad         TO 'ad_analyst'@'%';
-- GRANT SELECT ON ad_platform.vw_fact_ad_event  TO 'ad_analyst'@'%';
-- GRANT SELECT ON ad_platform.fact_daily_campaign_performance TO 'ad_analyst'@'%';
-- GRANT SELECT ON ad_platform.vw_campaign_performance TO 'ad_analyst'@'%';
-- GRANT SELECT ON ad_platform.vw_ad_performance       TO 'ad_analyst'@'%';

-- *********************************************************************
-- 7. OPTIONAL STEP: AUDIT / LOGGING STRUCTURE
-- *********************************************************************

DROP TABLE IF EXISTS audit_log;
CREATE TABLE audit_log (
    audit_id    BIGINT PRIMARY KEY AUTO_INCREMENT,
    entity_name VARCHAR(50),
    entity_id   VARCHAR(50),
    action      ENUM('INSERT','UPDATE','DELETE'),
    changed_by  VARCHAR(50),
    changed_at  DATETIME,
    details     TEXT
) ENGINE=InnoDB;

-- Example trigger template (commented out; customize before use):
-- DELIMITER $$
-- CREATE TRIGGER trg_ads_update
-- AFTER UPDATE ON ads
-- FOR EACH ROW
-- BEGIN
--   INSERT INTO audit_log (entity_name, entity_id, action, changed_by, changed_at, details)
--   VALUES (
--     'ads',
--     NEW.ad_id,
--     'UPDATE',
--     USER(),
--     NOW(),
--     CONCAT('Old platform=', OLD.ad_platform, ', New platform=', NEW.ad_platform)
--   );
-- END$$
-- DELIMITER ;

-- *********************************************************************
-- END OF SCRIPT
-- *********************************************************************
