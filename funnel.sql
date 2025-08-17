WITH ordered_events AS (
    SELECT
        user_id,
        platform,
        event_name,
        timestamp
    FROM user_events
),

-- Step 1: PageView
pageviews AS (
    SELECT user_id, platform, MIN(timestamp) AS pageview_time
    FROM ordered_events
    WHERE event_name = 'PageView'
    GROUP BY user_id, platform
),

-- Step 2: Download within 72h of PageView
downloads AS (
    SELECT p.user_id, p.platform, MIN(d.timestamp) AS download_time
    FROM pageviews p
    JOIN ordered_events d
      ON d.user_id = p.user_id
     AND d.platform = p.platform
     AND d.event_name = 'Download'
     AND d.timestamp > p.pageview_time
     AND d.timestamp <= p.pageview_time + INTERVAL '72 hours'
    GROUP BY p.user_id, p.platform
),

-- Step 3: Install within 72h of Download
installs AS (
    SELECT d.user_id, d.platform, MIN(i.timestamp) AS install_time
    FROM downloads d
    JOIN ordered_events i
      ON i.user_id = d.user_id
     AND i.platform = d.platform
     AND i.event_name = 'Install'
     AND i.timestamp > d.download_time
     AND i.timestamp <= d.download_time + INTERVAL '72 hours'
    GROUP BY d.user_id, d.platform
),

-- Step 4: Purchase within 72h of Install
purchases AS (
    SELECT i.user_id, i.platform, MIN(pu.timestamp) AS purchase_time
    FROM installs i
    JOIN ordered_events pu
      ON pu.user_id = i.user_id
     AND pu.platform = i.platform
     AND pu.event_name = 'Purchase'
     AND pu.timestamp > i.install_time
     AND pu.timestamp <= i.install_time + INTERVAL '72 hours'
    GROUP BY i.user_id, i.platform
),

-- Funnel: combine all steps
funnel AS (
    SELECT
        p.user_id,
        p.platform,
        p.pageview_time,
        d.download_time,
        i.install_time,
        pu.purchase_time
    FROM pageviews p
    LEFT JOIN downloads d ON d.user_id = p.user_id AND d.platform = p.platform
    LEFT JOIN installs i  ON i.user_id = p.user_id AND i.platform = p.platform
    LEFT JOIN purchases pu ON pu.user_id = p.user_id AND pu.platform = p.platform
)

-- Final aggregation with counts + conversion rates
SELECT
    platform,
    COUNT(DISTINCT CASE WHEN pageview_time IS NOT NULL THEN user_id END) AS pageviews,
    COUNT(DISTINCT CASE WHEN download_time IS NOT NULL THEN user_id END) AS downloads,
    COUNT(DISTINCT CASE WHEN install_time IS NOT NULL THEN user_id END) AS installs,
    COUNT(DISTINCT CASE WHEN purchase_time IS NOT NULL THEN user_id END) AS purchases,

    ROUND(
        CAST(COUNT(DISTINCT CASE WHEN download_time IS NOT NULL THEN user_id END) AS DECIMAL(10,3))
        / NULLIF(COUNT(DISTINCT CASE WHEN pageview_time IS NOT NULL THEN user_id END), 0),
        3
    ) AS cv_pageview_to_download,

    ROUND(
        CAST(COUNT(DISTINCT CASE WHEN install_time IS NOT NULL THEN user_id END) AS DECIMAL(10,3))
        / NULLIF(COUNT(DISTINCT CASE WHEN download_time IS NOT NULL THEN user_id END), 0),
        3
    ) AS cv_download_to_install,

    ROUND(
        CAST(COUNT(DISTINCT CASE WHEN purchase_time IS NOT NULL THEN user_id END) AS DECIMAL(10,3))
        / NULLIF(COUNT(DISTINCT CASE WHEN install_time IS NOT NULL THEN user_id END), 0),
        3
    ) AS cv_install_to_purchase

FROM funnel
GROUP BY platform
ORDER BY platform;

