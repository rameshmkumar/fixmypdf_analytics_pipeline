-- Sample Business Intelligence Queries
-- Demonstrates the analytical capabilities of the star schema data warehouse

-- ====================================================================
-- EXECUTIVE DASHBOARD QUERIES
-- ====================================================================

-- 1. Key Performance Indicators (Real-time)
SELECT 
    SUM(total_uploads) as total_user_sessions,
    SUM(total_processing) as active_processing,
    SUM(total_downloads) as completed_transactions,
    ROUND(SUM(total_downloads) * 100.0 / SUM(total_uploads), 2) as overall_conversion_rate,
    COUNT(DISTINCT tool_key) as active_tools,
    COUNT(DISTINCT date) as reporting_days
FROM fact_daily_kpis;

-- 2. Daily Growth Trends
SELECT 
    date,
    total_uploads as initiated_sessions,
    total_downloads as completed_transactions,
    ROUND(total_downloads * 100.0 / total_uploads, 1) as daily_conversion_rate,
    LAG(total_downloads) OVER (ORDER BY date) as prev_day_downloads,
    ROUND(
        (total_downloads - LAG(total_downloads) OVER (ORDER BY date)) * 100.0 / 
        LAG(total_downloads) OVER (ORDER BY date), 1
    ) as growth_rate_percent
FROM fact_daily_kpis
WHERE total_uploads > 0
ORDER BY date DESC
LIMIT 7;

-- ====================================================================
-- PRODUCT ANALYTICS QUERIES  
-- ====================================================================

-- 3. Tool Performance Analysis
SELECT 
    t.tool_display_name,
    t.tool_category,
    SUM(k.total_downloads) as completed_transactions,
    SUM(k.total_uploads) as initiated_sessions,
    ROUND(AVG(k.upload_to_download_rate), 1) as avg_conversion_rate,
    SUM(k.unique_sessions) as total_sessions,
    RANK() OVER (ORDER BY SUM(k.total_downloads) DESC) as performance_rank
FROM fact_daily_kpis k
JOIN dim_tools t ON k.tool_key = t.tool_key
GROUP BY t.tool_display_name, t.tool_category, t.sort_order
HAVING SUM(k.total_downloads) > 0
ORDER BY completed_transactions DESC;

-- 4. Category Performance Comparison
SELECT 
    t.tool_category,
    COUNT(DISTINCT t.tool_key) as tools_in_category,
    SUM(k.total_downloads) as category_transactions,
    ROUND(AVG(k.upload_to_download_rate), 1) as avg_category_conversion,
    SUM(k.unique_sessions) as category_sessions,
    ROUND(
        SUM(k.total_downloads) * 100.0 / 
        SUM(SUM(k.total_downloads)) OVER (), 1
    ) as market_share_percent
FROM fact_daily_kpis k
JOIN dim_tools t ON k.tool_key = t.tool_key
GROUP BY t.tool_category
ORDER BY category_transactions DESC;

-- ====================================================================
-- USER BEHAVIOR ANALYTICS
-- ====================================================================

-- 5. Peak Usage Analysis (Hourly Patterns)
SELECT 
    tm.hour,
    tm.day_name,
    COUNT(*) as total_events,
    COUNT(DISTINCT f.session_key) as unique_sessions,
    SUM(CASE WHEN f.download_flag THEN 1 ELSE 0 END) as successful_completions,
    ROUND(
        SUM(CASE WHEN f.download_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
    ) as completion_rate
FROM fact_analytics f
JOIN dim_time tm ON f.time_key = tm.time_key
GROUP BY tm.hour, tm.day_name, tm.day_of_week
ORDER BY tm.day_of_week, tm.hour;

-- 6. Session Journey Analysis
SELECT 
    s.device_type,
    s.browser,
    COUNT(DISTINCT f.session_key) as sessions,
    AVG(
        CASE WHEN f.download_flag THEN 1 ELSE 0 END
    ) as avg_completion_rate,
    COUNT(f.analytics_key) as total_events,
    ROUND(COUNT(f.analytics_key) * 1.0 / COUNT(DISTINCT f.session_key), 1) as avg_events_per_session
FROM fact_analytics f
JOIN dim_sessions s ON f.session_key = s.session_key
WHERE s.browser != 'Unknown'
GROUP BY s.device_type, s.browser
HAVING COUNT(DISTINCT f.session_key) >= 3
ORDER BY sessions DESC;

-- ====================================================================
-- CONVERSION FUNNEL ANALYSIS
-- ====================================================================

-- 7. Complete Conversion Funnel
WITH funnel_stages AS (
    SELECT 
        'Stage 1: Page Views' as stage,
        1 as stage_order,
        COUNT(*) as users,
        100.0 as conversion_rate
    FROM fact_analytics f
    JOIN dim_event_types et ON f.event_type_key = et.event_type_key
    WHERE et.event_type = 'page_view'
    
    UNION ALL
    
    SELECT 
        'Stage 2: File Uploads' as stage,
        2 as stage_order,
        COUNT(*) as users,
        COUNT(*) * 100.0 / (
            SELECT COUNT(*) FROM fact_analytics f2
            JOIN dim_event_types et2 ON f2.event_type_key = et2.event_type_key
            WHERE et2.event_type = 'page_view'
        ) as conversion_rate
    FROM fact_analytics f
    WHERE f.upload_flag = true
    
    UNION ALL
    
    SELECT 
        'Stage 3: Processing Started' as stage,
        3 as stage_order,
        COUNT(*) as users,
        COUNT(*) * 100.0 / (
            SELECT COUNT(*) FROM fact_analytics f2
            JOIN dim_event_types et2 ON f2.event_type_key = et2.event_type_key
            WHERE et2.event_type = 'page_view'
        ) as conversion_rate
    FROM fact_analytics f
    WHERE f.processing_flag = true
    
    UNION ALL
    
    SELECT 
        'Stage 4: Downloads Completed' as stage,
        4 as stage_order,
        COUNT(*) as users,
        COUNT(*) * 100.0 / (
            SELECT COUNT(*) FROM fact_analytics f2
            JOIN dim_event_types et2 ON f2.event_type_key = et2.event_type_key
            WHERE et2.event_type = 'page_view'
        ) as conversion_rate
    FROM fact_analytics f
    WHERE f.download_flag = true
)
SELECT 
    stage,
    users,
    ROUND(conversion_rate, 1) as conversion_rate_percent,
    LAG(users) OVER (ORDER BY stage_order) - users as drop_off_count,
    ROUND(
        (LAG(users) OVER (ORDER BY stage_order) - users) * 100.0 / 
        LAG(users) OVER (ORDER BY stage_order), 1
    ) as drop_off_rate_percent
FROM funnel_stages
ORDER BY stage_order;

-- ====================================================================
-- ADVANCED ANALYTICS
-- ====================================================================

-- 8. Cohort Analysis (Tool Adoption)
SELECT 
    t.tool_category,
    tm.date,
    COUNT(DISTINCT f.session_key) as new_users,
    SUM(COUNT(DISTINCT f.session_key)) OVER (
        PARTITION BY t.tool_category 
        ORDER BY tm.date 
        ROWS UNBOUNDED PRECEDING
    ) as cumulative_users
FROM fact_analytics f
JOIN dim_tools t ON f.tool_key = t.tool_key
JOIN dim_time tm ON f.time_key = tm.time_key
GROUP BY t.tool_category, tm.date
ORDER BY t.tool_category, tm.date;

-- 9. Performance Anomaly Detection
SELECT 
    date,
    tool_key,
    total_downloads,
    AVG(total_downloads) OVER (
        PARTITION BY tool_key 
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
    ) as rolling_avg_downloads,
    CASE 
        WHEN total_downloads > 2 * AVG(total_downloads) OVER (
            PARTITION BY tool_key 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
        ) THEN 'High Performance'
        WHEN total_downloads < 0.5 * AVG(total_downloads) OVER (
            PARTITION BY tool_key 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
        ) THEN 'Low Performance'
        ELSE 'Normal'
    END as performance_flag
FROM fact_daily_kpis
WHERE total_downloads > 0
ORDER BY date DESC, total_downloads DESC;

-- ====================================================================
-- DATA QUALITY VERIFICATION
-- ====================================================================

-- 10. Data Quality Health Check
SELECT 
    'fact_analytics' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT session_key) as unique_sessions,
    MIN(created_at) as earliest_record,
    MAX(created_at) as latest_record,
    SUM(CASE WHEN tool_key IS NULL THEN 1 ELSE 0 END) as null_tool_keys
FROM fact_analytics

UNION ALL

SELECT 
    'fact_daily_kpis' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT tool_key) as unique_tools,
    MIN(date) as earliest_date,
    MAX(date) as latest_date,
    SUM(CASE WHEN total_downloads > total_uploads THEN 1 ELSE 0 END) as data_anomalies
FROM fact_daily_kpis;