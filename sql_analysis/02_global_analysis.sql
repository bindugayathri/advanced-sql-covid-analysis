-- =====================================================
-- COVID-19 Data Analysis Project
-- File: 02_global_analysis.sql
-- Description: Global impact analysis with advanced SQL techniques
-- Skills: Window functions, complex aggregations, subqueries, CTEs
-- =====================================================

-- 1. GLOBAL TREND ANALYSIS WITH WINDOW FUNCTIONS
-- =====================================================

-- Daily global cases and deaths with 7-day moving averages
WITH global_daily_stats AS (
    SELECT 
        date,
        SUM(new_cases) as global_new_cases,
        SUM(new_deaths) as global_new_deaths,
        SUM(total_cases) as global_total_cases,
        SUM(total_deaths) as global_total_deaths,
        SUM(population) as global_population
    FROM CovidDeaths
    WHERE continent IS NOT NULL
    GROUP BY date
),
global_trends AS (
    SELECT 
        date,
        global_new_cases,
        global_new_deaths,
        global_total_cases,
        global_total_deaths,
        global_population,
        -- 7-day moving averages
        AVG(global_new_cases) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as cases_7day_avg,
        AVG(global_new_deaths) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as deaths_7day_avg,
        -- Cumulative rates
        ROUND(global_total_cases * 100.0 / global_population, 2) as infection_rate_percent,
        ROUND(global_total_deaths * 100.0 / global_total_cases, 2) as case_fatality_rate_percent,
        -- Growth rates (day-over-day)
        LAG(global_total_cases, 1) OVER (ORDER BY date) as prev_total_cases,
        LAG(global_total_deaths, 1) OVER (ORDER BY date) as prev_total_deaths
    FROM global_daily_stats
    WHERE global_new_cases IS NOT NULL
)
SELECT 
    date,
    global_new_cases,
    ROUND(cases_7day_avg, 0) as cases_7day_avg,
    global_new_deaths,
    ROUND(deaths_7day_avg, 0) as deaths_7day_avg,
    infection_rate_percent,
    case_fatality_rate_percent,
    CASE 
        WHEN prev_total_cases > 0 THEN ROUND(((global_total_cases - prev_total_cases) * 100.0 / prev_total_cases), 2)
        ELSE NULL 
    END as daily_growth_rate_percent
FROM global_trends
WHERE date >= '2020-03-01'  -- Start from significant COVID period
ORDER BY date DESC
LIMIT 30;

-- 2. COUNTRY RANKING ANALYSIS
-- =====================================================

-- Top countries by various metrics with rankings
WITH country_rankings AS (
    SELECT 
        location,
        continent,
        population,
        total_cases,
        total_deaths,
        ROUND(total_cases * 100.0 / population, 2) as infection_rate_percent,
        ROUND(total_deaths * 100.0 / population, 2) as death_rate_percent,
        ROUND(total_deaths * 100.0 / total_cases, 2) as case_fatality_rate_percent,
        -- Rankings using window functions
        ROW_NUMBER() OVER (ORDER BY total_cases DESC) as rank_by_total_cases,
        ROW_NUMBER() OVER (ORDER BY total_deaths DESC) as rank_by_total_deaths,
        ROW_NUMBER() OVER (ORDER BY (total_cases * 100.0 / population) DESC) as rank_by_infection_rate,
        ROW_NUMBER() OVER (ORDER BY (total_deaths * 100.0 / population) DESC) as rank_by_death_rate,
        ROW_NUMBER() OVER (ORDER BY (total_deaths * 100.0 / total_cases) DESC) as rank_by_fatality_rate
    FROM CovidDeaths
    WHERE continent IS NOT NULL
    AND date = (SELECT MAX(date) FROM CovidDeaths WHERE continent IS NOT NULL)
    AND total_cases > 0
    AND total_deaths > 0
)
SELECT 
    location,
    continent,
    population,
    total_cases,
    total_deaths,
    infection_rate_percent,
    death_rate_percent,
    case_fatality_rate_percent,
    rank_by_total_cases,
    rank_by_total_deaths,
    rank_by_infection_rate,
    rank_by_death_rate,
    rank_by_fatality_rate
FROM country_rankings
WHERE rank_by_total_cases <= 20 OR rank_by_total_deaths <= 20
ORDER BY rank_by_total_cases;

-- 3. CONTINENTAL COMPARISON WITH PERCENTILES
-- =====================================================

-- Continental analysis with percentile rankings
WITH continental_stats AS (
    SELECT 
        continent,
        COUNT(DISTINCT location) as countries_count,
        ROUND(AVG(total_cases * 100.0 / population), 2) as avg_infection_rate,
        ROUND(AVG(total_deaths * 100.0 / population), 2) as avg_death_rate,
        ROUND(AVG(total_deaths * 100.0 / total_cases), 2) as avg_fatality_rate,
        ROUND(SUM(total_cases), 0) as total_cases_sum,
        ROUND(SUM(total_deaths), 0) as total_deaths_sum,
        ROUND(SUM(population), 0) as total_population
    FROM CovidDeaths
    WHERE continent IS NOT NULL
    AND date = (SELECT MAX(date) FROM CovidDeaths WHERE continent IS NOT NULL)
    AND total_cases > 0
    GROUP BY continent
),
continental_percentiles AS (
    SELECT 
        *,
        -- Percentile rankings within continents
        PERCENT_RANK() OVER (ORDER BY avg_infection_rate) as infection_rate_percentile,
        PERCENT_RANK() OVER (ORDER BY avg_death_rate) as death_rate_percentile,
        PERCENT_RANK() OVER (ORDER BY avg_fatality_rate) as fatality_rate_percentile
    FROM continental_stats
)
SELECT 
    continent,
    countries_count,
    avg_infection_rate,
    avg_death_rate,
    avg_fatality_rate,
    total_cases_sum,
    total_deaths_sum,
    total_population,
    ROUND(infection_rate_percentile * 100, 1) as infection_rate_percentile,
    ROUND(death_rate_percentile * 100, 1) as death_rate_percentile,
    ROUND(fatality_rate_percentile * 100, 1) as fatality_rate_percentile
FROM continental_percentiles
ORDER BY total_deaths_sum DESC;

-- 4. PEAK ANALYSIS WITH WINDOW FUNCTIONS
-- =====================================================

-- Identify peak days for each country
WITH daily_peaks AS (
    SELECT 
        location,
        date,
        new_cases,
        new_deaths,
        -- Find peaks using window functions
        CASE 
            WHEN new_cases = MAX(new_cases) OVER (PARTITION BY location) THEN 'Peak Cases'
            ELSE 'Not Peak'
        END as case_peak_status,
        CASE 
            WHEN new_deaths = MAX(new_deaths) OVER (PARTITION BY location) THEN 'Peak Deaths'
            ELSE 'Not Peak'
        END as death_peak_status,
        -- Rank peaks by magnitude
        RANK() OVER (PARTITION BY location ORDER BY new_cases DESC) as case_rank,
        RANK() OVER (PARTITION BY location ORDER BY new_deaths DESC) as death_rank
    FROM CovidDeaths
    WHERE continent IS NOT NULL
    AND new_cases IS NOT NULL
    AND new_deaths IS NOT NULL
    AND new_cases > 0
    AND new_deaths > 0
)
SELECT 
    location,
    date,
    new_cases,
    new_deaths,
    case_peak_status,
    death_peak_status,
    case_rank,
    death_rank
FROM daily_peaks
WHERE case_rank = 1 OR death_rank = 1
ORDER BY new_cases DESC
LIMIT 20;

-- 5. GROWTH PHASE ANALYSIS
-- =====================================================

-- Analyze growth phases using cumulative sums and thresholds
WITH growth_phases AS (
    SELECT 
        location,
        date,
        total_cases,
        total_deaths,
        -- Define growth phases based on cumulative cases
        CASE 
            WHEN total_cases < 100 THEN 'Initial Phase (<100 cases)'
            WHEN total_cases < 1000 THEN 'Early Growth (100-1K cases)'
            WHEN total_cases < 10000 THEN 'Rapid Growth (1K-10K cases)'
            WHEN total_cases < 100000 THEN 'Major Outbreak (10K-100K cases)'
            WHEN total_cases < 1000000 THEN 'Large Scale (100K-1M cases)'
            ELSE 'Massive Outbreak (>1M cases)'
        END as growth_phase,
        -- Days since first case
        DATEDIFF(date, MIN(date) OVER (PARTITION BY location)) as days_since_first_case
    FROM CovidDeaths
    WHERE continent IS NOT NULL
    AND total_cases > 0
),
phase_analysis AS (
    SELECT 
        location,
        growth_phase,
        COUNT(*) as days_in_phase,
        MIN(date) as phase_start_date,
        MAX(date) as phase_end_date,
        MAX(total_cases) as max_cases_in_phase,
        MAX(total_deaths) as max_deaths_in_phase
    FROM growth_phases
    GROUP BY location, growth_phase
)
SELECT 
    growth_phase,
    COUNT(DISTINCT location) as countries_count,
    ROUND(AVG(days_in_phase), 1) as avg_days_in_phase,
    ROUND(AVG(max_cases_in_phase), 0) as avg_max_cases,
    ROUND(AVG(max_deaths_in_phase), 0) as avg_max_deaths
FROM phase_analysis
GROUP BY growth_phase
ORDER BY 
    CASE growth_phase
        WHEN 'Initial Phase (<100 cases)' THEN 1
        WHEN 'Early Growth (100-1K cases)' THEN 2
        WHEN 'Rapid Growth (1K-10K cases)' THEN 3
        WHEN 'Major Outbreak (10K-100K cases)' THEN 4
        WHEN 'Large Scale (100K-1M cases)' THEN 5
        WHEN 'Massive Outbreak (>1M cases)' THEN 6
    END;

-- 6. CORRELATION ANALYSIS BETWEEN CASES AND DEATHS
-- =====================================================

-- Analyze correlation between daily new cases and deaths with lag
WITH correlation_data AS (
    SELECT 
        location,
        date,
        new_cases,
        new_deaths,
        -- Lag deaths by 14 days (typical time between infection and death)
        LAG(new_deaths, 14) OVER (PARTITION BY location ORDER BY date) as deaths_14_days_later,
        -- 7-day moving averages
        AVG(new_cases) OVER (PARTITION BY location ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as cases_7day_avg,
        AVG(new_deaths) OVER (PARTITION BY location ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as deaths_7day_avg
    FROM CovidDeaths
    WHERE continent IS NOT NULL
    AND new_cases IS NOT NULL
    AND new_deaths IS NOT NULL
    AND new_cases > 0
    AND new_deaths > 0
)
SELECT 
    location,
    COUNT(*) as data_points,
    ROUND(AVG(new_cases), 0) as avg_daily_cases,
    ROUND(AVG(new_deaths), 0) as avg_daily_deaths,
    ROUND(AVG(deaths_14_days_later), 0) as avg_deaths_14_days_later,
    ROUND(AVG(cases_7day_avg), 0) as avg_7day_cases,
    ROUND(AVG(deaths_7day_avg), 0) as avg_7day_deaths
FROM correlation_data
WHERE deaths_14_days_later IS NOT NULL
GROUP BY location
HAVING COUNT(*) > 30  -- Only countries with sufficient data
ORDER BY avg_daily_cases DESC
LIMIT 20; 