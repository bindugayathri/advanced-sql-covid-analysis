-- =====================================================
-- COVID-19 Data Analysis Project
-- File: 01_data_exploration.sql
-- Description: Comprehensive data exploration and understanding
-- Skills: Data profiling, NULL handling, data quality assessment
-- =====================================================

-- 1. DATASET OVERVIEW AND DATA PROFILING
-- =====================================================

-- Check total records in each dataset
SELECT 
    'CovidDeaths' as dataset_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT location) as unique_countries,
    COUNT(DISTINCT date) as unique_dates,
    MIN(date) as earliest_date,
    MAX(date) as latest_date
FROM CovidDeaths
WHERE continent IS NOT NULL

UNION ALL

SELECT 
    'CovidVaccinations' as dataset_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT location) as unique_countries,
    COUNT(DISTINCT date) as unique_dates,
    MIN(date) as earliest_date,
    MAX(date) as latest_date
FROM CovidVaccinations
WHERE continent IS NOT NULL;

-- 2. DATA QUALITY ASSESSMENT
-- =====================================================

-- Check for missing values in key columns
WITH missing_data_analysis AS (
    SELECT 
        'total_cases' as column_name,
        COUNT(*) as total_rows,
        COUNT(total_cases) as non_null_count,
        ROUND((COUNT(total_cases) * 100.0 / COUNT(*)), 2) as completeness_percentage
    FROM CovidDeaths
    WHERE continent IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'total_deaths' as column_name,
        COUNT(*) as total_rows,
        COUNT(total_deaths) as non_null_count,
        ROUND((COUNT(total_deaths) * 100.0 / COUNT(*)), 2) as completeness_percentage
    FROM CovidDeaths
    WHERE continent IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'total_vaccinations' as column_name,
        COUNT(*) as total_rows,
        COUNT(total_vaccinations) as non_null_count,
        ROUND((COUNT(total_vaccinations) * 100.0 / COUNT(*)), 2) as completeness_percentage
    FROM CovidVaccinations
    WHERE continent IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'people_fully_vaccinated' as column_name,
        COUNT(*) as total_rows,
        COUNT(people_fully_vaccinated) as non_null_count,
        ROUND((COUNT(people_fully_vaccinated) * 100.0 / COUNT(*)), 2) as completeness_percentage
    FROM CovidVaccinations
    WHERE continent IS NOT NULL
)
SELECT * FROM missing_data_analysis
ORDER BY completeness_percentage DESC;

-- 3. CONTINENT AND LOCATION ANALYSIS
-- =====================================================

-- Analyze data distribution by continent
SELECT 
    continent,
    COUNT(DISTINCT location) as countries_count,
    COUNT(*) as total_records,
    ROUND(AVG(population), 0) as avg_population,
    ROUND(SUM(total_cases), 0) as total_cases_sum,
    ROUND(SUM(total_deaths), 0) as total_deaths_sum
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY total_deaths_sum DESC;

-- 4. POPULATION ANALYSIS
-- =====================================================

-- Population statistics by continent
SELECT 
    continent,
    COUNT(DISTINCT location) as countries,
    ROUND(MIN(population), 0) as min_population,
    ROUND(MAX(population), 0) as max_population,
    ROUND(AVG(population), 0) as avg_population,
    ROUND(STDDEV(population), 0) as population_std_dev
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY avg_population DESC;

-- 5. DATE RANGE ANALYSIS
-- =====================================================

-- Analyze the time span of data
SELECT 
    location,
    MIN(date) as first_record_date,
    MAX(date) as last_record_date,
    DATEDIFF(MAX(date), MIN(date)) as days_span,
    COUNT(*) as total_days_with_data
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location
HAVING COUNT(*) > 100  -- Only countries with substantial data
ORDER BY days_span DESC
LIMIT 20;

-- 6. OUTLIER DETECTION
-- =====================================================

-- Detect potential outliers in daily new cases
WITH case_outliers AS (
    SELECT 
        location,
        date,
        new_cases,
        AVG(new_cases) OVER (PARTITION BY location ORDER BY date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) as avg_7day_cases,
        STDDEV(new_cases) OVER (PARTITION BY location ORDER BY date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) as std_7day_cases
    FROM CovidDeaths
    WHERE continent IS NOT NULL 
    AND new_cases IS NOT NULL
    AND new_cases > 0
)
SELECT 
    location,
    date,
    new_cases,
    ROUND(avg_7day_cases, 0) as avg_7day_cases,
    ROUND(std_7day_cases, 0) as std_7day_cases,
    CASE 
        WHEN new_cases > (avg_7day_cases + 2 * std_7day_cases) THEN 'High Outlier'
        WHEN new_cases < (avg_7day_cases - 2 * std_7day_cases) THEN 'Low Outlier'
        ELSE 'Normal'
    END as outlier_status
FROM case_outliers
WHERE new_cases > (avg_7day_cases + 2 * std_7day_cases)
ORDER BY new_cases DESC
LIMIT 20;

-- 7. DATA CONSISTENCY CHECK
-- =====================================================

-- Check for data consistency issues
SELECT 
    location,
    COUNT(*) as total_records,
    COUNT(DISTINCT population) as unique_population_values,
    CASE 
        WHEN COUNT(DISTINCT population) > 1 THEN 'Population varies - potential data issue'
        ELSE 'Population consistent'
    END as population_consistency,
    COUNT(DISTINCT continent) as unique_continent_values,
    CASE 
        WHEN COUNT(DISTINCT continent) > 1 THEN 'Continent varies - potential data issue'
        ELSE 'Continent consistent'
    END as continent_consistency
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location
HAVING COUNT(DISTINCT population) > 1 OR COUNT(DISTINCT continent) > 1
ORDER BY total_records DESC;

-- 8. SUMMARY STATISTICS
-- =====================================================

-- Overall summary statistics
SELECT 
    'Global Summary' as metric,
    COUNT(DISTINCT location) as total_countries,
    ROUND(SUM(population), 0) as total_population,
    ROUND(SUM(total_cases), 0) as total_cases,
    ROUND(SUM(total_deaths), 0) as total_deaths,
    ROUND(SUM(total_cases) * 100.0 / SUM(population), 2) as global_infection_rate_percent,
    ROUND(SUM(total_deaths) * 100.0 / SUM(total_cases), 2) as global_case_fatality_rate_percent
FROM CovidDeaths
WHERE continent IS NOT NULL
AND date = (SELECT MAX(date) FROM CovidDeaths WHERE continent IS NOT NULL); 