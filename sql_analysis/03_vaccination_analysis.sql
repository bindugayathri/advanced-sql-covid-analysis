-- =====================================================
-- COVID-19 Data Analysis Project
-- File: 03_vaccination_analysis.sql
-- Description: Vaccination effectiveness analysis with complex joins
-- Skills: Complex joins, correlation analysis, time-series comparison
-- =====================================================

-- 1. VACCINATION COVERAGE ANALYSIS
-- =====================================================

-- Global vaccination progress with detailed metrics
WITH vaccination_coverage AS (
    SELECT 
        v.location,
        v.continent,
        v.date,
        v.population,
        v.total_vaccinations,
        v.people_vaccinated,
        v.people_fully_vaccinated,
        v.total_boosters,
        v.new_vaccinations,
        -- Calculate vaccination rates
        ROUND(v.total_vaccinations * 100.0 / v.population, 2) as total_vaccination_rate,
        ROUND(v.people_vaccinated * 100.0 / v.population, 2) as people_vaccinated_rate,
        ROUND(v.people_fully_vaccinated * 100.0 / v.population, 2) as fully_vaccinated_rate,
        ROUND(v.total_boosters * 100.0 / v.population, 2) as booster_rate
    FROM CovidVaccinations v
    WHERE v.continent IS NOT NULL
    AND v.total_vaccinations IS NOT NULL
),
latest_vaccination AS (
    SELECT 
        location,
        continent,
        population,
        total_vaccinations,
        people_vaccinated,
        people_fully_vaccinated,
        total_boosters,
        total_vaccination_rate,
        people_vaccinated_rate,
        fully_vaccinated_rate,
        booster_rate,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY date DESC) as rn
    FROM vaccination_coverage
)
SELECT 
    location,
    continent,
    population,
    total_vaccinations,
    people_vaccinated,
    people_fully_vaccinated,
    total_boosters,
    total_vaccination_rate,
    people_vaccinated_rate,
    fully_vaccinated_rate,
    booster_rate
FROM latest_vaccination
WHERE rn = 1
ORDER BY fully_vaccinated_rate DESC
LIMIT 30;

-- 2. VACCINATION EFFECTIVENESS ANALYSIS
-- =====================================================

-- Compare death rates before and after vaccination campaigns
WITH vaccination_effectiveness AS (
    SELECT 
        v.location,
        v.continent,
        v.date,
        v.people_fully_vaccinated,
        v.people_fully_vaccinated_per_hundred,
        d.new_cases,
        d.new_deaths,
        d.total_cases,
        d.total_deaths,
        d.population,
        -- Define vaccination phases
        CASE 
            WHEN v.people_fully_vaccinated_per_hundred < 10 THEN 'Pre-Vaccination (<10%)'
            WHEN v.people_fully_vaccinated_per_hundred < 30 THEN 'Early Vaccination (10-30%)'
            WHEN v.people_fully_vaccinated_per_hundred < 50 THEN 'Mid Vaccination (30-50%)'
            WHEN v.people_fully_vaccinated_per_hundred < 70 THEN 'High Vaccination (50-70%)'
            ELSE 'Full Vaccination (>70%)'
        END as vaccination_phase,
        -- 7-day moving averages for smoothing
        AVG(d.new_cases) OVER (PARTITION BY v.location ORDER BY v.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as cases_7day_avg,
        AVG(d.new_deaths) OVER (PARTITION BY v.location ORDER BY v.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as deaths_7day_avg
    FROM CovidVaccinations v
    INNER JOIN CovidDeaths d ON v.location = d.location AND v.date = d.date
    WHERE v.continent IS NOT NULL
    AND d.continent IS NOT NULL
    AND v.people_fully_vaccinated_per_hundred IS NOT NULL
    AND d.new_cases IS NOT NULL
    AND d.new_deaths IS NOT NULL
),
phase_comparison AS (
    SELECT 
        location,
        vaccination_phase,
        COUNT(*) as days_in_phase,
        ROUND(AVG(cases_7day_avg), 0) as avg_daily_cases,
        ROUND(AVG(deaths_7day_avg), 0) as avg_daily_deaths,
        ROUND(AVG(people_fully_vaccinated_per_hundred), 1) as avg_vaccination_rate,
        ROUND(SUM(new_cases), 0) as total_cases_in_phase,
        ROUND(SUM(new_deaths), 0) as total_deaths_in_phase
    FROM vaccination_effectiveness
    GROUP BY location, vaccination_phase
    HAVING COUNT(*) > 7  -- Only phases with at least a week of data
)
SELECT 
    vaccination_phase,
    COUNT(DISTINCT location) as countries_count,
    ROUND(AVG(avg_daily_cases), 0) as avg_daily_cases,
    ROUND(AVG(avg_daily_deaths), 0) as avg_daily_deaths,
    ROUND(AVG(avg_vaccination_rate), 1) as avg_vaccination_rate,
    ROUND(SUM(total_cases_in_phase), 0) as total_cases,
    ROUND(SUM(total_deaths_in_phase), 0) as total_deaths
FROM phase_comparison
GROUP BY vaccination_phase
ORDER BY 
    CASE vaccination_phase
        WHEN 'Pre-Vaccination (<10%)' THEN 1
        WHEN 'Early Vaccination (10-30%)' THEN 2
        WHEN 'Mid Vaccination (30-50%)' THEN 3
        WHEN 'High Vaccination (50-70%)' THEN 4
        WHEN 'Full Vaccination (>70%)' THEN 5
    END;

-- 3. VACCINATION ROLLOUT TIMELINE ANALYSIS
-- =====================================================

-- Analyze vaccination rollout speed and effectiveness
WITH vaccination_timeline AS (
    SELECT 
        location,
        continent,
        date,
        people_fully_vaccinated_per_hundred,
        -- Find key vaccination milestones
        CASE 
            WHEN people_fully_vaccinated_per_hundred >= 10 AND 
                 LAG(people_fully_vaccinated_per_hundred, 1) OVER (PARTITION BY location ORDER BY date) < 10 
            THEN '10% Milestone'
            WHEN people_fully_vaccinated_per_hundred >= 30 AND 
                 LAG(people_fully_vaccinated_per_hundred, 1) OVER (PARTITION BY location ORDER BY date) < 30 
            THEN '30% Milestone'
            WHEN people_fully_vaccinated_per_hundred >= 50 AND 
                 LAG(people_fully_vaccinated_per_hundred, 1) OVER (PARTITION BY location ORDER BY date) < 50 
            THEN '50% Milestone'
            WHEN people_fully_vaccinated_per_hundred >= 70 AND 
                 LAG(people_fully_vaccinated_per_hundred, 1) OVER (PARTITION BY location ORDER BY date) < 70 
            THEN '70% Milestone'
            ELSE NULL
        END as milestone_reached
    FROM CovidVaccinations
    WHERE continent IS NOT NULL
    AND people_fully_vaccinated_per_hundred IS NOT NULL
),
milestone_analysis AS (
    SELECT 
        location,
        continent,
        milestone_reached,
        date as milestone_date,
        people_fully_vaccinated_per_hundred,
        ROW_NUMBER() OVER (PARTITION BY location, milestone_reached ORDER BY date) as milestone_rank
    FROM vaccination_timeline
    WHERE milestone_reached IS NOT NULL
),
rollout_speed AS (
    SELECT 
        location,
        continent,
        MAX(CASE WHEN milestone_reached = '10% Milestone' THEN milestone_date END) as reached_10_percent,
        MAX(CASE WHEN milestone_reached = '30% Milestone' THEN milestone_date END) as reached_30_percent,
        MAX(CASE WHEN milestone_reached = '50% Milestone' THEN milestone_date END) as reached_50_percent,
        MAX(CASE WHEN milestone_reached = '70% Milestone' THEN milestone_date END) as reached_70_percent
    FROM milestone_analysis
    GROUP BY location, continent
)
SELECT 
    location,
    continent,
    reached_10_percent,
    reached_30_percent,
    reached_50_percent,
    reached_70_percent,
    DATEDIFF(reached_30_percent, reached_10_percent) as days_10_to_30_percent,
    DATEDIFF(reached_50_percent, reached_30_percent) as days_30_to_50_percent,
    DATEDIFF(reached_70_percent, reached_50_percent) as days_50_to_70_percent
FROM rollout_speed
WHERE reached_10_percent IS NOT NULL
AND reached_30_percent IS NOT NULL
ORDER BY reached_10_percent
LIMIT 30;

-- 4. VACCINATION VS DEATH RATE CORRELATION
-- =====================================================

-- Analyze correlation between vaccination rates and death rates
WITH vaccination_death_correlation AS (
    SELECT 
        v.location,
        v.continent,
        v.date,
        v.people_fully_vaccinated_per_hundred,
        d.new_deaths_per_million,
        d.total_deaths_per_million,
        -- Calculate vaccination effectiveness metrics
        CASE 
            WHEN v.people_fully_vaccinated_per_hundred > 50 THEN 'High Vaccination (>50%)'
            WHEN v.people_fully_vaccinated_per_hundred > 30 THEN 'Medium Vaccination (30-50%)'
            WHEN v.people_fully_vaccinated_per_hundred > 10 THEN 'Low Vaccination (10-30%)'
            ELSE 'Minimal Vaccination (<10%)'
        END as vaccination_level,
        -- 14-day moving averages for correlation analysis
        AVG(d.new_deaths_per_million) OVER (PARTITION BY v.location ORDER BY v.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as deaths_14day_avg
    FROM CovidVaccinations v
    INNER JOIN CovidDeaths d ON v.location = d.location AND v.date = d.date
    WHERE v.continent IS NOT NULL
    AND d.continent IS NOT NULL
    AND v.people_fully_vaccinated_per_hundred IS NOT NULL
    AND d.new_deaths_per_million IS NOT NULL
    AND v.people_fully_vaccinated_per_hundred > 0
),
correlation_summary AS (
    SELECT 
        location,
        vaccination_level,
        COUNT(*) as data_points,
        ROUND(AVG(people_fully_vaccinated_per_hundred), 1) as avg_vaccination_rate,
        ROUND(AVG(deaths_14day_avg), 1) as avg_death_rate,
        ROUND(MAX(people_fully_vaccinated_per_hundred), 1) as max_vaccination_rate,
        ROUND(MIN(deaths_14day_avg), 1) as min_death_rate,
        ROUND(MAX(deaths_14day_avg), 1) as max_death_rate
    FROM vaccination_death_correlation
    GROUP BY location, vaccination_level
    HAVING COUNT(*) > 14  -- At least 2 weeks of data
)
SELECT 
    vaccination_level,
    COUNT(DISTINCT location) as countries_count,
    ROUND(AVG(avg_vaccination_rate), 1) as avg_vaccination_rate,
    ROUND(AVG(avg_death_rate), 1) as avg_death_rate,
    ROUND(AVG(max_vaccination_rate), 1) as avg_max_vaccination,
    ROUND(AVG(min_death_rate), 1) as avg_min_death_rate,
    ROUND(AVG(max_death_rate), 1) as avg_max_death_rate
FROM correlation_summary
GROUP BY vaccination_level
ORDER BY 
    CASE vaccination_level
        WHEN 'Minimal Vaccination (<10%)' THEN 1
        WHEN 'Low Vaccination (10-30%)' THEN 2
        WHEN 'Medium Vaccination (30-50%)' THEN 3
        WHEN 'High Vaccination (>50%)' THEN 4
    END;

-- 5. BOOSTER DOSE ANALYSIS
-- =====================================================

-- Analyze booster dose effectiveness and uptake
WITH booster_analysis AS (
    SELECT 
        v.location,
        v.continent,
        v.date,
        v.total_boosters,
        v.total_boosters_per_hundred,
        v.people_fully_vaccinated,
        v.people_fully_vaccinated_per_hundred,
        d.new_cases,
        d.new_deaths,
        -- Calculate booster effectiveness
        CASE 
            WHEN v.total_boosters_per_hundred > 30 THEN 'High Booster Coverage (>30%)'
            WHEN v.total_boosters_per_hundred > 15 THEN 'Medium Booster Coverage (15-30%)'
            WHEN v.total_boosters_per_hundred > 5 THEN 'Low Booster Coverage (5-15%)'
            ELSE 'Minimal Booster Coverage (<5%)'
        END as booster_level,
        -- Booster uptake rate
        ROUND(v.total_boosters * 100.0 / v.people_fully_vaccinated, 2) as booster_uptake_rate
    FROM CovidVaccinations v
    INNER JOIN CovidDeaths d ON v.location = d.location AND v.date = d.date
    WHERE v.continent IS NOT NULL
    AND d.continent IS NOT NULL
    AND v.total_boosters IS NOT NULL
    AND v.people_fully_vaccinated IS NOT NULL
    AND v.people_fully_vaccinated > 0
),
booster_effectiveness AS (
    SELECT 
        location,
        continent,
        booster_level,
        COUNT(*) as data_points,
        ROUND(AVG(total_boosters_per_hundred), 1) as avg_booster_rate,
        ROUND(AVG(booster_uptake_rate), 1) as avg_booster_uptake,
        ROUND(AVG(new_cases), 0) as avg_daily_cases,
        ROUND(AVG(new_deaths), 0) as avg_daily_deaths,
        ROUND(MAX(total_boosters_per_hundred), 1) as max_booster_rate
    FROM booster_analysis
    GROUP BY location, continent, booster_level
    HAVING COUNT(*) > 7
)
SELECT 
    booster_level,
    COUNT(DISTINCT location) as countries_count,
    ROUND(AVG(avg_booster_rate), 1) as avg_booster_rate,
    ROUND(AVG(avg_booster_uptake), 1) as avg_booster_uptake,
    ROUND(AVG(avg_daily_cases), 0) as avg_daily_cases,
    ROUND(AVG(avg_daily_deaths), 0) as avg_daily_deaths
FROM booster_effectiveness
GROUP BY booster_level
ORDER BY 
    CASE booster_level
        WHEN 'Minimal Booster Coverage (<5%)' THEN 1
        WHEN 'Low Booster Coverage (5-15%)' THEN 2
        WHEN 'Medium Booster Coverage (15-30%)' THEN 3
        WHEN 'High Booster Coverage (>30%)' THEN 4
    END;

-- 6. VACCINATION INEQUALITY ANALYSIS
-- =====================================================

-- Analyze vaccination inequality across continents and countries
WITH vaccination_inequality AS (
    SELECT 
        v.location,
        v.continent,
        v.people_fully_vaccinated_per_hundred,
        v.gdp_per_capita,
        v.human_development_index,
        -- Categorize countries by development level
        CASE 
            WHEN v.human_development_index >= 0.8 THEN 'Very High Development'
            WHEN v.human_development_index >= 0.7 THEN 'High Development'
            WHEN v.human_development_index >= 0.55 THEN 'Medium Development'
            ELSE 'Low Development'
        END as development_level
    FROM CovidVaccinations v
    WHERE v.continent IS NOT NULL
    AND v.people_fully_vaccinated_per_hundred IS NOT NULL
    AND v.human_development_index IS NOT NULL
    AND v.date = (
        SELECT MAX(date) 
        FROM CovidVaccinations v2 
        WHERE v2.location = v.location 
        AND v2.people_fully_vaccinated_per_hundred IS NOT NULL
    )
)
SELECT 
    continent,
    development_level,
    COUNT(*) as countries_count,
    ROUND(AVG(people_fully_vaccinated_per_hundred), 1) as avg_vaccination_rate,
    ROUND(MIN(people_fully_vaccinated_per_hundred), 1) as min_vaccination_rate,
    ROUND(MAX(people_fully_vaccinated_per_hundred), 1) as max_vaccination_rate,
    ROUND(STDDEV(people_fully_vaccinated_per_hundred), 1) as vaccination_std_dev,
    ROUND(AVG(gdp_per_capita), 0) as avg_gdp_per_capita,
    ROUND(AVG(human_development_index), 3) as avg_hdi
FROM vaccination_inequality
GROUP BY continent, development_level
ORDER BY continent, 
    CASE development_level
        WHEN 'Very High Development' THEN 1
        WHEN 'High Development' THEN 2
        WHEN 'Medium Development' THEN 3
        WHEN 'Low Development' THEN 4
    END; 