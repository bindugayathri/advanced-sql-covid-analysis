# Advanced SQL COVID-19 Data Analysis Project

## Project Overview
This project demonstrates advanced SQL skills through comprehensive analysis of COVID-19 data, including deaths and vaccinations across countries worldwide. The analysis showcases complex SQL techniques and provides valuable insights into the pandemic's impact.

## Skills Demonstrated
- **Advanced Joins**: Multiple table joins with complex conditions
- **Window Functions**: ROW_NUMBER(), RANK(), LAG(), LEAD(), PARTITION BY
- **Common Table Expressions (CTEs)**: Recursive and multiple CTEs
- **Subqueries**: Correlated and non-correlated subqueries
- **Advanced Aggregations**: GROUP BY with multiple columns, HAVING clauses
- **Date Functions**: Date manipulation and time-series analysis
- **Performance Optimization**: Indexing strategies and query optimization
- **Data Quality**: Handling NULL values and data validation

## Dataset Description
- **CovidDeaths.csv**: Contains daily COVID-19 death statistics by country
- **CovidVaccinations.csv**: Contains vaccination data and demographic information

## Key Analysis Areas
1. **Global Impact Analysis**: Death rates, case fatality ratios, and trends
2. **Vaccination Effectiveness**: Correlation between vaccination rates and death rates
3. **Geographic Analysis**: Continental and country-level comparisons
4. **Time Series Analysis**: Trend analysis and peak identification
5. **Demographic Insights**: Age, GDP, and healthcare factors
6. **Performance Metrics**: Rolling averages and moving statistics

## Files Structure
```
├── README.md
├── sql_analysis/
│   ├── 01_data_exploration.sql
│   ├── 02_global_analysis.sql
│   ├── 03_vaccination_analysis.sql
│   ├── 04_geographic_analysis.sql
│   ├── 05_time_series_analysis.sql
│   ├── 06_demographic_analysis.sql
│   ├── 07_performance_optimization.sql
│   └── 08_advanced_insights.sql
├── results/
│   └── key_findings.md
└── CovidDeaths.csv
└── CovidVaccinations.csv
```

## Setup Instructions
1. Import the CSV files into your preferred SQL database (MySQL, PostgreSQL, SQL Server)
2. Execute the SQL scripts in numerical order
3. Review the results and insights generated

## Key Findings
- Countries with highest vaccination rates showed significant reduction in death rates
- Geographic patterns in COVID-19 impact and response effectiveness
- Correlation between healthcare infrastructure and pandemic outcomes
- Time-based trends and seasonal patterns in the data

## Technologies Used
- SQL (ANSI Standard)
- Data Analysis
- Statistical Analysis
- Performance Optimization 