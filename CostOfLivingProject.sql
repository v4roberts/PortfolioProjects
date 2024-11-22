--Explore Cost of Living Data from 2020

-- Determine what each column looks like
EXEC sp_help 'cost_of_living';

-- Look at states where the average income is higher than the cost of living for that state

select state, round(avg(median_family_income), 2) as avg_income, round(avg(total_cost), 2) as avg_CostOfLiving 
from cost_of_living
group by state
having AVG(median_family_income) > AVG(total_cost)
order by state asc;

select top 5 state, round(avg(median_family_income), 2) as avg_income, round(avg(total_cost), 2) as avg_COL, round(avg(median_family_income) - avg(total_cost), 2) AS income_surplus
from cost_of_living
group by state
having AVG(median_family_income) > AVG(total_cost)
order by income_surplus desc;

select top 5 county, round(avg(median_family_income), 2) as avg_income, round(avg(total_cost), 2) as avg_COL, round(avg(median_family_income) - avg(total_cost), 2) AS income_surplus
from cost_of_living
group by county
having AVG(median_family_income) > AVG(total_cost)
order by income_surplus desc;

select top 5 areaname, round(avg(median_family_income), 2) as avg_income, round(avg(total_cost), 2) as avg_COL, round(avg(median_family_income) - avg(total_cost), 2) AS income_surplus
from cost_of_living
group by areaname
having AVG(median_family_income) > AVG(total_cost)
order by income_surplus desc;

-- Look at single parents with 3 or more children

select *
from cost_of_living
where [Parent Count] = 1 and [Children Count] >= 3;

select (select count(*) from cost_of_living where [Parent Count] = 1 and [Children Count] >= 3)*100.0/count(*) as PCTofFamilies1pMc
from cost_of_living;

-- Two parent households

select (select count(*) from cost_of_living where [Parent Count] = 2)*100.0/count(*) as PCTofFamilies2P
from cost_of_living;

-- Places with the highest cost of living disparity based on avg median income versus total cost of living

select state, round(avg(median_family_income), 2) as avg_income, round(avg(total_cost), 2) as avg_CostOfLiving 
from cost_of_living
group by state
order by state asc;

select state, Round(avg(median_family_income), 2) as avg_income, round(avg(total_cost), 2) as avg_cost_of_living, (select round(avg(total_cost), 2) - round(avg(median_family_income), 2)) as income_needed
from cost_of_living
group by state
order by state asc;

select top 5 state, Round(avg(median_family_income), 2) as avg_income, round(avg(total_cost), 2) as avg_cost_of_living, (select round(avg(total_cost), 2) - round(avg(median_family_income), 2)) as income_disparity
from cost_of_living
group by state
order by income_needed desc;

select top 5 state, county, Round(avg(median_family_income), 2) as avg_income, round(avg(total_cost), 2) as avg_cost_of_living, (select round(avg(total_cost), 2) - round(avg(median_family_income), 2)) as income_disparity
from cost_of_living
group by state
order by income_needed desc;

-- Exploring childcare costs across the country

select state, round(avg(childcare_cost), 2) as avg_childcare
from cost_of_living
group by state
order by state asc;

select top 5 state, round(avg(childcare_cost), 2) as avg_childcare
from cost_of_living
group by state
order by avg_childcare desc;

select state, ROUND(AVG(childcare_cost) * 100.0 / AVG(total_cost), 2) AS childcare_pct_of_COL
from cost_of_living
group by state
order by state asc;

select state, ROUND(AVG(childcare_cost) * 100.0 / AVG(median_family_income), 2) AS childcare_pct_of_income
from cost_of_living
group by state
order by state asc;

-- Exploring housing data

select state, round(avg(housing_cost), 2) as housing_cost
from cost_of_living
group by state
order by state;

select state, ROUND(AVG(housing_cost) * 100.0 / AVG(total_cost), 2) AS housing_pct_of_COL
from cost_of_living
group by state
order by state asc;

select state, ROUND(AVG(housing_cost) * 100.0 / AVG(median_family_income), 2) AS housing_pct_of_income
from cost_of_living
group by state
order by state asc;

select state, round(avg(housing_cost), 2) as housing_cost
from cost_of_living
where isMetro = 'TRUE'
group by state
order by state;

WITH MetroHousing AS (
    SELECT 
        state, 
        ROUND(AVG(housing_cost), 2) AS metro_housing_cost
    FROM 
        cost_of_living
    WHERE 
        isMetro = 'TRUE'
    GROUP BY 
        state
),

OverallHousing AS (
    SELECT 
        state, 
        ROUND(AVG(housing_cost), 2) AS overall_housing_cost
    FROM 
        cost_of_living
    GROUP BY 
        state
)

SELECT
    o.state, 
    o.overall_housing_cost, 
    m.metro_housing_cost, 
    ROUND(m.metro_housing_cost - o.overall_housing_cost, 2) AS housing_cost_difference
FROM 
    OverallHousing o
LEFT JOIN 
    MetroHousing m ON o.state = m.state
ORDER BY 
    o.state;

WITH MetroHousing AS (
    SELECT 
        state, 
        ROUND(AVG(housing_cost), 2) AS metro_housing_cost
    FROM 
        cost_of_living
    WHERE 
        isMetro = 'TRUE'
    GROUP BY 
        state
),

OverallHousing AS (
    SELECT 
        state, 
        ROUND(AVG(housing_cost), 2) AS overall_housing_cost
    FROM 
        cost_of_living
    GROUP BY 
        state
)

SELECT
	top 5
    o.state, 
    o.overall_housing_cost, 
    m.metro_housing_cost, 
    ROUND(m.metro_housing_cost - o.overall_housing_cost, 2) AS housing_cost_difference
FROM 
    OverallHousing o
LEFT JOIN 
    MetroHousing m ON o.state = m.state
ORDER BY 
    housing_cost_difference desc;

with MetroCOL as (
	select 
		state, 
		round(avg(total_cost), 2) as metro_COL
	from cost_of_living
	where isMetro = 'TRUE'
	group by state
),
OverallCOL as (
	select
		state,
		round(avg(total_cost), 2) as total_COL
	from cost_of_living
	group by state
)

-- Cost of living metro versus overall

Select o.state, o.total_COL, m.metro_COL, round(m.metro_COL - o.total_COL, 2) as MetroCOL_difference
from OverallCOL o
join MetroCOL m on o.state = m.state
order by o.state;

with MetroCOL as (
	select 
		state, 
		round(avg(total_cost), 2) as metro_COL
	from cost_of_living
	where isMetro = 'TRUE'
	group by state
),
OverallCOL as (
	select
		state,
		round(avg(total_cost), 2) as total_COL
	from cost_of_living
	group by state
)

Select top 5 o.state, m.metro_COL, o.total_COL, round(m.metro_COL - o.total_COL, 2) as MetroCOL_difference
from OverallCOL o
join MetroCOL m on o.state = m.state
order by MetroCOL_difference desc;

with MetroCOL as (
	select 
		state, 
		round(avg(total_cost), 2) as metro_COL
	from cost_of_living
	where isMetro = 'TRUE'
	group by state
),
NonMetroCOL as (
	select
		state,
		round(avg(total_cost), 2) as nonMetro_COL
	from cost_of_living
	where isMetro = 'FALSE'
	group by state
),
OverallCOL as (
	select
		state,
		round(avg(total_cost), 2) as total_COL
	from cost_of_living
	group by state
)

select m.state, m.metro_COL, n.nonMetro_COL, o.total_COL
from MetroCOL m
join NonMetroCOL n on m.state = n.state
join OverallCOL o on n.state = o.state
order by m.state;

-- Most expensive to live in

select top 5 state, round(avg(total_cost), 2) as total_COL
from cost_of_living
group by state
order by total_COL desc;

select top 5 county, round(avg(total_cost), 2) as total_COL
from cost_of_living
group by county
order by total_COL desc;

select top 5 areaname, round(avg(total_cost), 2) as total_COL
from cost_of_living
group by areaname
order by total_COL desc;

-- Least expensive to live in

select top 5 state, round(avg(total_cost), 2) as total_COL
from cost_of_living
group by state
order by total_COL asc;

select top 5 county, round(avg(total_cost), 2) as total_COL
from cost_of_living
group by county
order by total_COL asc;

select top 5 areaname, round(avg(total_cost), 2) as total_COL
from cost_of_living
group by areaname
order by total_COL asc;

-- Healthcare Exploration

select state, round(avg(healthcare_cost), 2) as avg_healthcare_cost
from cost_of_living
group by state
order by avg_healthcare_cost desc;

select top 5 state, round(avg(healthcare_cost), 2) as avg_healthcare_cost
from cost_of_living
group by state
order by avg_healthcare_cost asc;

select top 5 county, state, round(avg(healthcare_cost), 2) as avg_healthcare_cost
from cost_of_living
group by county, state
order by avg_healthcare_cost asc;

select top 5 county, state, round(avg(healthcare_cost), 2) as avg_healthcare_cost
from cost_of_living
group by county, state
order by avg_healthcare_cost desc;

select top 5 state, round(avg(healthcare_cost), 2) as avg_healthcare_cost
from cost_of_living
group by state
order by avg_healthcare_cost desc;

select state, ROUND(AVG(healthcare_cost) * 100.0 / AVG(total_cost), 2) AS healthcare_pct_of_COL
from cost_of_living
group by state
order by state asc;

select state, ROUND(AVG(healthcare_cost) * 100.0 / AVG(median_family_income), 2) AS healthcare_pct_of_income
from cost_of_living
group by state
order by state asc;

select state, round(max(healthcare_cost), 2) as max_healthcare_cost, round(min(healthcare_cost), 2) as min_healthcare_cost
from cost_of_living
group by state;

-- Transportation Cost Exploration

select state, round(avg(transportation_cost), 2) as avg_transportation_cost
from cost_of_living
group by state
order by avg_transportation_cost desc;

select top 5 state, round(avg(transportation_cost), 2) as avg_transportation_cost
from cost_of_living
group by state
order by avg_transportation_cost asc;

select top 5 county, state, round(avg(transportation_cost), 2) as avg_transportation_cost
from cost_of_living
group by county, state
order by avg_transportation_cost asc;

select top 5 county, state, round(avg(transportation_cost), 2) as avg_transportation_cost
from cost_of_living
group by county, state
order by avg_transportation_cost desc;

select top 5 state, round(avg(transportation_cost), 2) as avg_transportation_cost
from cost_of_living
group by state
order by avg_transportation_cost desc;

select state, ROUND(AVG(transportation_cost) * 100.0 / AVG(total_cost), 2) AS transportation_pct_of_COL
from cost_of_living
group by state
order by state asc;

select state, ROUND(AVG(transportation_cost) * 100.0 / AVG(median_family_income), 2) AS transportation_pct_of_income
from cost_of_living
group by state
order by state asc;

select state, round(max(transportation_cost), 2) as max_transportation_cost, round(min(healthcare_cost), 2) as min_transportation_cost
from cost_of_living
group by state;

-- MISC

SELECT TOP 5 county, state,
       ROUND(AVG(housing_cost), 2) AS avg_housing_cost,
       ROUND(AVG(food_cost), 2) AS avg_food_cost,
       ROUND(AVG(childcare_cost), 2) AS avg_childcare_cost,
       ROUND(AVG(healthcare_cost), 2) AS avg_healthcare_cost
FROM cost_of_living
GROUP BY county, state
ORDER BY (AVG(housing_cost) + AVG(food_cost) + AVG(childcare_cost) + AVG(healthcare_cost)) DESC;

SELECT isMetro, 
       ROUND(AVG(housing_cost), 2) AS avg_housing_cost,
       ROUND(AVG(food_cost), 2) AS avg_food_cost,
       ROUND(AVG(transportation_cost), 2) AS avg_transportation_cost,
       ROUND(AVG(healthcare_cost), 2) AS avg_healthcare_cost,
       ROUND(AVG(childcare_cost), 2) AS avg_childcare_cost,
       ROUND(AVG(total_cost), 2) AS avg_total_cost
FROM cost_of_living
GROUP BY isMetro
ORDER BY isMetro;

SELECT TOP 5 
    county, 
    state,
    ROUND(MAX(housing_cost) - MIN(housing_cost), 2) AS housing_variance,
    ROUND(MAX(food_cost) - MIN(food_cost), 2) AS food_variance,
    ROUND(MAX(transportation_cost) - MIN(transportation_cost), 2) AS transportation_variance,
    ROUND(MAX(childcare_cost) - MIN(childcare_cost), 2) AS childcare_variance
FROM 
    cost_of_living
GROUP BY 
    county, state
ORDER BY 
    ROUND(MAX(housing_cost) - MIN(housing_cost), 2) + 
    ROUND(MAX(food_cost) - MIN(food_cost), 2) + 
    ROUND(MAX(transportation_cost) - MIN(transportation_cost), 2) + 
    ROUND(MAX(childcare_cost) - MIN(childcare_cost), 2) DESC;

SELECT state, 
      ROUND(AVG(taxes), 2) AS avg_taxes,
      ROUND(AVG(taxes) * 100.0 / AVG(total_cost), 2) AS taxes_pct_of_COL,
      ROUND(AVG(taxes) * 100.0 / AVG(median_family_income), 2) AS taxes_pct_of_income
FROM cost_of_living
GROUP BY state
ORDER BY taxes_pct_of_COL DESC;

--Queries for Visualization

SELECT state, ROUND(AVG(median_family_income), 2) AS avg_income, ROUND(AVG(total_cost), 2) AS avg_CostOfLiving
FROM cost_of_living
GROUP BY state
HAVING AVG(median_family_income) > AVG(total_cost)
ORDER BY state ASC;

SELECT TOP 5 state, ROUND(AVG(median_family_income), 2) AS avg_income, ROUND(AVG(total_cost), 2) AS avg_COL, ROUND(AVG(median_family_income) - AVG(total_cost), 2) AS income_surplus
FROM cost_of_living
GROUP BY state
HAVING AVG(median_family_income) > AVG(total_cost)
ORDER BY income_surplus DESC;

SELECT state, ROUND(AVG(childcare_cost) * 100.0 / AVG(total_cost), 2) AS childcare_pct_of_COL
FROM cost_of_living
GROUP BY state
ORDER BY state ASC;

SELECT state, ROUND(AVG(housing_cost) * 100.0 / AVG(total_cost), 2) AS housing_pct_of_COL
FROM cost_of_living
GROUP BY state
ORDER BY state ASC;

SELECT isMetro, 
       ROUND(AVG(housing_cost), 2) AS avg_housing_cost,
       ROUND(AVG(food_cost), 2) AS avg_food_cost,
       ROUND(AVG(transportation_cost), 2) AS avg_transportation_cost,
       ROUND(AVG(healthcare_cost), 2) AS avg_healthcare_cost,
       ROUND(AVG(childcare_cost), 2) AS avg_childcare_cost,
       ROUND(AVG(total_cost), 2) AS avg_total_cost
FROM cost_of_living
GROUP BY isMetro
ORDER BY isMetro;

SELECT TOP 5 state, ROUND(AVG(total_cost), 2) AS total_COL
FROM cost_of_living
GROUP BY state
ORDER BY total_COL DESC;

SELECT TOP 5 state, ROUND(AVG(total_cost), 2) AS total_COL
FROM cost_of_living
GROUP BY state
ORDER BY total_COL ASC;


SELECT TOP 5 
    county, 
    state,
    ROUND(MAX(housing_cost) - MIN(housing_cost), 2) AS housing_variance,
    ROUND(MAX(food_cost) - MIN(food_cost), 2) AS food_variance,
    ROUND(MAX(transportation_cost) - MIN(transportation_cost), 2) AS transportation_variance,
    ROUND(MAX(childcare_cost) - MIN(childcare_cost), 2) AS childcare_variance
FROM 
    cost_of_living
GROUP BY 
    county, state
ORDER BY 
    ROUND(MAX(housing_cost) - MIN(housing_cost), 2) + 
    ROUND(MAX(food_cost) - MIN(food_cost), 2) + 
    ROUND(MAX(transportation_cost) - MIN(transportation_cost), 2) + 
    ROUND(MAX(childcare_cost) - MIN(childcare_cost), 2) DESC;

SELECT state, 
      ROUND(AVG(taxes), 2) AS avg_taxes,
      ROUND(AVG(taxes) * 100.0 / AVG(total_cost), 2) AS taxes_pct_of_COL,
      ROUND(AVG(taxes) * 100.0 / AVG(median_family_income), 2) AS taxes_pct_of_income
FROM cost_of_living
GROUP BY state
ORDER BY taxes_pct_of_COL DESC;

SELECT TOP 5 state, ROUND(AVG(transportation_cost), 2) AS avg_transportation_cost
FROM cost_of_living
GROUP BY state
ORDER BY avg_transportation_cost DESC;

SELECT TOP 5 state, ROUND(AVG(transportation_cost), 2) AS avg_transportation_cost
FROM cost_of_living
GROUP BY state
ORDER BY avg_transportation_cost ASC;

SELECT 
    ROUND(AVG(total_cost), 2) AS overall_avg_cost_of_living,
    ROUND(AVG(median_family_income), 2) AS overall_avg_median_family_income
FROM cost_of_living;

-- Oklahoma Specific Insights

SELECT 
    ROUND(AVG(housing_cost), 2) AS avg_housing_cost,
    ROUND(AVG(food_cost), 2) AS avg_food_cost,
    ROUND(AVG(transportation_cost), 2) AS avg_transportation_cost,
    ROUND(AVG(healthcare_cost), 2) AS avg_healthcare_cost,
    ROUND(AVG(childcare_cost), 2) AS avg_childcare_cost,
    ROUND(AVG(total_cost), 2) AS avg_total_cost,
    ROUND(AVG(median_family_income), 2) AS avg_median_family_income
FROM cost_of_living
WHERE state = 'OK';

SELECT 
    ROUND(AVG(median_family_income), 2) AS avg_income,
    ROUND(AVG(total_cost), 2) AS avg_cost_of_living,
    ROUND(AVG(median_family_income) - AVG(total_cost), 2) AS income_surplus
FROM cost_of_living
WHERE state = 'OK';

SELECT 
    ROUND(AVG(childcare_cost), 2) AS avg_childcare_cost,
    ROUND(AVG(childcare_cost) * 100.0 / AVG(total_cost), 2) AS childcare_pct_of_COL,
    ROUND(AVG(childcare_cost) * 100.0 / AVG(median_family_income), 2) AS childcare_pct_of_income
FROM cost_of_living
WHERE state = 'OK';

SELECT 
    isMetro,
    ROUND(AVG(housing_cost), 2) AS avg_housing_cost,
    ROUND(AVG(food_cost), 2) AS avg_food_cost,
    ROUND(AVG(transportation_cost), 2) AS avg_transportation_cost,
    ROUND(AVG(healthcare_cost), 2) AS avg_healthcare_cost,
    ROUND(AVG(childcare_cost), 2) AS avg_childcare_cost,
    ROUND(AVG(total_cost), 2) AS avg_total_cost
FROM cost_of_living
WHERE state = 'OK'
GROUP BY isMetro;

SELECT TOP 5
    county,
    ROUND(AVG(total_cost), 2) AS avg_cost_of_living,
    ROUND(AVG(median_family_income), 2) AS avg_income,
    ROUND(AVG(total_cost) - AVG(median_family_income), 2) AS income_deficit
FROM cost_of_living
WHERE state = 'OK'
GROUP BY county
ORDER BY avg_cost_of_living asc;

SELECT 
    'Oklahoma' AS region, 
    ROUND(AVG(total_cost), 2) AS avg_cost_of_living,
    ROUND(AVG(median_family_income), 2) AS avg_median_family_income
FROM cost_of_living
WHERE state = 'OK'
UNION ALL
SELECT 
    'National Average' AS region, 
    ROUND(AVG(total_cost), 2) AS avg_cost_of_living,
    ROUND(AVG(median_family_income), 2) AS avg_median_family_income
FROM cost_of_living;

select *
from cost_of_living
where state = 'OK';
