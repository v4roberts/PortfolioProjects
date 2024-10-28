select *
from PortfolioProject..CovidDeaths
where continent is not NULL
order by 3, 4

--select *
--from PortfolioProject..CovidVaccinations
--order by 3, 4

select location, date, total_cases, new_cases, total_deaths, population
from PortfolioProject..CovidDeaths
order by 1, 2

-- Looking at total cases vs. total deaths
-- Likelihood of dying of Covid by country

-- Used United States as a wealth counrty with a high average lifespan
select location, date, total_cases, total_deaths, (total_deaths/total_cases) * 100 as death_percentage
from PortfolioProject..CovidDeaths
where location like '%states%'
and continent is not null
order by 1, 2

-- Used Niger as a poorer counrty with a low average lifespan
select location, date, total_cases, total_deaths, (total_deaths/total_cases) * 100 as death_percentage
from PortfolioProject..CovidDeaths
where location = 'Niger'
order by 1, 2

-- The death percentage of those that contracted Covid in the United States (1.8%) versus those that contracted Covid in Niger (3.7%) is less than half as many. 

-- Looking at total cases versus the population.
-- 97% of the United States has access to clean water and by 4/30/2021 roughly 10% of the country's population had contracted Covid
select location, date, total_cases, population, (total_cases/population) * 100 as contract_percentage
from PortfolioProject..CovidDeaths
where location like '%states%'
order by 1, 2


-- 54% of Niger has access to clean water and by the same time, only 0.02% of the population had contracted Covid. Despite lack of resource, emergency response in Niger was elevated for a significant time https://pmc.ncbi.nlm.nih.gov/articles/PMC7719275/
select location, date, total_cases, population, (total_cases/population) * 100 as contract_percentage
from PortfolioProject..CovidDeaths
where location = 'Niger'
order by 1, 2

-- Countries with highest infection rate compared to populations
select location, population, max(total_cases) as highest_infectionCount, max((total_cases/population)) * 100 as percent_populationInfected
from PortfolioProject..CovidDeaths
--where location like '%states%'
group by location, population
order by percent_populationInfected desc

-- Countries with the highest death count per population
select location, max(cast(total_deaths as int)) as TotalDeaths
from PortfolioProject..CovidDeaths
where continent is not NULL
group by location, population
order by TotalDeaths desc

-- Let's break things down by continent



-- Showing continents with the highest death count per population

select continent, max(cast(total_deaths as int)) as TotalDeaths
from PortfolioProject..CovidDeaths
where continent is not NULL
group by continent
order by TotalDeaths desc


-- Global Numbers

select date, sum(new_cases) as TotalNewCasesEveryDay, sum(cast(new_deaths as int)) as TotalNewDeathsEachDay, sum(cast(new_deaths as int))/sum(new_cases) * 100 as DeathPercentage--, total_deaths, (total_deaths/total_cases) * 100 as death_percentage
from PortfolioProject..CovidDeaths
--where location like '%states%'
where continent is not null
group by date
order by 1, 2

--Ordered by date death percentage to see the days in the data where the death toll was highest
--February 24, 2020 is set as the day where the percentage of deaths is around 28% of total new cases.
select date, sum(new_cases) as TotalNewCasesEveryDay, sum(cast(new_deaths as int)) as TotalNewDeathsEachDay, sum(cast(new_deaths as int))/sum(new_cases) * 100 as DeathPercentage--, total_deaths, (total_deaths/total_cases) * 100 as death_percentage
from PortfolioProject..CovidDeaths
--where location like '%states%'
where continent is not null
group by date
order by DeathPercentage desc

select sum(new_cases) as TotalNewCasesEveryDay, sum(cast(new_deaths as int)) as TotalNewDeathsEachDay, sum(cast(new_deaths as int))/sum(new_cases) * 100 as DeathPercentage--, total_deaths, (total_deaths/total_cases) * 100 as death_percentage
from PortfolioProject..CovidDeaths
--where location like '%states%'
where continent is not null
--group by date
order by 1, 2

-- Looking at total population vs. vaccinations

select dea.continent, dea.location, dea.date, dea.population, dea.new_vaccinations
, sum(cast(vac.new_vaccinations as int)) over (partition by dea.location order by dea.location, dea.date) as RollingCountVaccinated--partition breaks it up by location
, (RollingCountVaccinated/population) * 100 as RollingPctVac
from PortfolioProject..CovidDeaths dea
join PortfolioProject..CovidVaccinations vac
	on dea.location = vac.location and dea.date = vac.date
where dea.continent is not null
order by 2, 3




-- Use CTE

With PopvsVac (Continent, Location, Date, Population, New_Vaccinations, RollingCountVaccinated)
as
(
select dea.continent, dea.location, dea.date, dea.population, dea.new_vaccinations
, sum(cast(vac.new_vaccinations as int)) over (partition by dea.location order by dea.location, dea.date) as RollingCountVaccinated--partition breaks it up by location
--, (RollingCountVaccinated/population) * 100 as RollingPctVac
from PortfolioProject..CovidDeaths dea
join PortfolioProject..CovidVaccinations vac
	on dea.location = vac.location and dea.date = vac.date
where dea.continent is not null
--order by 2, 3
)
Select *, (RollingCountVaccinated/population) * 100 as RollingPctVac
From PopvsVac

--Temp Table

Drop table if exists #PercentPopulationVaccinated
Create Table #PercentPopulationVaccinated
(
Continent nvarchar (255),
Location nvarchar (255),
Date datetime,
Population numeric,
New_Vaccinations numeric,
RollingCountVaccinated numeric
)

Insert into #PercentPopulationVaccinated
select dea.continent, dea.location, dea.date, dea.population, dea.new_vaccinations
, sum(cast(vac.new_vaccinations as int)) over (partition by dea.location order by dea.location, dea.date) as RollingCountVaccinated--partition breaks it up by location
--, (RollingCountVaccinated/population) * 100 as RollingPctVac
from PortfolioProject..CovidDeaths dea
join PortfolioProject..CovidVaccinations vac
	on dea.location = vac.location and dea.date = vac.date
where dea.continent is not null
--order by 2, 3

Select *, (RollingCountVaccinated/population) * 100 as RollingPctVac
From #PercentPopulationVaccinated

--Creating View to store data for later visualizations

Create View PercentPopulationVaccinated as
select dea.continent, dea.location, dea.date, dea.population, dea.new_vaccinations
, sum(cast(vac.new_vaccinations as int)) over (partition by dea.location order by dea.location, dea.date) as RollingCountVaccinated--partition breaks it up by location
--, (RollingCountVaccinated/population) * 100 as RollingPctVac
from PortfolioProject..CovidDeaths dea
join PortfolioProject..CovidVaccinations vac
	on dea.location = vac.location and dea.date = vac.date
where dea.continent is not null
--order by 2, 3