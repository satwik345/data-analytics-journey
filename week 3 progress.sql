
-- Data Cleaning
select * from layoffs;
select * from layoffs_cleaned;
select * from layoffs_cleaned2;


-- creating a new table to do data cleaning without messing up raw data
create table layoffs_cleaned like layoffs;


-- coping data from orginal table 
insert layoffs_cleaned select * from layoffs;


-- checking from duplicates 
-- deleting them from cte is not allowed in mysql, so create a new table with row number and then delete them
with cte as
(
select *, row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_cleaned
)
select * from cte where row_num > 1;

CREATE TABLE `layoffs_cleaned2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_cleaned2 select *, row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_cleaned;

select * from layoffs_cleaned2 where row_num > 1;
delete from layoffs_cleaned2 where row_num > 1;


-- standardizing data
select company,trim(company) from layoffs_cleaned2;
update layoffs_cleaned2 set company = trim(company);

select distinct location from layoffs_cleaned2 order by 1;
update layoffs_cleaned2 set location = trim(location);
select location from layoffs_cleaned2 where location like '%.';
update layoffs_cleaned2 set location = trim(trailing('.') from location) where location like '%.' ;

select distinct industry from layoffs_cleaned2 order by 1;
update layoffs_cleaned2 set company = trim(company);
select industry from layoffs_cleaned2 where industry like 'Crypto%';
update layoffs_cleaned2 set industry = 'Crypto' where industry = 'Crypto Currency' or industry = 'CryptoCurrency';

select distinct country from layoffs_cleaned2 order by 1;
update layoffs_cleaned2 set country = trim(country);
select country from layoffs_cleaned2 where country like '%.';
update layoffs_cleaned2 set country = trim(trailing('.') from country) where country like '%.' ;

update layoffs_cleaned2 set `date` = str_to_date(`date`,'%m/%d/%Y');
alter table layoffs_cleaned2 modify column `date` date;


-- Removing null and blank values from columns which are possible 
update layoffs_cleaned2 set industry = null where industry = '';
select * from layoffs_cleaned2 where industry is null ;
select * from layoffs_cleaned2 as t1 join layoffs_cleaned2 as t2 
on t1.company = t2.company
where t1.industry is null and t2.industry is not null;
update layoffs_cleaned2 as t1 join layoffs_cleaned2 as t2 
on t1.company = t2.company
set t1.industry = t2.industry 
where t1.industry is null and t2.industry is not null;


 
select * from layoffs_cleaned2  where total_laid_off is null and percentage_laid_off is null;
delete from layoffs_cleaned2  where total_laid_off is null and percentage_laid_off is null;
alter table layoffs_cleaned2 drop column row_num;



-- EXPLORATORY DATA ANALYSIS (EDA)

SELECT * FROM world_layoffs.layoffs_cleaned2;

-- 1. total layoffs overall
select sum(total_laid_off) as  total_laid_off from layoffs_cleaned2;


-- 2. total layoffs country wise 
 with laidoff_country_wise as
 (
 select country, count(company) as total_companies, sum(total_laid_off) as total_laidoff from layoffs_cleaned2
 where total_laid_off is not null
 group by country
 order by total_laidoff desc 
 )
select * from laidoff_country_wise;


-- 3. total layoffs industry wise  
 with laidoff_industry_wise as
 (
 select industry, count(company) as total_companies, sum(total_laid_off) as total_laidoff from layoffs_cleaned2
 where total_laid_off is not null
 group by industry
 order by total_laidoff desc 
 )
select * from laidoff_industry_wise;


-- 4. total layoffs company wise  
 with laidoff_company_wise as
 (
 select company, sum(total_laid_off) as total_laidoff from layoffs_cleaned2
 where total_laid_off is not null
 group by company
 order by total_laidoff desc 
 )
select * from laidoff_company_wise;


-- 5. Total layoffs funds wise  
WITH laidoff_funds_wise AS
(
    SELECT company, SUM(total_laid_off) AS total_laid_off,funds_raised_millions
    FROM layoffs_cleaned2
    WHERE total_laid_off and funds_raised_millions IS NOT NULL
    GROUP BY company, funds_raised_millions
    ORDER BY funds_raised_millions DESC
)
SELECT * FROM laidoff_funds_wise;


-- 6.total layoffs location wise  
 with laidoff_location_wise as
 (
 select location,  count(company) as total_companies, sum(total_laid_off) as total_laidoff from layoffs_cleaned2
 where total_laid_off is not null
 group by location
 order by total_laidoff desc 
 )
select * from laidoff_location_wise;


-- 7. total layoffs stage wise  
 with laidoff_stage_wise as
 (
 select stage,  count(company) as total_companies, sum(total_laid_off) as total_laidoff from layoffs_cleaned2
 where total_laid_off AND stage is not null
 group by stage
 order by total_laidoff desc 
 )
select * from laidoff_stage_wise;


-- 8. total layoffs year wise 
with laidoff_year_wise as
(
select count(company) as total_companies, sum(total_laid_off) as total_laidoff, year(`date`) as year from layoffs_cleaned2
where year(`date`)  is not null
group by year(`date`) 
order by year(`date`) 
)
select * from laidoff_year_wise;


-- 9. total layoffs month wise
with laidoff_year_wise as
(
select count(company) as total_companies, sum(total_laid_off) as total_laidoff, month(`date`) as month from layoffs_cleaned2
where `date`  is not null
group by month 
order by month 
)
select * from laidoff_year_wise;


-- 10. running total of layoffs
with laidoff_year_wise as
(
select mid(`date`,1,7) as month, sum(total_laid_off) as total_laidoff from layoffs_cleaned2
where `date` is not null
group by month
order by month
)
select *, sum(total_laidoff) over(order by month) as runnumg_total from laidoff_year_wise;

-- 11. top 5 largest layoffs by companyies year wise
with year_company_wise as 
(
select company, year(`date`) as year, sum(total_laid_off) as total_laidoff from layoffs_cleaned2
group by company, year
),
top_5 as
(
select *, dense_rank() over(partition by year order by total_laidoff desc) as ranking from year_company_wise
where year and total_laidoff is not null
)
select * from top_5
where ranking < 6;

