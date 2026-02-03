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
