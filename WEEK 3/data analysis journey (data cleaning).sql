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
