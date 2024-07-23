select * from layoffs;

-- 1. remove deuplicates
-- 2. standarization
-- 3. remove null and blank values
-- 4. remove any columns

create table lay_at
like layoffs;

insert into lay_at
select * from layoffs;

select * from lay_at;

-- Removing Duplicates

select *, 
row_number() over(partition by company ,
location ,
industry ,
total_laid_off ,
percentage_laid_off ,
date ,
stage,
country ,
funds_raised_millions) as row_num
from lay_at;

with dup_cte as (
select *, 
row_number() over(partition by company ,
location ,
industry ,
total_laid_off ,
percentage_laid_off ,
date ,
stage,
country ,
funds_raised_millions) as row_num
from lay_at
)
select *
from dup_cte
where row_num>1;

CREATE TABLE `lay_at3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into lay_at3 
select *, 
row_number() over(partition by company ,
location ,
industry ,
total_laid_off ,
percentage_laid_off ,
date ,
stage,
country ,
funds_raised_millions) as row_num
from lay_at;

select * from lay_at3
where row_num>1;

delete 
 from lay_at3
 where row_num>1;



-- Standarizing data

select company, trim(company)
from lay_at3;

update lay_at3
set company=trim(company);

select *
from lay_at3;

select distinct industry
from lay_at3
order by 1;

select *
from lay_at3
where industry like 'Crypto%';

update lay_at3
set industry='Crypto'
where industry like 'Crypto%';

select distinct location
from lay_at3
order by 1;

select distinct country
from lay_at3
order by 1;

select distinct country, trim(trailing '.'from country) as country1
from lay_at3
order by 1;

update lay_at3
set country=trim(trailing '.'from country) 
where country like 'United States%';

select date,
str_to_date(date,'%m/%d/%Y')
from lay_at3;

update lay_at3
set date=str_to_date(date,'%m/%d/%Y')
;

select * from lay_at3;

alter table lay_at3
modify column date date;


-- null and blank values

select * 
from lay_at3
where total_laid_off is null
and percentage_laid_off is null;

select *
from lay_at3
where industry is null
or industry='';

select *
from lay_at3
where company='Airbnb';

update lay_at3
set industry='Travel'
where company='Airbnb';

select * 
from lay_at3 t1
join lay_at3 t2
on t1.company=t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update lay_at3 
set industry=null
where industry ='';

update lay_at3 t1
join lay_at3 t2
on t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is null 
and t2.industry is not null;

select distinct industry from lay_at3 order by 1;

-- deleting null values

delete
from lay_at3
where total_laid_off is null
and percentage_laid_off is null;

alter table lay_at3
drop column row_num;

-- Exploratory Data Analysis

select max(total_laid_off), max(percentage_laid_off)
from lay_at3;

select * from lay_at3
where percentage_laid_off=1
order by total_laid_off desc;

select * from lay_at3
where percentage_laid_off=1
order by funds_raised_millions desc;

select company, sum(total_laid_off) as sum
from lay_at3
group by company
order by sum desc;

select industry, sum(total_laid_off) as sum
from lay_at3
group by industry
order by sum desc;

select min(date), max(date) from lay_at3;

select country, sum(total_laid_off) as sum
from lay_at3
group by country
order by sum desc;

select date, sum(total_laid_off) as sum
from lay_at3
group by date
order by sum desc;

select year(date), sum(total_laid_off) as sum
from lay_at3
group by year(date)
order by sum desc;

select stage, sum(total_laid_off) as sum
from lay_at3
group by stage
order by sum desc;

select company, sum(percentage_laid_off) as sum
from lay_at3
group by company
order by sum desc;

select substring(date, 1, 7) as month , sum(total_laid_off) as sum
from lay_at3
where substring(date, 1, 7) is not null
group by month
order by 1 asc;

with rolling_total as (
select substring(date, 1, 7) as month , sum(total_laid_off) as sum
from lay_at3
where substring(date, 1, 7) is not null
group by month
order by 1 asc
)
select month, sum, sum(sum) over(order by month) as rolling
from rolling_total;

select company, year(date), sum(total_laid_off) as sum
from lay_at3
group by company, year(date)
order by 3 desc ;

with company_year(company, years, total_laid_off) as (
select company, year(date), sum(total_laid_off) as sum
from lay_at3
group by company, year(date)
order by 3 desc 
), company_year_rank as (
select * , dense_rank() over(partition by years order by total_laid_off desc) as dense_ranks
from company_year
where years is not null)
select *
from company_year_rank
where dense_ranks <=5;