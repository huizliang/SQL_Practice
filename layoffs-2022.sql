-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- CREATE a copy of the table 

CREATE TABLE layoffs_staging
LIKE layoffs;

-- insert data into copied table

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- view table
SELECT *
FROM layoffs_staging;

-- query to create a new column called row_num and assign all distinct rows with the row_num 1 and duplicates as 2
SELECT *,
ROW_NUMBER () OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- filter the above query to show only row_num'2' or duplicates
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER () OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- double check to see if results above are duplicates
SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

-- create a new table with the row_num column
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- INSERT row_num data into new table (it has nulls right now)
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER () OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Filter layoffs_staging2 to show duplicates
SELECT *
FROM layoffs_staging2
WHERE row_num = 2;

-- Delete row containing row_num 2 (duplicates)
DELETE 
FROM layoffs_staging2
WHERE row_num >1;

-- Standardizing data

-- trim extra spaces from company names
SELECT company, (TRIM(company))
FROM layoffs_staging2;

-- replace the current company names with the trimmed company names
UPDATE layoffs_staging2
SET company = TRIM(company);

-- check to see if industry names are consistent (you'll see that cryto currency is written in diff ways)
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- make all crypto industry names consistent
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- repeat this process for other columns

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United%';


-- change date format
-- change date from string to date 
-- change date format to year, month, date (case sensitive)

SELECT `date`,
str_to_date(`date`,'%m/%d/%Y')
FROM layoffs_staging2;


-- update date
UPDATE layoffs_staging2
SET `date` = str_to_date(`date`,'%m/%d/%Y');

SELECT *
FROM layoffs_staging2;


-- change data type of date to date
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- dealing with nulls

-- find rows where industry is null or blank
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- look up companies to see if you can figure out their industry. If yes, update it. In this case, airbnb is Travel
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- use JOIN to pull up rows where industry is null or blank AND not null so you can compare them side by side
SELECT t1.company, t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- update blank industries to nulls so it's easier to update them all at once
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- update all null industries in t1 to matching non-null inustries in t2
UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- check for nulls or blanks again
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';


-- find company with most layoffs
SELECT MAX(total_laid_off), company
FROM layoffs_staging2
GROUP BY company
ORDER BY 1 DESC;

-- see companies with 100% layoffs
SELECT * 
From layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- total layoffs by company
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP by company
ORDER BY 2 DESC;

-- time span of layoffs
SELECT MIN(date), MAX(date)
FROM layoffs_staging2;

-- see total layoffs by year
SELECT YEAR(date), SUM(total_laid_off)
FROM layoffs_staging2
GROUP by YEAR(date)
ORDER BY 1 DESC;

-- see total layoffs by month and year
SELECT SUBSTRING(date,1,7) AS `year_month`, 
SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(date,1,7) IS NOT NULL
GROUP BY `year_month`
Order by 1 ASC;

-- add rolling_total column
WITH rolling_total AS
( 
SELECT SUBSTRING(date,1,7) AS `year_month`, 
SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(date,1,7) IS NOT NULL
GROUP BY `year_month`
Order by 1 ASC
)
SELECT `year_month`, total_off
,SUM(total_off) OVER(ORDER BY `year_month`) AS rolling_total
FROM rolling_total;


-- find how many layoffs per year by company
SELECT company, year(date), SUM(total_laid_off)
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL
GROUP by company, year(date);



-- find top 5 companies with largest total laid off for each year
WITH company_year (company, year, total_laid_off) AS
(
SELECT company, year(date), SUM(total_laid_off)
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL
GROUP by company, year(date)
), Company_Year_Rank AS -- second CTE to rank first CTE 
(SELECT *, 
DENSE_RANK() OVER (PARTITION BY year ORDER BY total_laid_off DESC) AS Ranking
FROM company_year
WHERE year IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5
;

-- find company with most layoffs
SELECT MAX(total_laid_off), company
FROM layoffs_staging2
GROUP BY company
ORDER BY 1 DESC;

-- see companies with 100% layoffs
SELECT * 
From layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- total layoffs by company
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP by company
ORDER BY 2 DESC;

-- time span of layoffs
SELECT MIN(date), MAX(date)
FROM layoffs_staging2;

-- see total layoffs by year
SELECT YEAR(date), SUM(total_laid_off)
FROM layoffs_staging2
GROUP by YEAR(date)
ORDER BY 1 DESC;

-- see total layoffs by month and year
SELECT SUBSTRING(date,1,7) AS `year_month`, 
SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(date,1,7) IS NOT NULL
GROUP BY `year_month`
Order by 1 ASC;

-- add rolling_total column
WITH rolling_total AS
( 
SELECT SUBSTRING(date,1,7) AS `year_month`, 
SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(date,1,7) IS NOT NULL
GROUP BY `year_month`
Order by 1 ASC
)
SELECT `year_month`, total_off
,SUM(total_off) OVER(ORDER BY `year_month`) AS rolling_total
FROM rolling_total;


-- find how many layoffs per year by company
SELECT company, year(date), SUM(total_laid_off)
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL
GROUP by company, year(date);



