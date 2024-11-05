-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

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



