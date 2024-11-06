-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT * FROM world_layoffs.layoffs;

-- CREATE COPY OF RAW DATA	
CREATE TABLE layoffs_raw
LIKE layoffs;

INSERT layoffs_raw
SELECT*
FROM layoffs;

-- CHECK FOR DUPLICATES
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs;

WITH checkduplicate_cte AS 
(SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs
)
SELECT *
FROM checkduplicate_cte
WHERE row_num >1;

-- CREATE NEW TABLE AND INSERT DISTINCT RECORDS ONLY
CREATE TABLE layoffs_distinct
LIKE layoffs;

ALTER TABLE layoffs_distinct
ADD row_num INT NOT NULL;

INSERT layoffs_distinct
WITH checkduplicate_cte AS 
(SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs
)
SELECT *
FROM checkduplicate_cte
WHERE row_num =1;

-- DROP ROW_NUM COLUMN
ALTER TABLE layoffs_distinct
DROP row_num;




-- STANDARDIZING

-- TRIM EXTRA SPACES
UPDATE layoffs_distinct
SET company = TRIM(company);

-- CHECK FOR INCONSISTENCIES IN EACH COLUMN
SELECT DISTINCT industry
FROM layoffs_distinct
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_distinct
ORDER BY 1;

-- STANDARZIE VALUES IN EACH COLUMN
SELECT *
FROM layoffs_distinct
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_distinct
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- TRIM EXTRA CHARACTERS AT THE END OF A COUNTRY NAME
UPDATE layoffs_distinct
SET country = TRIM(Trailing '.' FROM country)
WHERE country LIKE 'United States%';


-- CHANGE DATES to DATE FORMAT 
UPDATE layoffs_distinct
SET `date` = str_to_date(`date`,'%m/%d/%Y');

-- FIND NULL DATES
SELECT date, company	
FROM layoffs_distinct
WHERE STR_TO_DATE(date, '%m/%d/%Y') IS NULL;

-- CHANGE DATE COLUMN TO DATE DATATYPE
ALTER TABLE layoffs_distinct
MODIFY COLUMN `date` DATE;

-- FIND NULLS
SELECT *
FROM layoffs_distinct
WHERE industry IS NULL 
OR industry = '';

-- SHOW SIDE BY SIDE COMPARISION OF COMPANIES THAT HAVE INDUSTRY THAT ARE FILLED OUT, NULLS, or BLANKS
SELECT lay1.company, lay1.industry, lay2.industry
FROM layoffs_distinct lay1
JOIN layoffs_distinct lay2
	ON lay1.company = lay2.company
WHERE (lay1.industry IS NULL OR lay1.industry = '')
AND lay2.industry IS NOT NULL;

-- SET ALL BLANK INDUSTRIES TO NULL
UPDATE layoffs_distinct
SET industry = NULL
WHERE INDUSTRY = '';

--POPULATE NULL INDUSTRY WITH AVAILABLE INDUSTRY FROM THE SAME COMPANY
UPDATE layoffs_distinct lay1
JOIN layoffs_distinct lay2
	ON lay1.company = lay2.company
SET lay1.industry = lay2.industry
WHERE lay1.industry is null AND lay2.industry IS NOT NULL;
