-- Data Cleaning
SELECT *
FROM layoffs
LIMIT 10;

/*Steps to follow:
1. Remove duplicates.
2. Standardize the data.
3. Null values or blank values.
4. Remove any columns rows.
*/

-- Create a copy of the original table(layoffs) where changes will be made without affecting the raw data.
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging
LIMIT 10;

--1. Removing duplicates.
SELECT*,
    ROW_NUMBER() OVER (
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging;

WITH duplicate_cte AS(
    SELECT*,
        ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
    FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

--Create a layoffs_staging2 database, filter by row_nums and then delete those equal to  2
CREATE TABLE layoffs_staging2(
    company text, 
    location text, 
    industry text, 
    total_laid_off int DEFAULT NULL, 
    percentage_laid_off text, 
    date text, 
    stage text, 
    country text, 
    funds_raised_millions int DEFAULT NULL,
    row_num int
);

INSERT INTO layoffs_staging2
SELECT*,
        ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
    FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

--2. Standardizing data
SELECT company, TRIM(company)
FROM layoffs_staging2

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'; 

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'; 

--Changing date data type
SELECT date, 
    STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET date = STR_TO_DATE(`date`, '%m/%d/%Y'); 

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

--Populating null values
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
   ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 AS t2
   ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

--select and delete rows where total_laid_off and percentage_laid_off are NULL

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

--Drop column row_num
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

























