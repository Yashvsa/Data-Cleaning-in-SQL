
-- 1. Create a copy of raw dataset

CREATE TABLE Layoffs_staging
LIKE layoffs

-- 2. check for duplicates
SELECT *, 
ROW_NUMBER () OVER
(PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS ROW_NUM 
FROM layoffs_staging

-- USE CTE FOR MORE FILTERING 
-- ANY ROW NUMBER GREATER THAN 1 IS A DUPLICATE

WITH cte_duplicate_check AS
(
SELECT *, 
ROW_NUMBER () OVER
(PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS ROW_NUM 
FROM layoffs_staging
)
select * FROM cte_duplicate_check WHERE
ROW_NUM > 1; 

-- CREATING A NEW TABLE WITH ALL RESULTS TO THEN DELETE THE DUPLICATES, ADD ROW_NUMBER COLUMN

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
  `ROW_NUM`INT -- add row number column
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- INSERT THE INFORMATION INTO THE NEW TABLE 

INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER () OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS ROW_NUM 
FROM layoffs_staging;

SELECT * FROM layoffs_staging2 WHERE ROW_NUM > 1; -- QUERY TABLE TO CHECK DUPLICATES BEFORE DELETING(GOOD PRACTICE) 

DELETE FROM layoffs_staging2 WHERE ROW_NUM > 1; -- DELETES THE DUPLICATED RECORDS


-- 3. STANDARDIZE DATA 
-- Check for spaces in text before and after text and remove them 
SELECT * FROM layoffs_staging2;

SELECT company, trim(company) FROM layoffs_staging2;

UPDATE layoffs_staging2 
SET company = trim(company);

-- Change date to time column from text
SELECT `date`, str_to_date(`date`, '%m/%d/%Y') FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date` FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;



UPDATE layoffs_staging2 SET 
industry = NULL
where industry = '';

SELECT t1.industry, t2.industry FROM layoffs_staging2 t1
INNER JOIN layoffs_staging2 t2
ON t1.company = t2.company 
AND t1.location = t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;


SELECT count(*) FROM layoffs_staging2; -- Check Count of records

DELETE FROM layoffs_staging2 WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT count(*) FROM layoffs_staging2; -- Check count of records to compare with previous count if duplicates were null 


-- 4. removing the rows or columns

ALTER TABLE layoffs_staging2 
DROP COLUMN ROW_NUM;
