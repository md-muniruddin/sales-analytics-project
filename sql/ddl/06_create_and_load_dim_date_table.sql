CREATE TABLE dim_date (
    date DATE NOT NULL,
    year SMALLINT NOT NULL,
    month TINYINT NOT NULL,
    month_name VARCHAR(9) NOT NULL,
    year_month CHAR(7) NOT NULL,
    week_of_year TINYINT NOT NULL,
    day_of_week TINYINT NOT NULL,
    day_name VARCHAR(9) NOT NULL,
    is_weekend BIT NOT NULL,
    CONSTRAINT pk_dim_date PRIMARY KEY CLUSTERED (date)
);

--- 2nd
DECLARE @start_date DATE = '2010-01-01';
DECLARE @end_date   DATE = '2035-12-31';

WITH date_series AS (
    SELECT @start_date AS date_value
    UNION ALL
    SELECT DATEADD(DAY, 1, date_value)
    FROM date_series
    WHERE date_value < @end_date
)
INSERT INTO dim_date (
    date,
    year,
    month,
    month_name,
    year_month,
    week_of_year,
    day_of_week,
    day_name,
    is_weekend
)
SELECT
    date_value AS date,
    YEAR(date_value) AS year,
    MONTH(date_value) AS month,
    DATENAME(MONTH, date_value) AS month_name,
    FORMAT(date_value, 'yyyy-MM') AS year_month,
    DATEPART(WEEK, date_value) AS week_of_year,
    DATEPART(WEEKDAY, date_value) AS day_of_week,
    DATENAME(WEEKDAY, date_value) AS day_name,
    CASE
        WHEN DATEPART(WEEKDAY, date_value) IN (1, 7) THEN 1
        ELSE 0
    END AS is_weekend
FROM date_series
OPTION (MAXRECURSION 0);

-- 3rd

ALTER TABLE dim_date
ADD iso_week TINYINT,
    year_week CHAR(8);

    UPDATE dim_date
SET
    iso_week = DATEPART(ISO_WEEK, date),
    year_week = CONCAT(
        YEAR(date),
        '-W',
        RIGHT('0' + CAST(DATEPART(ISO_WEEK, date) AS VARCHAR(2)), 2)
    );