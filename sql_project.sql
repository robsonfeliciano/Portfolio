/*
Amazon Stocks with Covid 19 Data Exploration
Skills used: Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types
*/

SELECT * FROM amazon;

-- Correct the data type

SELECT CAST(date as date) as new_date, CAST(open as double) as new_open, CAST(high as double) as new_high, CAST(low as double) as new_low,
CAST(close as double) as new_close, volume FROM amazon;

-- Create a Support Table for the Amazon Stocks

CREATE TABLE new_amazon AS
(SELECT CAST(date as date) as new_date, CAST(open as double) as new_open, CAST(high as double) as new_high, CAST(low as double) as new_low,
CAST(close as double) as new_close, volume FROM amazon);

DROP TABLE new_amazon;

-- Verify the new Table

SELECT * FROM new_amazon;

-- Max/Min Stock value per month
SELECT YEAR(new_date), MONTH(new_date), MAX(new_close), MIN(new_close) FROM new_amazon
GROUP BY 1,2;

-- Add the increase percentage

SELECT *, ROUND ((new_close / LAG(new_close) OVER (ORDER BY new_date) - 1), 3) as increase_decrease FROM new_amazon;

SELECT new_close, LAG(new_close) OVER (ORDER BY new_date) FROM new_amazon;

-- Add one more column to highlight the increase/decrease

SELECT *, ROUND ((new_close / LAG(new_close) OVER (ORDER BY new_date) - 1), 3) as increase_decrease_percentage,
CASE 
	WHEN ROUND ((new_close / LAG(new_close) OVER (ORDER BY new_date) - 1), 3) > 0 THEN 'Increase' 
    WHEN ROUND ((new_close / LAG(new_close) OVER (ORDER BY new_date) - 1), 3) < 0 THEN 'Decrease'
      WHEN ROUND ((new_close / LAG(new_close) OVER (ORDER BY new_date) - 1), 3) = 0 THEN 'Same'    
 END as increase_decrease
 FROM new_amazon;
 
 -- Use CTE to add more columns
 
 WITH support (new_date, new_open, new_high, new_low, new_close, volume, increase_decrease_percentage, increase_decrease)
 AS
 (SELECT *, ROUND ((new_close / LAG(new_close) OVER (ORDER BY new_date) - 1), 3) as increase_decrease_percentage,
CASE 
	WHEN ROUND ((new_close / LAG(new_close) OVER (ORDER BY new_date) - 1), 3) > 0 THEN 'Increase' 
    WHEN ROUND ((new_close / LAG(new_close) OVER (ORDER BY new_date) - 1), 3) < 0 THEN 'Decrease'
    WHEN ROUND ((new_close / LAG(new_close) OVER (ORDER BY new_date) - 1), 3) = 0 THEN 'Same'    
 END as increase_decrease 
 FROM new_amazon )
 
 SELECT *, CASE
 WHEN increase_decrease = 'Decrease' 
	THEN COUNT(increase_decrease) OVER (Partition BY increase_decrease ORDER BY new_date) 
	ELSE 0
END as decrease_count, CASE
 WHEN increase_decrease = 'Increase' 
	THEN COUNT(increase_decrease) OVER (Partition BY increase_decrease ORDER BY new_date) 
	ELSE 0
END as increase_count, CASE
 WHEN increase_decrease = 'Same' 
	THEN COUNT(increase_decrease) OVER (Partition BY increase_decrease ORDER BY new_date) 
	ELSE 0
END as same_count
FROM support
ORDER BY 1;

-- Create a final table with the Amazon stock data

CREATE TABLE amazon_final
AS
( WITH support (new_date, new_open, new_high, new_low, new_close, volume, increase_decrease_percentage, increase_decrease)
 AS
 (SELECT *, ROUND ((new_close / LAG(new_close) OVER (ORDER BY new_date) - 1), 3) as increase_decrease_percentage,
CASE 
	WHEN ROUND ((new_close / LAG(new_close) OVER (ORDER BY new_date) - 1), 3) > 0 THEN 'Increase' 
    WHEN ROUND ((new_close / LAG(new_close) OVER (ORDER BY new_date) - 1), 3) < 0 THEN 'Decrease'
    WHEN ROUND ((new_close / LAG(new_close) OVER (ORDER BY new_date) - 1), 3) = 0 THEN 'Same'    
 END as increase_decrease 
 FROM new_amazon )
 
 SELECT *, CASE
 WHEN increase_decrease = 'Decrease' 
	THEN COUNT(increase_decrease) OVER (Partition BY increase_decrease ORDER BY new_date) 
	ELSE 0
END as decrease_count, CASE
 WHEN increase_decrease = 'Increase' 
	THEN COUNT(increase_decrease) OVER (Partition BY increase_decrease ORDER BY new_date) 
	ELSE 0
END as increase_count, CASE
 WHEN increase_decrease = 'Same' 
	THEN COUNT(increase_decrease) OVER (Partition BY increase_decrease ORDER BY new_date) 
	ELSE 0
END as same_count
FROM support
ORDER BY 1);

-- Work with the covid data

SELECT * FROM covid;

-- Correct the Data Type

SELECT CAST(date as date) as new_date, death, deathincrease, positive, positiveincrease FROM covid ORDER BY 1; 

-- Create a support table

CREATE TABLE new_covid
AS
(SELECT CAST(date as date) as new_date, death, deathincrease, positive, positiveincrease FROM covid ORDER BY 1);

DROP TABLE new_covid;

-- Join the two table on the date

SELECT new_covid.new_date, new_covid.positive, new_covid.positiveincrease, amazon_final.new_close, amazon_final.increase_decrease
FROM new_covid
INNER JOIN amazon_final ON new_covid.new_date = amazon_final.new_date;

-- Analysis of the combined data

SELECT new_covid.*, amazon_final.new_close, amazon_final.increase_decrease, positive/(SELECT MAX(positive) FROM new_covid), positiveincrease/positive, amazon_final.increase_decrease_percentage
FROM new_covid
INNER JOIN amazon_final ON new_covid.new_date = amazon_final.new_date;

