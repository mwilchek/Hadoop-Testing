CREATE EXTERNAL TABLE r_table (
rgram STRING, 
year INT,
occurrences INT, 
books INT
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' 
LOCATION '/user/hadoop/r-bigrams/';

CREATE TABLE r_output AS
SELECT rgram AS names,
SUM(occurrences) AS total_occurrances, 
SUM(books) AS total_books, 
SUM(occurrences)/SUM(books) AS avg_ocr,
MIN (year) AS first_year,
MAX (year) AS last_year,
COUNT(rgram) as duration
FROM r_table
GROUP BY rgram;

CREATE TABLE r_output1 AS
SELECT *
FROM r_output
WHERE first_year == 1950 AND duration ==60
ORDER BY avg_ocr DESC, names 
LIMIT 50;

CREATE TABLE r_output2 AS
SELECT *
FROM r_output1
ORDER BY avg_ocr;

INSERT OVERWRITE LOCAL DIRECTORY 'routput' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
select * from r_output2;