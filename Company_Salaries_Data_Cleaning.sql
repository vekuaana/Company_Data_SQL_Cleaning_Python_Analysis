----- 1. CREATE, FILL TABLES AND ADJUST 
-- Create a table named companies with columns company_name, company_city, company_state, company_type, and const_site_category
CREATE TABLE companies (
    company_name varchar(255),
    company_city varchar(255),
	company_state varchar(255),
	company_type varchar(255),
	const_site_category varchar(255)
);

-- Copy data from the specified CSV file '/tmp/companies.csv' into the companies table, assuming CSV format with ';' delimiter and the first row as header
COPY companies FROM '/tmp/companies.csv' DELIMITER ';' CSV HEADER;

-- Create a table named employees with columns comp_code_emp, employee_code_emp, employee_name_emp, gender, and age
CREATE TABLE employees (
    comp_code_emp varchar(255),
    employee_code_emp int,
	employee_name_emp varchar(255),
	gender varchar(10),
	age int
);



-- Copy data from the specified CSV file '/tmp/employees.csv' into the employees table, assuming CSV format with ';' delimiter and the first row as header
COPY employees FROM '/tmp/employees.csv' DELIMITER ';' CSV HEADER;

-- Create a table named functions with columns function_code, function, and function_group
CREATE TABLE functions (
    function_code int,
    function varchar(255),
	function_group varchar(255)
);

-- Copy data from the specified CSV file '/tmp/functions.csv' into the functions table, assuming CSV format with ';' delimiter and the first row as header
COPY functions FROM '/tmp/functions.csv' DELIMITER ';' CSV HEADER;

-- Create a table named salaries with columns comp_code, comp_name, employee_id, employee_name, date, func_code, func, and salary
CREATE TABLE salaries (
    comp_code varchar(255),
    comp_name varchar(255),
	employee_id int,
	employee_name varchar(255),
	date timestamp,
	func_code int,
	func varchar(255),
	salary varchar(255) --after loading table, change , to . and data type to float
);

-- Copy data from the specified CSV file '/tmp/salaries.csv' into the salaries table, assuming CSV format with ';' delimiter and the first row as header
COPY salaries FROM '/tmp/salaries.csv' DELIMITER ';' CSV HEADER;
	
-- Join all the tables together in a new table emp_dataset

SELECT * 
INTO emp_dataset
FROM salaries s
LEFT JOIN employees e 
ON s.employee_id = e.employee_code_emp
LEFT JOIN companies c 
ON s.comp_name = c.company_name
LEFT JOIN functions f 
ON s.func_code = f.function_code
	
-- Change separator to '.' and cast double
UPDATE emp_dataset 
SET salary = REPLACE(salary, ',', '.'); 
ALTER TABLE emp_dataset
ALTER COLUMN salary TYPE float USING salary::double precision

-- Columns are selected and renamed for data analysis.
SELECT
    date AS month_year,
    employee_id, 
    employee_name,
    age,
    gender,
    salary,
    function,
    function_group, 
    comp_name AS company_name, 
    company_city, 
    company_state, 
    company_type, 
    const_site_category
INTO df_employee
FROM emp_dataset;

----- 2. DATA EXPLORATION AND CLEANING
-- AGE
SELECT DISTINCT age
FROM df_employee
ORDER BY age ASC -- age varies from 18 to 42
-- GENDER
SELECT DISTINCT gender
FROM df_employee -- F or M, We will standardize this columns to 'Female' for 'F' and 'Male' for 'M'

UPDATE df_employee
SET gender = CASE gender
					WHEN 'M' then 'Male'
					WHEN 'F' then 'Female'
			 END;
-- SALARY
SELECT DISTINCT salary
FROM df_employee -- NULL values are present!
	
SELECT MAX(salary), MIN(salary)
FROM df_employee -- The maximum salary is 1 million. This seems to be too much. Let's inspect the row. 

SELECT * 
FROM df_employee
WHERE salary = 1000000;
--- 1 milion salary is find for 8 people of the same company, on the same month (january)
--- This must be an error 
--- Check the salary of these people for other months

SELECT employee_name, salary, month_year
FROM df_employee
WHERE employee_name IN (
	SELECT employee_name
	FROM df_employee
	WHERE salary = 1000000 ) 
ORDER BY employee_name;
-- We see that 1 milion is realy an unusual salary. Moreover, we can see the date january is duplicated for some employee
-- We will delete salaries equal to 1 million.
DELETE FROM df_employee
WHERE salary = 1000000

SELECT DISTINCT comp_code
FROM df_employee -- Does not seem to be a usefull variable. This will be droped

--MONTH
SELECT DISTINCT month_year 
FROM df_employee
ORDER BY month_year  ASC
-- COMPANY NAME
SELECT DISTINCT comp_name
FROM df_employee
ORDER BY comp_name ASC
-- COMPANY CITY
SELECT DISTINCT company_city
FROM df_employee
ORDER BY company_city ASC -- There is a duplicate company city spelled wrong Goiania instead of Goiania

UPDATE df_employee
SET company_city = 'Goianiaa'
WHERE company_city = 'Goiania'
--COMPANY STATE
SELECT DISTINCT company_state
FROM df_employee;
--COMPANY TYPE 
SELECT DISTINCT company_type
FROM df_employee; -- There is a duplicate Construction Sites anc Construction Site

UPDATE df_employee
SET company_type = 'Construction Site'
WHERE company_type = 'Construction Sites'
--CONSTRUCTION SITE
SELECT DISTINCT const_site_category
FROM df_employee -- NULL values are present!

-- SUM UP: We have to deal with the NULL values in const_site_categpru and salary and with the extrem values in salary.
-- Moreover, duplicate values are present in the dataset (at least duplicate date)

-- 2.1 FOCUS ON NULL VALUES
SELECT * FROM df_employee
WHERE month_year ISNULL
		       OR employee_id ISNULL
		       OR employee_name ISNULL
			   OR age ISNULL
			   OR gender ISNULL
		       OR salary ISNULL
			   OR function ISNULL
		       OR function_group ISNULL
		       OR company_name ISNULL
		       OR company_city ISNULL
		       OR company_state ISNULL
		       OR company_type ISNULL
		       OR const_site_category ISNULL; --There is 70 null values in the salary column.

-- For the 'salary' column, we could estimate missing salaries by calculating the average salary within various categories, such as company_name, function, function_group, and company_state. However, due to the small number of missing values (70), we have opted to drop rows with NULL values in the 'salary' column.
-- Regarding the 'const_site_category' column, since there are many missing values, we have chosen to retain the column for analysis purposes, handling NULL values as needed when analyzing the 'const_site_category' variable.

DELETE FROM df_employee
WHERE salary IS NULL;

-- 2.2 FOCUS ON DUPLICATE : Deletion of duplicates
SELECT CONCAT(employee_name,' ', CAST(month_year AS varchar))
FROM df_employee --7979 rows
SELECT DISTINCT CONCAT(employee_name,' ', CAST(month_year AS varchar)) AS unique_id
FROM df_employee --7793 rows
ORDER BY unique_id ASC 
-- There is 186 duplicates 

-- We create the variable unique_id and we delete the duplicates 
ALTER TABLE df_employee
ADD unique_id varchar(255);

UPDATE df_employee
SET unique_id = CONCAT(employee_name,' ', CAST(month_year AS varchar));

DELETE FROM df_employee
WHERE ctid IN (
    SELECT ctid
    FROM (
        SELECT ctid, ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY ctid) AS rn
        FROM df_employee
    ) AS sub
    WHERE rn > 1
);

-- Our dataset is clean!

