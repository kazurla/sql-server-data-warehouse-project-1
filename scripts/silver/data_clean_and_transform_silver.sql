/*
----------------------------------------------
SILVER LAYER DATA CLEANSING AND TRANSFORMATIONS
----------------------------------------------
1. Check and Remove Duplicates or nulls in the primary key
*/
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL

--Query shows cases where the primary key is used more than once; the issue can be resolved by taking the latest record
SELECT * 
FROM (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_dt DESC) AS flag_last
FROM bronze.crm_cust_info)t
WHERE flag_last = 1

--2.Check unwanted spaces in string values (cst_firstname, cst_lastname)

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

--The query show the first names with unwanted spaces to transform data into the correct format

TRIM(cst_firstname) AS cst_firstname
TRIM(cst_lastname) AS cst_lastname

--3.Data Standardisation and Handling Missing Data
--Checking the consistency of values in low cardinality columns like Gender (M, F) 

SELECT DISTINCT cst_gender
FROM bronze.crm_cust_info

--Query shows NULLs. We agreed to store clear values rather than abbreviated terms.
--Transform 'F' to 'Female'; 'M' to 'Male' and all null values to 'n/a'

SELECT cst_gender
CASE
	WHEN UPPER(TRIM(cst_gender))='M' THEN 'Male'
	WHEN UPPER(TRIM(cst_gender))='F' THEN 'Female'
	ELSE 'n/a'
END AS cst_gender

--4.Derive new columns
--Format of prod_key column in crm_prod_info table : 'CO-RF-TR-D83B-54' should be transformed to 'CO_RF' to match 
--id column in erp_px_cat_g1v2 table

SELECT prod_key
REPLACE(SUBSTRING(prod_key,1,5),'-','_') AS cat_id
FROM bronze.crm_prod_info

--5. Check the quality of data and handle missing data
--Check for negative numbers and null values in the product cost (prod_cost) column

SELECT prod_cost
FROM bronze.crm_prd_info
WHERE prod_cost < 0 OR prod_cost IS NULL

--Query shows no case for product cost being a negative number, but there are nulls,
--so change nulls to zero...

ISNULL(prod_cost, 0)

/*
6. Data Enrichment and data type casting
After checking the start and end dates of products in bronze.crm_prd_info, inconsistencies identified.
Some end dates precede their start dates.
Agreed with the business to change start and end dates as follows:
In case there is more than one record, the earliest record's start and end date will be accepted as the start and end dates,
for the second-earliest record, the earliest record's end date -1 day will be the start date.
The latest record will not have an end date
Cast all dates as DATE, not DATETIME
*/

SELECT CAST(prod_start_dt) AS prod_start_dt
CAST(LEAD(prod_start_date) OVER(PARTITION BY prod_key ORDER BY prod_start_dt)-1 AS DATE) AS prod_end_dt

/*
7. Transforming an integer to date
sls_order_dt column in bronze.crm_sales_details table is integer, but should be in date format.
Before transforming it to date format, we need to make some quality checks:
-if there are any nulls,
-if there are any values<0,
-if there are any values less than 8 digits,
-if the integer is >20500101 or <19000101
*/
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt<0 OR --1
LEN(sls_order_dt)!= 8 OR --2
sls_order_dt>20500101 OR --3
sls_order_dt<19000101  --4

--the query above returns that there are only issues with the 1st and 3rd clauses; these values will be transformed to nulls
--Below is the transformation

SELECT sls_order_dt
CASE 
	WHEN sls_order_dt<0 OR LEN(sls_order_dt)!=8 THEN NULL
	ELSE CAST(CAST (sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt

/*
