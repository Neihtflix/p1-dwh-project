/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncate Silver tables.
		- Insert transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '==============================================';
		PRINT 'Loading Silver Layer';
		PRINT '==============================================';

		PRINT '----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '-- Truncating table : silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info

		PRINT '++ Inserting data into : silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		-- Removing duplicates, unwanted space, normalize gender and marital status to readable formats, handling missing values
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE UPPER(TRIM(cst_marital_status))
				WHEN 'S' THEN 'Single'
				WHEN 'M' THEN 'Married'
				ELSE 'n/a' 
			END cst_marital_status,
			CASE UPPER(TRIM(cst_gndr))
				WHEN 'F' THEN 'Female'
				WHEN 'M' THEN 'Male'
				ELSE 'n/a'
			END cst_gndr,
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t
		WHERE flag_last = 1; -- Select the most recent record per customer

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds ';
		PRINT '---------------'


		SET @start_time = GETDATE();
		PRINT '-- Truncating table : silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info

		PRINT '++ Inserting data into : silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(TRIM(prd_key), 1, 5),'-','_') AS cat_id,	-- Extract Category ID
			SUBSTRING(TRIM(prd_key), 7, LEN(TRIM(prd_key))) AS prd_key,	-- Extract Product Key
			prd_nm,
			COALESCE(prd_cost,0) AS prd_cost,
			CASE UPPER(TRIM(prd_line)) 
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,											-- Map product line codes to readable format
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE
			) AS prd_end_dt												-- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds ';
		PRINT '---------------'

		SET @start_time = GETDATE();
		PRINT '-- Truncating table : silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details

		PRINT '++ Inserting data into : silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			-- Business rules : sales = qty * price
			-- Recalculating Sales if negative, zero or null values 
			CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END sls_sales,
			sls_quantity,
			-- Recalculating Price if zero or null values
			CASE WHEN sls_price <= 0 OR sls_price IS NULL
					THEN sls_sales / NULLIF(sls_quantity,0)
				ELSE sls_price
			END sls_price
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds ';
		PRINT '---------------'

		PRINT '----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '-- Truncating table : silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12

		PRINT '++ Inserting data into : silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid))
				ELSE cid
			END cid,
			CASE WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END bdate,
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				ELSE 'n/a'
			END gen
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds ';
		PRINT '---------------'


		SET @start_time = GETDATE();
		PRINT '-- Truncating table : silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101

		PRINT '++ Inserting data into : silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
		SELECT
			REPLACE(cid,'-','') AS cid,
			CASE 
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END cntry
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds ';
		PRINT '---------------'


		SET @start_time = GETDATE();
		PRINT '-- Truncating table : silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2

		PRINT '++ Inserting data into : silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT 
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds ';
		PRINT '---------------'

		SET @batch_end_time = GETDATE();
		PRINT '==============================================';
		PRINT 'Loading Silver layer is completed';
		PRINT '   >> Total duration: ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' milliseconds ';
		PRINT '==============================================';

		/*
		SELECT
			cst_id,
			COUNT(*)
		FROM silver.crm_cust_info
		GROUP BY cst_id
		HAVING COUNT(*) > 1 OR cst_id IS NULL;

		-- Checking for unwanted space
		SELECT
			cst_firstname
		FROM silver.crm_cust_info
		WHERE cst_firstname != TRIM(cst_firstname);


		-- Data standardization and consistency
		SELECT
			DISTINCT(cst_marital_status)
		FROM silver.crm_cust_info;

		SELECT
			DISTINCT(cst_gndr)
		FROM silver.crm_cust_info;

		SELECT * FROM silver.crm_cust_info;
		*/
	END TRY
	BEGIN CATCH
		PRINT '==============================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error message' + ERROR_MESSAGE();
		PRINT 'Error message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==============================================';
	END CATCH
END
