/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '==============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==============================================';

		PRINT '----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '-- Truncating table : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '++ Inserting data into : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\hienn\Downloads\Work\CODE\data_courses\DWH\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			-- Starting line 2, first row being the column names
			FIRSTROW = 2,
			-- File delimiter
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds ';
		PRINT '---------------'

		/*
		-- Base Path
		DECLARE @RootPath NVARCHAR(500) = 'C:\Users\hienn\Downloads\Work\CODE\data_courses\DWH\sql-data-warehouse-project\datasets'

		-- Build full path
		DECLARE @FilePath NVARCHAR(500) = @RootPath + '\source_crm\prd_info.csv';

		-- Build SQL Command
		DECLARE @BulkInsertQuery NVARCHAR(MAX);

		SET @BulkInsertQuery = 
			N'BULK INSERT bronze.crm_prd_info
			FROM ''' + @FilePath + '''
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				TABLOCK
			);';

		-- 4. Execute the constructed string
		EXEC sp_executesql @BulkInsertQuery;

		PRINT 'Data loaded successfully from: ' + @FilePath;
		*/

		SET @start_time = GETDATE();
		PRINT '-- Truncating table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '++ Inserting data into : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\hienn\Downloads\Work\CODE\data_courses\DWH\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			-- Starting line 2, first row being the column names
			FIRSTROW = 2,
			-- File delimiter
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds ';
		PRINT '---------------'

		SET @start_time = GETDATE();
		PRINT '-- Truncating table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '++ Inserting data into : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\hienn\Downloads\Work\CODE\data_courses\DWH\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			-- Starting line 2, first row being the column names
			FIRSTROW = 2,
			-- File delimiter
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds ';
		PRINT '---------------'

		PRINT '----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '-- Truncating table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '++ Inserting data into : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\hienn\Downloads\Work\CODE\data_courses\DWH\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			-- Starting line 2, first row being the column names
			FIRSTROW = 2,
			-- File delimiter
			FORMAT = 'CSV',
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds ';
		PRINT '---------------'

		SET @start_time = GETDATE();
		PRINT '-- Truncating table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '++ Inserting data into : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\hienn\Downloads\Work\CODE\data_courses\DWH\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			-- Starting line 2, first row being the column names
			FIRSTROW = 2,
			-- File delimiter
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds ';
		PRINT '---------------'


		SET @start_time = GETDATE();
		PRINT '-- Truncating table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '++ Inserting data into : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\hienn\Downloads\Work\CODE\data_courses\DWH\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			-- Starting line 2, first row being the column names
			FIRSTROW = 2,
			-- File delimiter
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds ';
		PRINT '---------------'

		SET @batch_end_time = GETDATE();
		PRINT '==============================================';
		PRINT 'Loading Bronze layer is completed';
		PRINT '   >> Total duration: ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' milliseconds ';
		PRINT '==============================================';

	END TRY
	BEGIN CATCH
		PRINT '==============================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error message' + ERROR_MESSAGE();
		PRINT 'Error message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==============================================';
	END CATCH
END