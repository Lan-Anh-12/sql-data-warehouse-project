USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
DECLARE @start_time DATETIME , @end_time DATETIME 
BEGIN TRY
	PRINT'=================================='
	PRINT 'Loading bronze layer'
	PRINT'=================================='

	-- Bảng trống thì mới nạp dữ liệu 
	SET @start_time = GETDATE();
	PRINT 'Loading table bronze.crm_cust_info'
	TRUNCATE TABLE bronze.crm_cust_info;
	BULK INSERT bronze.crm_cust_info 
	FROM 'D:\Downloads\DATA_FILE_CSV\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);
	SET @end_time = GETDATE();
	PRINT 'Load duration: '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT'-----------------------------------------------'

	SET @start_time = GETDATE();
	PRINT 'Loading table bronze.crm_prd_info'
	TRUNCATE TABLE bronze.crm_prd_info;
	BULK INSERT bronze.crm_prd_info 
	FROM 'D:\Downloads\DATA_FILE_CSV\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);
	SET @end_time = GETDATE();
	PRINT 'Load duration: '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT'-----------------------------------------------'

	SET @start_time = GETDATE();
	PRINT 'Loading table bronze.crm_sales_details'
	TRUNCATE TABLE bronze.crm_sales_details;
	BULK INSERT bronze.crm_sales_details
	FROM 'D:\Downloads\DATA_FILE_CSV\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);
	SET @end_time = GETDATE();
	PRINT 'Load duration: '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT'-----------------------------------------------'

	SET @start_time = GETDATE();
	PRINT 'Loading table bronze.erp_cust_az12'
	TRUNCATE TABLE bronze.erp_cust_az12;
	BULK INSERT bronze.erp_cust_az12
	FROM 'D:\Downloads\DATA_FILE_CSV\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);
	SET @end_time = GETDATE();
	PRINT 'Load duration: '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT'-----------------------------------------------'

	SET @start_time = GETDATE();
	PRINT 'Loading table bronze.erp_loc_a101'
	TRUNCATE TABLE bronze.erp_loc_a101;
	BULK INSERT bronze.erp_loc_a101
	FROM 'D:\Downloads\DATA_FILE_CSV\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);
	SET @end_time = GETDATE();
	PRINT 'Load duration: '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT'-----------------------------------------------'

	SET @start_time = GETDATE();
	PRINT 'Loading table bronze.erp_px_cat_g1v2'
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'D:\Downloads\DATA_FILE_CSV\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);
	SET @end_time = GETDATE();
	PRINT 'Load duration: '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	PRINT'-----------------------------------------------'
END TRY

BEGIN CATCH 
	PRINT 'Error during loading bronze layer'
	PRINT 'Error message ' + ERROR_MESSAGE();
	PRINT 'Error message ' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error message ' + CAST (ERROR_STATE() AS NVARCHAR);
END CATCH 

END;

EXEC bronze.load_bronze;
