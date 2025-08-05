/* BULK INSERT İLE HEDEF TABLODAN BRONZE ŞEMASINA VERİ AKTARIMI SÜRECİ */

EXEC bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;

BEGIN TRY
SET @batch_start_time = GETDATE();

SET @start_time = GETDATE();
TRUNCATE TABLE bronze.crm_cust_info;

BULK INSERT bronze.crm_cust_info from 'C:\Users\ASUS\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
SET @end_time = GETDATE();
PRINT 'Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
PRINT '---------------'

SET @start_time = GETDATE();
TRUNCATE TABLE bronze.crm_prd_info;

BULK INSERT bronze.crm_cust_info from 'C:\Users\ASUS\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
SET @end_time = GETDATE();
PRINT 'Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
PRINT '---------------'

SET @start_time = GETDATE();
TRUNCATE TABLE bronze.crm_sales_details;

BULK INSERT bronze.crm_sales_details from 'C:\Users\ASUS\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
SET @end_time = GETDATE();
PRINT 'Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
PRINT '---------------'

SET @start_time = GETDATE();
TRUNCATE TABLE bronze.erp_px_cat_g1v2;

BULK INSERT bronze.erp_px_cat_g1v2 from 'C:\Users\ASUS\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
SET @end_time = GETDATE();
PRINT 'Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
PRINT '---------------'

SET @start_time = GETDATE();
TRUNCATE TABLE bronze.erp_cust_az12;

BULK INSERT bronze.erp_cust_az12 from 'C:\Users\ASUS\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
SET @end_time = GETDATE();
PRINT 'Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
PRINT '---------------'


SET @start_time = GETDATE();
TRUNCATE TABLE bronze.erp_loc_a101;

BULK INSERT bronze.erp_loc_a101 from 'C:\Users\ASUS\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH (
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
SET @end_time = GETDATE();
PRINT 'Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
PRINT '---------------'

SET @batch_end_time = GETDATE();
PRINT 'Bronze Katmanı Yükleme İşlemi Tamamlandı'
PRINT 'Toplam Yükleme Süresi:'+ CAST(DATEDIFF(SECOND, @batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';

END TRY
BEGIN CATCH
	PRINT  '==================================='
	PRINT  'BRONZE KATMAN YÜKLENİRKEN HATA OLUŞTU.'
	PRINT  'Hata Mesajı'+ ERROR_MESSAGE();
	PRINT  'Hata Mesajı'+ CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT  'Hata Mesajı'+ CAST(ERROR_STATE() AS NVARCHAR);
	PRINT  '==================================='
END CATCH
END
