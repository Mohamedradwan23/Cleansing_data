-- when you want to drop database but it was used what should you do :
ALTER DATABASE DataWarehouse SET OFFLINE WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
-- Creating database,schemas and tables 
Create database DataWarehouseData ;
go 
Use DataWarehouseData ;
go 
create schema bronzee;
go
create schema silverr ;
go
create schema goldd ;

 -- creating tables 
 create table bronzee.crm_cust_info(
	 cst_id			int ,
	 cst_key	    nvarchar(50),
	 cst_fname		nvarchar(50),
	 cst_lname		nvarchar(50),
	 cst_material_status nvarchar(50),
	 cst_gender		nvarchar (50),
	 cst_create_date DATE
 )
 go

 CREATE TABLE  bronzee.crm_prd_info (
	prd_id			int,
	prd_key			nvarchar(50),
	prd_nm			nvarchar(50),
	prd_cost		decimal(10,2),
	prd_line		nvarchar(50),
	prd_start_dt	DATETIME,
	prd_end_dt		DATETIME
	)
go
 CREATE TABLE  bronzee.crm_sales_details (
	sls_ord_num		nvarchar(50),
	sls_prd_key		nvarchar(50),
	sls_cust_id		INT,
	sls_order_dt	INT,
	sls_ship_dt		INT,
	sls_due_dt	    INT,
	sls_sales		INT,
	sls_quantity    INT,
	sls_price		INT	
	)


Create Table bronzee.erp_loc_a101(
		cid     nvarchar(50),
		cntry	nvarchar(50)
)
go
Create table bronzee.erp_cust_az12(
		cid  nvarchar(50),
		bdate Date,
		gen  nvarchar(50)
)
go
Create table bronzee.erp_px_car_g1v2(
		id				nvarchar(50),
		cat				nvarchar(50),
		subcat			nvarchar(50),				
		maintenance     nvarchar(50)
)

USE DataWarehouseData;

CREATE OR ALTER PROCEDURE bronzee.LOAD_bronzee AS 

BEGIN
TRUNCATE TABLE bronzee.CRM_CUST_INFO;
BULK INSERT bronzee.CRM_CUST_INFO
FROM 'C:\Users\moham\OneDrive\bronze.crm_cust_info.csv'
WITH(
	FIRSTROW= 2,
	FIELDTERMINATOR = ',',
	TABLOCK );

TRUNCATE TABLE bronzee.CRM_PRD_INFO;
BULK INSERT bronzee.CRM_PRD_INFO
FROM 'C:\Users\moham\OneDrive\bronze.crm_prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);

TRUNCATE TABLE bronzee.CRM_SALES_DETAILS;
BULK INSERT bronzee.CRM_SALES_DETAILS
FROM 'C:\Users\moham\OneDrive\bronze.crm_sales_details.csv'
WITH (
	FIRSTROW = 2 ,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
TRUNCATE TABLE bronzee.ERP_CUST_AZ12;
BULK INSERT bronzee.ERP_CUST_AZ12
FROM  'C:\Users\moham\OneDrive\bronze.erp_cust_az12.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK);

TRUNCATE TABLE bronzee.ERP_LOC_A101;
BULK INSERT bronzee.ERP_LOC_A101
FROM 'C:\Users\moham\OneDrive\bronze.erp_loc_a101.csv'
WITH(
	FIRSTROW= 2,
	FIELDTERMINATOR = ',', 
	TABLOCK
	);
TRUNCATE TABLE bronzee.ERP_PX_CAR_G1V2;
BULK INSERT bronzee.ERP_PX_CAR_G1V2
FROM 'C:\Users\moham\OneDrive\bronze.erp_px_cat_g1v2.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK 
	);

END
