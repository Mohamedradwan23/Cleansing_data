use DataWarehouseData ;
go 
-------------------------------------
create table silverr.crm_cust_info(
	 cst_id			int ,
	 cst_key	    nvarchar(50),
	 cst_fname		nvarchar(50),
	 cst_lname		nvarchar(50),
	 cst_material_status nvarchar(50),
	 cst_gender		nvarchar (50),
	 cst_create_date DATE
 )
insert into silverr.crm_cust_info (
	 cst_id	,
	 cst_key,
	 cst_fname,
	 cst_lname,
	 cst_create_date ,
	 cst_gender	,
	 cst_material_status)
	SELECT cst_id, 
	   cst_key,
	   trim(cst_fname) as cst_fname,
	   trim(cst_lname) as cst_lname, 
	   cst_create_date,
	case
		when upper(trim(cst_gender)) ='M' then 'male'
		when upper(trim(cst_gender))= 'F' then  'female'
		else 'not available'
	end as cst_gender,
	case
		when upper(trim(cst_material_status)) = 'M' then 'married'
		when upper(trim(cst_material_status)) = 'S' then 'single'
		else 'not available'
	end as cst_material_status
FROM(
SELECT 
*,
ROW_NUMBER()OVER(PARTITION BY CST_ID ORDER BY CST_CREATE_DATE desc) AS RANKK 
FROM bronzee.crm_cust_info)t
WHERE CST_ID  is not null;

select * from silverr.crm_cust_info;

-------------------------------------
-- PRD CLEANSING PROCESS :-

 select prd_id ,
 count(*) as dup_num
 from bronzee.crm_prd_info
 group by prd_id
 having count(*) > 1 ; -- there is no duplicates

 SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE
        WHEN UPPER(TRIM(PRD_LINE)) = 'M' THEN 'MOUNTAIN'
        WHEN UPPER(TRIM(PRD_LINE)) = 'R' THEN 'ROAD'
        WHEN UPPER(TRIM(PRD_LINE)) = 'S' THEN 'OTHER SALES'
        WHEN UPPER(TRIM(PRD_LINE)) = 'T' THEN 'TOURING'
        ELSE 'N/V'
    END AS PRD_LINE,
    cast(prd_start_dt as date) as prd_start_date,
    cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1  as date) as prd_end_dt
FROM bronzee.crm_prd_info;


 select * 
 from bronzee.crm_prd_info ;

 -- Creating a silver layer of prd_info
 if OBJECT_ID ('silverr.crm_prd_info','U') is not null
	drop table silverr.crm_prd_info ;
 
 CREATE TABLE  silverr.crm_prd_info (
	prd_id			int,
	cat_id			nvarchar(50),
	prd_key			nvarchar(50),
	prd_nm			nvarchar(50),
	prd_cost		decimal(10,2),
	prd_line		nvarchar(50),
	prd_start_dt	DATE,
	prd_end_dt		DATE
	)
-- inserting what we clean :-
INSERT INTO silverr.crm_prd_info (
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
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE
        WHEN UPPER(TRIM(PRD_LINE)) = 'M' THEN 'MOUNTAIN'
        WHEN UPPER(TRIM(PRD_LINE)) = 'R' THEN 'ROAD'
        WHEN UPPER(TRIM(PRD_LINE)) = 'S' THEN 'OTHER SALES'
        WHEN UPPER(TRIM(PRD_LINE)) = 'T' THEN 'TOURING'
        ELSE 'N/V'
    END AS PRD_LINE,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM bronzee.crm_prd_info;

-------------------------------------
select * from bronzee.crm_sales_details ;
--
 if OBJECT_ID ('silverr.crm_sales_details','U') is not null
	drop table silverr.crm_sales_details ;
CREATE TABLE  silverr.crm_sales_details (
	sls_ord_num		nvarchar(50),
	sls_prd_key		nvarchar(50),
	sls_cust_id		INT,
	sls_order_dt	date,
	sls_ship_dt		date,
	sls_due_dt	    date,
	sls_sales		INT,
	sls_quantity    INT,
	sls_price		INT	
	)



insert into silverr.crm_sales_details (
sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)	
select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		When sls_order_dt = 0 or len(sls_order_dt) != 8 then null 
		else cast(cast(sls_order_dt as varchar) as date)
		end as sls_order_dt ,
	CASE 
		When sls_order_dt = 0 or len(sls_ship_dt) != 8 then null 
		else cast(cast(sls_ship_dt as varchar) as date)
		end as sls_ship_dt ,
	CASE 
		When sls_due_dt = 0 or len(sls_due_dt) != 8 then null 
		else cast(cast(sls_due_dt as varchar) as date)
		end as sls_due_dt ,
	CASE
		When sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
		then sls_quantity * abs(sls_price) 
		else sls_sales end as sls_sales,
	sls_quantity,
	CASE 
		when sls_price is null or sls_price <= 0 then sls_sales / nullif(sls_quantity ,0)
		else sls_price end as sls_price

from bronzee.crm_sales_details


select * from silverr.crm_sales_details;
-------------------------------------------------
if OBJECT_ID ('silverr.erp_cust_az12','U') is not null
	drop table silverr.erp_cust_az12 ;

Create table silverr.erp_cust_az12(
		cid  nvarchar(50),
		bdate Date,
		gen  nvarchar(50)
)




insert into silverr.erp_cust_az12(cid,bdate,gen)
select 
case
	when cid like 'NAS%' then SUBSTRING(cid,4 ,len(cid))
	else cid end as cid,
case 
	when bdate > GETDATE() then null
	else bdate end as bdate,
case
	when UPPER(trim(gen)) in ('male','m') then 'male'
	when upper(trim(gen)) in ('female','f') then 'female'
	else 'n/v'
	end as gen

from bronzee.erp_cust_az12;
----------------------------------------------


if OBJECT_ID ('silverr.erp_loc_a101','U') is not null
	drop table silverr.erp_loc_a101 ;

Create Table silverr.erp_loc_a101(
		cid     nvarchar(50),
		cntry	nvarchar(50)
)
go
insert into silverr.erp_loc_a101 ( cid, cntry)
select 
	 REPLACE(cid, '-' ,''),
	 case 
		when upper(trim(cntry)) in ('DE' , 'Germany') then 'Germany'
		when upper(trim(cntry)) in ('US' , 'Usa') then 'United_state'
		when trim(cntry) = '' or cntry is  null then 'n/v'
		else trim(cntry) end as cntry
from bronzee.erp_loc_a101 ;
----------------------------------------
if OBJECT_ID ('silverr.erp_px_car_g1v2','U') is not null
	drop table silverr.erp_px_car_g1v2 ;

Create table silverr.erp_px_car_g1v2(
		id				nvarchar(50),
		cat				nvarchar(50),
		subcat			nvarchar(50),				
		maintenance     nvarchar(50)
)
go

insert into  silverr.erp_px_car_g1v2(id,cat,subcat,maintenance)
select 
id,
cat,
subcat,
maintenance
from bronzee.erp_px_car_g1v2 ;