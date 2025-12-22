/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/
-- Master Source of Data is CRM 


-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,	-- surrogate key
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr			-- CRM is the master source 
		ELSE COALESCE(ca.gen,'n/a')							-- Fall back to ERP data
	END AS gender,
	ca.bdate AS birthdate,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	ci.cst_create_date AS created_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
	ON ci.cst_key = la.cid;
GO


-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
-- Dimension table
SELECT
	ROW_NUMBER() OVER (ORDER BY pdi.prd_start_dt, pdi.prd_key) AS product_key, -- surrogate key
	pdi.prd_id AS product_id,
	pdi.prd_key AS product_number,
	pdi.prd_nm AS product_name,
	pdi.cat_id AS category_id,
	pxc.cat AS category,
	pxc.subcat AS sub_category,
	pxc.maintenance,
	pdi.prd_cost AS cost,
	pdi.prd_line AS product_line,
	pdi.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pdi
LEFT JOIN silver.erp_px_cat_g1v2 AS pxc
	ON pdi.cat_id = pxc.id
WHERE prd_end_dt IS NULL;	-- filter out all historical data, keeping only the newest version of each product
GO


-- =============================================================================
-- Create Dimension: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
	-- dimension keys
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	-- dates
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	-- measures
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
	ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
	ON sd.sls_cust_id = cu.customer_id;
GO
