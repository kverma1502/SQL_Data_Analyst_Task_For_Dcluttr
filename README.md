--Using MS-SQL Server (SSMS tool)

--TASK_1:
create database dcluttr_task

BULK INSERT staging_blinkit
FROM 'D:\Dcluttr_task\all_blinkit_city_map.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\r\n'
);

BULK INSERT staging_blinkit
FROM 'D:\Dcluttr_task\all_blinkit_categories.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\r\n'
);


BULK INSERT staging_blinkit
FROM 'D:\Dcluttr_task\all_blinkit_category_scraping_stream.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\r\n'
);


--#TASK_2:
--Creating a required table

CREATE TABLE blinkit_city_insights (
    date                DATE,
    sku_id              INT,
    city_name           VARCHAR(100),
    brand_id            INT,
    brand               VARCHAR(100),
    image_url           VARCHAR(MAX),
    sku_name            VARCHAR(255),
    category_id         INT,
    category_name       VARCHAR(100),
    sub_category_id     INT,
    sub_category_name   VARCHAR(100),
    est_qty_sold        INT,
    est_sales_sp        DECIMAL(18,2),
    est_sales_mrp       DECIMAL(18,2),
    listed_ds_count     INT,
    ds_count            INT,
    wt_osa              FLOAT,
    wt_osa_ls           FLOAT,
    mrp                 DECIMAL(10,2),
    sp                  DECIMAL(10,2),
    discount            FLOAT,
    PRIMARY KEY (date, sku_id, city_name)
);


-- Step 1: CTE to calculate inventory movement

WITH inv_movement AS (
    SELECT
        s.store_id,
        s.sku_id,
        cm.city_name,
        s.created_at AS curr_time,
        s.inventory AS current_inventory,
        LEAD(s.inventory) OVER (PARTITION BY s.sku_id, s.store_id ORDER BY s.created_at) AS next_inventory,
        LEAD(s.created_at) OVER (PARTITION BY s.sku_id, s.store_id ORDER BY s.created_at) AS next_time,
        DATEDIFF(HOUR, s.created_at, LEAD(s.created_at) OVER (PARTITION BY s.sku_id, s.store_id ORDER BY s.created_at)) AS hour_diff,
        c.l1_category_id AS category_id,
        c.l1_category AS category_name,
        c.l2_category_id AS sub_category_id,
        c.l2_categor AS sub_category_name,
        s.sku_name,
        s.brand,
        s.brand_id,
        s.image_url,
        s.selling_price,
        s.mrp
    FROM all_blinkit_category_scraping_stream s
    JOIN blinkit_categories c ON s.l2_category_id = c.l2_category_id
    JOIN blinkit_city_map cm ON s.store_id = cm.store_id
),
estimated_sales AS (
    SELECT
        *,
        CASE 
            WHEN next_inventory IS NULL THEN 0
            WHEN current_inventory > next_inventory THEN current_inventory - next_inventory
            ELSE 0
        END AS est_sold_units,
        CAST(curr_time AS DATE) AS date
    FROM inv_movement
),

-- Step 2: Aggregate estimated sales per sku-city-date

sku_city_daily_sales AS (
    SELECT
        date,
        sku_id,
        city_name,
        MIN(sku_name) AS sku_name,
        MIN(brand_id) AS brand_id,
        MIN(brand) AS brand,
        MIN(image_url) AS image_url,
        MIN(category_id) AS category_id,
        MIN(category_name) AS category_name,
        MIN(sub_category_id) AS sub_category_id,
        MIN(sub_category_name) AS sub_category_name,
        SUM(est_sold_units) AS est_qty_sold
    FROM estimated_sales
    GROUP BY date, sku_id, city_name
),

-- Step 3: Mode logic for selling_price

price_mode AS (
    SELECT
        date,
        sku_id,
        city_name,
        selling_price AS sp,
        RANK() OVER (PARTITION BY date, sku_id, city_name ORDER BY COUNT(*) DESC) AS rnk
    FROM estimated_sales
    GROUP BY date, sku_id, city_name, selling_price
),
mrp_mode AS (
    SELECT
        date,
        sku_id,
        city_name,
        mrp,
        RANK() OVER (PARTITION BY date, sku_id, city_name ORDER BY COUNT(*) DESC) AS rnk
    FROM estimated_sales
    GROUP BY date, sku_id, city_name, mrp
),

-- Step 4: Dark store count info

listed_ds_count_cte AS (
    SELECT 
        CAST(s.created_at AS DATE) AS date,
        s.sku_id,
        cm.city_name,
        COUNT(DISTINCT s.store_id) AS listed_ds_count
    FROM all_blinkit_category_scraping_stream s
    JOIN blinkit_city_map cm ON s.store_id = cm.store_id
    GROUP BY CAST(s.created_at AS DATE), s.sku_id, cm.city_name
),
ds_count_cte AS (
    SELECT COUNT(DISTINCT store_id) AS ds_count
    FROM all_blinkit_category_scraping_stream
),
in_stock_cte AS (
    SELECT
        CAST(s.created_at AS DATE) AS date,
        s.sku_id,
        cm.city_name,
        COUNT(DISTINCT CASE WHEN s.inventory > 0 THEN s.store_id END) AS in_stock_store_count
    FROM all_blinkit_category_scraping_stream s
    JOIN blinkit_city_map cm ON s.store_id = cm.store_id
    GROUP BY CAST(s.created_at AS DATE), s.sku_id, cm.city_name
)

-- Step 5: Final Insert

INSERT INTO blinkit_city_insights (
    date, sku_id, city_name, brand_id, brand, image_url, sku_name,
    category_id, category_name, sub_category_id, sub_category_name,
    est_qty_sold, est_sales_sp, est_sales_mrp,
    listed_ds_count, ds_count, wt_osa, wt_osa_ls,
    mrp, sp, discount
)
SELECT
    s.date,
    s.sku_id,
    s.city_name,
    s.brand_id,
    s.brand,
    s.image_url,
    s.sku_name,
    s.category_id,
    s.category_name,
    s.sub_category_id,
    s.sub_category_name,
    
    s.est_qty_sold,
    s.est_qty_sold * ISNULL(p.sp, 0) AS est_sales_sp,
    s.est_qty_sold * ISNULL(m.mrp, 0) AS est_sales_mrp,

    ISNULL(l.listed_ds_count, 0),
    d.ds_count,
    CAST(1.0 * ISNULL(i.in_stock_store_count, 0) / NULLIF(d.ds_count, 0) AS FLOAT) AS wt_osa,
    CAST(1.0 * ISNULL(i.in_stock_store_count, 0) / NULLIF(l.listed_ds_count, 0) AS FLOAT) AS wt_osa_ls,

    m.mrp,
    p.sp,
    CAST(1.0 * (ISNULL(m.mrp, 0) - ISNULL(p.sp, 0)) / NULLIF(m.mrp, 0) AS FLOAT) AS discount
FROM sku_city_daily_sales s
LEFT JOIN price_mode p ON s.date = p.date AND s.sku_id = p.sku_id AND s.city_name = p.city_name AND p.rnk = 1
LEFT JOIN mrp_mode m ON s.date = m.date AND s.sku_id = m.sku_id AND s.city_name = m.city_name AND m.rnk = 1
LEFT JOIN listed_ds_count_cte l ON s.date = l.date AND s.sku_id = l.sku_id AND s.city_name = l.city_name
LEFT JOIN in_stock_cte i ON s.date = i.date AND s.sku_id = i.sku_id AND s.city_name = i.city_name
CROSS JOIN ds_count_cte d;

/*
SHORT EXPLANATION:
LEAD() fetches the next row's inventory and created_at values (for the same sku_id and store_id ordered by time), enabling comparison across time slots.
Sales Calculation Logic: If next_inventory < current_inventory, the difference is treated as est_sold_units (i.e., estimated quantity sold in that time interval).
*/

select * from blinkit_city_insights;
