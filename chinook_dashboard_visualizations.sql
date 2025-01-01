// 1, SALES BY COUNTRIES TOP 10
SELECT 
    c.country AS country, 
    SUM(il.unitprice) AS total_sales 
FROM fact_invoiceline il
JOIN dim_customer c ON c.dim_customerid = il.dim_customerid
GROUP BY c.country
ORDER BY total_sales DESC
LIMIT 10;

// 2. MOST POPULAR SELLING GENRES
SELECT 
    t.genre AS genre,
    COUNT(il.fact_invoicelineid) AS units_sold
FROM fact_invoiceline il
JOIN dim_track t ON t.dim_trackID = il.dim_trackid
GROUP BY t.genre
ORDER BY units_sold DESC
LIMIT 5;

// 3. Sales Rep Units sold by month
SELECT
    c.supportrep_id AS supportrep_id,
    COUNT(c.dim_customerid) AS amount_of_units_sold,
    d.month AS month
FROM fact_invoiceline il
JOIN dim_customer c ON c.dim_customerid = il.dim_customerid
JOIN dim_date d ON d.dim_dateid = il.dim_dateid
GROUP BY c.supportrep_id, d.month
ORDER BY d.month;

// 4. Sales of Rock genre over the years
SELECT 
    d.Year,
    d.Month,
    SUM(il.UnitPrice) AS total_sales
FROM fact_invoiceLine il
JOIN dim_track t ON t.DIM_trackID = il.DIM_trackID
JOIN dim_date d ON d.DIM_dateID = il.DIM_dateID
WHERE t.Genre LIKE 'Rock'
GROUP BY d.Year, d.Month
ORDER BY d.Year, d.Month;

// 5. Latin vs Metal sales by years 
SELECT 
    t.genre AS genre,
    d.dim_dateid AS date,
    SUM(il.UnitPrice) AS total_sales
FROM fact_invoiceLine il
JOIN dim_track t ON t.DIM_trackID = il.DIM_trackID
JOIN dim_date d ON d.DIM_dateID = il.DIM_dateID
WHERE t.genre IN ('Metal', 'Latin')
GROUP BY t.genre, d.dim_dateid;

// 6. Most selling Artist by country
SELECT 
    x.country,
    x.artist,
    x.total_sales
FROM (
    SELECT 
        c.Country AS country,
        t.Artist AS artist,
        SUM(il.UnitPrice) AS total_sales,
        ROW_NUMBER() OVER (PARTITION BY c.Country ORDER BY SUM(il.UnitPrice) DESC) AS rank
    FROM fact_invoiceLine il
    JOIN dim_customer c ON c.DIM_customerID = il.DIM_customerID
    JOIN dim_track t ON t.DIM_trackID = il.DIM_trackID
    GROUP BY c.Country, t.Artist
) x
WHERE rank = 1
ORDER BY total_sales DESC;