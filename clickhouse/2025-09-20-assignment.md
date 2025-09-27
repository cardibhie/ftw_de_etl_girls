# 🌷 Day 2 – Dimensional Modeling with Chinook
📅 2025-09-27

---

## 1. Ingestion (DLT → RAW layer)
We ingest tables from the Chinook database into ClickHouse.

**Example Pipeline Snippet:**

```
@dlt.resource(write_disposition="append", name="artist_lou_jenner")
def artist():
    """Extract all artists from the Chinook sample DB."""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM artist;")
    for row in cur.fetchall():
        yield dict(row)
    conn.close()

```

**Run Command**
```
docker compose --profile jobs run --rm \
  dlt python extract-loads/02-dlt-chinook-pipeline.py

```
✅ Output: Data is loaded into raw schema, e.g.:
- raw.chinook_lou_jenner___artist_lou_jenner
- raw.chinook_lou_jenner___album_lou_jenner
- raw.chinook_lou_jenner___track_lou_jenner

![raw schema](../assets/ingestion.png)

## 2. Transformation (dbt → CLEAN layer)
We create staging views to standardize, clean, and type-cast raw data.

**Checklist of Cleaned Tables**
- [ ] Albums
- [ ] Artists
- [ ] Tracks
- [ ] Genres
- [ ] Invoices
- [ ] Invoice Line
- [ ] Customers
- [ ] Employees

**Example (Albums):**
```
{{ config(materialized="view", schema="clean", tags=["staging", "chinook"]) }}

SELECT
    MIN(toInt64(album_id)) AS album_id,
    COALESCE(
        initcap(replaceRegexpAll(trim(title), '\\s+', ' ')),
        'Unknown Album'
    ) AS album_title,
    toInt64(artist_id) AS artist_id,
    now() AS cleaned_at
FROM {{ source('raw', 'chinook_lou_jenner___albums_lou_jenner') }}
GROUP BY
    initcap(replaceRegexpAll(trim(title), '\\s+', ' ')),
    artist_id

```

**Run Command**
```
docker compose --profile jobs run --rm   -w /workdir/transforms/02_chinook   dbt build --profiles-dir . --target remote

```

✅ What staging achieves: 
- Deduplication
- Standardized casing (initcap)
- Consistent typing (int, decimal, string)
- Dropping DLT metadata

## 3. Dimensional Model (MART layer → Star Schema)
We design fact and dimension tables.

## Fact Table:
**fact_invoice_line** 
* Grain: one row per invoice line 
* Columns: invoice_line_id, invoice_id, track_id, quantity, price_usd, line_amount

## Dimension Tables:
* dim_track → joins album, artist, genre into one track view
* dim_album
* dim_artist
* dim_genre
* dim_customer (with support_rep_id → employee)
* dim_employee (sales reps, with hierarchy reports_to)
* dim_date (from invoice_date: date_key, year, month, quarter, etc.)

## Relationships:
* Album → Artist
* Track → Album, Genre
* InvoiceLine → Track, Invoice
* Invoice → Customer
* Customer → Employee (support_rep_id)

## 4.Visualization (Metabase)
* Connect to the mart schema.
* Build dashboards: revenue trends, customer segments, employee performance.
* Validate queries match the dimensional model.

## 5. Assignment Queries (Metabase)

**1. Top revenue by genre per country**
```
-- Top Revenue by Genre per Country
DROP TABLE IF EXISTS mart.Group1_TopRevenueByGenreCountry;
CREATE TABLE mart.Group1_TopRevenueByGenreCountry
ENGINE = MergeTree
ORDER BY tuple() AS
SELECT Country, GenreName, TotalRevenue
FROM (
   SELECT
       c.Country AS Country,
       g.name AS GenreName,
       round(SUM(f.LineAmount), 2) AS TotalRevenue,
       ROW_NUMBER() OVER (PARTITION BY c.Country ORDER BY SUM(f.LineAmount) DESC) AS GenreRank
   FROM mart.Group1_FactInvoiceLine AS f
   JOIN mart.Group1_DimCustomer      AS c ON f.CustomerKey = c.CustomerKey
   JOIN mart.Group1_DimTrack         AS t ON f.TrackKey = t.TrackKey
   JOIN mart.Group1_DimGenre         AS g ON t.GenreKey = g.GenreKey
   GROUP BY c.Country, g.name
) AS ranked
WHERE GenreRank = 1
ORDER BY Country;
```

**2. Customer segmentation by spending tier**
```
DROP TABLE IF EXISTS mart.Group1_CustomerSegmentation;
CREATE TABLE mart.Group1_CustomerSegmentation
ENGINE = MergeTree
ORDER BY tuple() AS
SELECT
   CASE
       WHEN TotalSpend > 50 THEN 'High'
       WHEN TotalSpend BETWEEN 20 AND 50 THEN 'Medium'
       ELSE 'Low'
   END AS SpendingTier,
   COUNT(*) AS CustomerCount
FROM (
   SELECT
       c.CustomerKey,
       SUM(f.LineAmount) AS TotalSpend
   FROM mart.Group1_FactInvoiceLine AS f
   JOIN mart.Group1_DimCustomer      AS c ON f.CustomerKey = c.CustomerKey
   GROUP BY c.CustomerKey
) AS spending
GROUP BY SpendingTier;
```

**3. Monthly sales trend** 
```
DROP TABLE IF EXISTS mart.Group1_MonthlyRevenue;
CREATE TABLE mart.Group1_MonthlyRevenue
ENGINE = MergeTree
ORDER BY tuple() AS
SELECT
  toStartOfMonth(d.Date) AS Month,
  SUM(f.LineAmount)     AS MonthlyRevenue
FROM mart.Group1_FactInvoiceLine AS f
JOIN mart.Group1_DimDate          AS d ON f.DateKey = d.DateKey
WHERE d.Date >= addMonths(today(), -24)
GROUP BY Month
ORDER BY Month;
```

**4. Employee sales performance**
```
DROP TABLE IF EXISTS mart.Group1_EmployeeSalesPerformance;
CREATE TABLE mart.Group1_EmployeeSalesPerformance
ENGINE = MergeTree
ORDER BY tuple() AS
SELECT
   e.EmployeeKey  AS EmployeeKey,
   e.first_name   AS FirstName,
   e.last_name    AS LastName,
   d.Year         AS Year,
   d.Quarter      AS Quarter,
   SUM(f.UnitPrice * f.Quantity) AS TotalRevenue,
   RANK() OVER (
       PARTITION BY d.Year, d.Quarter
       ORDER BY SUM(f.UnitPrice * f.Quantity) DESC
   ) AS EmployeeRank
FROM mart.Group1_FactInvoiceLine AS f
JOIN mart.Group1_DimEmployee      AS e ON f.EmployeeKey = e.EmployeeKey
JOIN mart.Group1_DimDate          AS d ON f.DateKey = d.DateKey
GROUP BY
   e.EmployeeKey,
   e.first_name,
   e.last_name,
   d.Year,
   d.Quarter
ORDER BY d.Year, d.Quarter, TotalRevenue DESC;

```

**5.Popular tracks by quantity sold**

```
DROP TABLE IF EXISTS mart.Group1_PopularTracks;
CREATE TABLE mart.Group1_PopularTracks
ENGINE = MergeTree
ORDER BY tuple() AS
SELECT
   t.Name                 AS TrackName,
   SUM(f.Quantity)        AS TotalQuantitySold,
   COUNT(*)               AS NumInvoiceLines,
   arrayStringConcat(groupUniqArray(a.Title), ', ') AS Albums,
   arrayStringConcat(groupUniqArray(ar.name),  ', ') AS Artists
FROM mart.Group1_FactInvoiceLine AS f
JOIN mart.Group1_DimTrack         AS t ON f.TrackKey = t.TrackKey
LEFT JOIN mart.Group1_DimAlbum    AS a ON t.AlbumKey = a.AlbumKey
LEFT JOIN mart.Group1_DimArtist   AS ar ON a.ArtistKey = ar.ArtistKey
GROUP BY t.Name
ORDER BY TotalQuantitySold DESC
LIMIT 20;
```

**6.Regional Pricing Insights**

```
DROP TABLE IF EXISTS mart.Group1_AvgUnitPriceByCountry;
CREATE TABLE mart.Group1_AvgUnitPriceByCountry
ENGINE = MergeTree
ORDER BY tuple() AS
SELECT
 c.Country AS Country,
 uniqExact(c.CustomerKey) AS DistinctCustomers,
 round( sum(f.LineAmount) / nullIf(sum(f.Quantity), 0), 2 ) AS AvgUnitPrice,
 round( sum(f.LineAmount) / nullIf(sum(f.Quantity), 0), 2 ) AS AvgUnitPriceCalc
FROM mart.Group1_FactInvoiceLine AS f
JOIN mart.Group1_DimCustomer      AS c ON f.CustomerKey = c.CustomerKey
GROUP BY c.Country
ORDER BY AvgUnitPrice DESC
LIMIT 50;
```

```
DROP TABLE IF EXISTS mart.Group1_AvgUnitPriceByTrackCountry;
CREATE TABLE mart.Group1_AvgUnitPriceByTrackCountry
ENGINE = MergeTree
ORDER BY tuple() AS
SELECT
 c.Country AS Country,
 t.Name    AS Track,
 round( sum(f.LineAmount) / nullIf(sum(f.Quantity), 0), 2 ) AS AvgPrice
FROM mart.Group1_FactInvoiceLine AS f
JOIN mart.Group1_DimCustomer      AS c ON f.CustomerKey = c.CustomerKey
JOIN mart.Group1_DimTrack         AS t ON f.TrackKey = t.TrackKey
GROUP BY c.Country, t.Name
ORDER BY c.Country, AvgPrice DESC;
```

## TEAM PROCESS 
![team process](../assets/team_process.png)




