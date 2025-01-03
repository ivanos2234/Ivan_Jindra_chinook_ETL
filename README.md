# **ETL proces datasetu Chinook**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z **Chinook** datasetu. Projekt sa zameriava na preskúmanie popularity rôznej hudby naprieč zákazníkom, na základe počtu predaných kusov jednotlivých skladieb a demografiky zákazníkov

---
## **1. Úvod a popis zdrojových dát**
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa eshopu s hudbou. umožńi nám to analyzovať hudobné trendy naprieč krajinám.

Zdrojové dáta pochádzajú z Chinook datasetu dostupného [tu](https://www.kaggle.com/datasets/saurabhbagchi/books-dataset). Dataset obsahuje šesť hlavných tabuliek a päť doplňujúcich tabuliek :
Hlavné:
- `Track`
- `Invoice`
- `InvoiceLine`
- `Customer`
- `Employee`
- `Playlist`

Vedľajšie:
- `PlaylistTrack` (spája track a playlist)
- `Album`
- `Artist`
- `Mediatype`
- `Genre` (Tieto štyri tabuľky sú všetky napojené k tabuľke track a podrobnejšie ju vysvetľujú)

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/ivanos2234/Ivan_Jindra_chinook_ETL/blob/main/erd_schema.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma Chinook</em>
</p>

---
## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_ratings`**, ktorá je prepojená s nasledujúcimi dimenziami:
- **`DIM_track`**: Obsahuje podrobné informácie o skladbách (Skľadateľ a Autor, Album, Meno skladby, Žáner...).
- **`DIM_customer`**: Obsahuje údaje o geografickej pohole zákazníkov, tak isto aj ID zamestnanca ktorý daného zákazníka obsluhoval.
- **`DIM_date`**: Zahrňuje informácie o dátumoch jednotlivých nákupov.

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="https://github.com/ivanos2234/Ivan_Jindra_chinook_ETL/blob/main/star_schema.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre Chinook</em>
</p>

---
## **3. ETL proces v Snowflake**
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom **MAGPIE_chinook_stage**. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

#### Kód:
```sql
CREATE OR REPLACE STAGE MAGPIE_chinook_stage;
```
Do stage boli následne nahraté súbory obsahujúce údaje o skladbách, jednotlivých transakciach, zákazníkoch a zamestnancoch. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz. Najskôr bol ale ešte vytvorený `FILE_FORMAT` s naźvom **MYPIPEFORMAT** ktorý bol potom použitý pri importovaní všetkých dát:


```sql
CREATE OR REPLACE FILE FORMAT MYPIPEFORMAT
  TYPE = CSV
  COMPRESSION = NONE
  FIELD_DELIMITER = ','
  FILE_EXTENSION = 'csv'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1;

COPY INTO album
FROM @MAGPIE_chinook_stage
FILES = ('album.csv')
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
ON_ERROR = CONTINUE;
```

V prípade nekonzistentných záznamov bol použitý parameter `ON_ERROR = 'CONTINUE'`, ktorý zabezpečil pokračovanie procesu bez prerušenia pri chybách. Chybné záznamy boli neskôr manuálne vložené do tuabuliek.

---
### **3.1 Transform (Transformácia dát)**

V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

Dimenzie boli navrhnuté na poskytovanie kontextu pre faktovú tabuľku. `dim_date` obsahuje jednotlivé dátumy v ktorých sa uskutočńili transakcie, vďaka tejto dimenzií je možné robiť podrobnejšie časové analýzy, ako napríklad, počet transkacií v závislosti od dní, mesiacov, rokov, tržby v jednotlivých rokoch... Je typu SCD 0, informácie v nej nemá význam meniť čiže sú nemenné.

```sql
CREATE OR REPLACE TABLE dim_date AS
SELECT
    CAST(InvoiceDate AS DATE) AS dim_dateID,
    DATE_PART(day, InvoiceDate) AS Day,
    CASE DATE_PART(dow, InvoiceDate) + 1
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS dayOfWeekAsString,
    DATE_PART(month, InvoiceDate) AS Month,
    CASE DATE_PART(month, InvoiceDate)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS MonthAsString,
    DATE_PART(year, InvoiceDate) AS Year
FROM (SELECT DISTINCT InvoiceDate FROM invoice) AS distinct_dates
ORDER BY dim_dateID;
```
Dimenzia `dim_customers` uchováva relevantné údaje o zákaznikoch, tak ako aj údaj o tom pod ktorého jednotlivý zákazník spadá. Z hľadiska typu to je SCD dimenzia typu 1, pre jedného zákazníka vie evidovať len najnovší záznam a môže podliehať zmenám.

```sql
CREATE OR REPLACE TABLE dim_customer AS
SELECT
    c.customerid AS dim_customerID,
    c.city AS city,
    c.state AS state,
    c.country AS country,
    c.supportrepid AS SupportRep_ID
FROM customer c
ORDER BY c.customerid;
```
Podobne `dim_track` obsahuje údaje o skladbách ako sú názov, album z ktorého skladba je, autor, typ úložiska, žáner. Dimenzia je typu SCD 0, lebo informácie o skladbách sú nemenné.

```sql
CREATE OR REPLACE TABLE dim_track AS
SELECT
    t.trackid AS dim_trackID,
    t.name AS name,
    al.title AS album,
    at.name AS artist,
    m.name AS mediatype,
    g.name AS genre,
    t.composer AS composer,
    t.milliseconds AS milliseconds
FROM track t
JOIN album al ON t.albumID = al.albumID
JOIN artist at ON al.artistID = at.artistID
JOIN mediatype m ON t.mediatypeID = m.mediatypeID
JOIN genre g ON t.genreID = g.genreID
ORDER BY dim_trackID;
```
Faktová tabuľka `fact_invoiceLine` obsahuje záznamy o jednotlivých transakciach a kľúčové metriky ako časový údaj kedy k transkacii došlo, hodnota transakcie a prepojenie na všetky dimenzie.
```sql
CREATE OR REPLACE TABLE fact_invoiceLine AS
SELECT
    il.invoicelineID AS fact_invoicelineID,
    CAST(i.invoicedate AS DATE) AS dim_dateID,
    il.trackid AS dim_trackID,
    i.customerid AS dim_customerID,
    i.invoiceID AS invoiceID, // since i decided to drop the invoice, i am adding only this single attribute that will allow me to calculate profits of each invoice
    il.unitprice AS unitPrice,
    il.quantity AS quantity,
    i.total AS totalPrice
FROM invoiceline il
JOIN invoice i ON il.invoiceID = i.invoiceID;
```

---
### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:
```sql
DROP TABLE IF EXISTS album;
DROP TABLE IF EXISTS artist;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS employee;
DROP TABLE IF EXISTS genre;
DROP TABLE IF EXISTS invoice;
DROP TABLE IF EXISTS invoiceline;
DROP TABLE IF EXISTS mediatype;
DROP TABLE IF EXISTS playlist;
DROP TABLE IF EXISTS playlisttrack;
DROP TABLE IF EXISTS track;
```
ETL proces v Snowflake umožnil spracovanie pôvodných dát z `.csv` formátu do viacdimenzionálneho modelu typu hviezda. Tento proces zahŕňal čistenie a reorganizáciu údajov. Výsledný model umožnuje analyzovať najmä preferencie zákazníkov, čiže to aké skladby sú najpredávanejšie, aké žánre akí interpreti, pričom poskytuje základy pre visualizáciu a reporty.

---
## **4 Vizualizácia dát**

Dashboard obsahuje `6 vizualizácií`, ktoré poskytujú základný prehľad o kľúčových metrikách a Trendoch týkajúcich sa skladieb, zákazníkov a jednotlivých transkacií, vďaka týmto visualizáciam bude možné optimalizovať preferencie na základe napríklad krajiny z ktorej pochádza zákazník. Umožňujú lepšie pochopiť správanie zákazníkov a ich preferencie.

<p align="center">
  <img src="https://github.com/ivanos2234/Ivan_Jindra_chinook_ETL/blob/main/Chinook_visualizations_1.png" alt="ERD Schema">
  <br>
  <em>Obrázok 3 Dashboard 1 Chinook datasetu</em>
</p>
<p align="center">
  <img src="https://github.com/ivanos2234/Ivan_Jindra_chinook_ETL/blob/main/Chinook_visualizations_2.png" alt="ERD Schema">
  <br>
  <em>Obrázok 3 Dashboard 2 Chinook datasetu</em>
</p>

---
### **Graf 1: Top 10 najviac aktívnich krajín**
Vďaka tejto visualizácii vieme z ktorých krajín si zákazníci najčastejšie objednávajú skladby. Je vidno že Kanada a USA sú ďaleko najaktívnejšie krajiny, vďaka týmto dátam viem na aké krajiny by sa mala spoločnosť zamerať v prípadných marketingových kampaní.

```sql
SELECT 
    c.country AS country, 
    SUM(il.unitprice) AS total_sales 
FROM fact_invoiceline il
JOIN dim_customer c ON c.dim_customerid = il.dim_customerid
GROUP BY c.country
ORDER BY total_sales DESC
LIMIT 10;
```
---
### **Graf 2: Najpredávanejšie žánre (TOP 5)**
Graf ukazuje päť žánrov ktoré sa celé obdobie pôsobenia vyniesli najviac ziskov. Z vizualizácie je vidieť že žáner `Rock` má dvojnásobne väčšie zisky než druhý žáner v poradí. Tieto dáta by sa dali využiť v prípadnom rozširovaní obchodu o nové skladby.

```sql
SELECT 
    t.genre AS genre,
    COUNT(il.fact_invoicelineid) AS units_sold
FROM fact_invoiceline il
JOIN dim_track t ON t.dim_trackID = il.dim_trackid
GROUP BY t.genre
ORDER BY units_sold DESC
LIMIT 5;
```
---
### **Graf 3: Efektivita Sales Rep zamestnancov podľa mesiacov**
Z grafu vidíme v ktorých mesiacoch sú jednotlivý zamestanci ako aktívny. Je vidno že napríklad v januároch má zamestannec 5 dvakrát toľko predajov ako zamestnanec 3. Údaje by mohli byť efektívne pri rozdeľovaní práce, poprípade pri vyhodnocovaní zamestnancov mesiaca.

```sql
SELECT
    c.supportrep_id AS supportrep_id,
    COUNT(c.dim_customerid) AS amount_of_units_sold,
    d.month AS month
FROM fact_invoiceline il
JOIN dim_customer c ON c.dim_customerid = il.dim_customerid
JOIN dim_date d ON d.dim_dateid = il.dim_dateid
GROUP BY c.supportrep_id, d.month
ORDER BY d.month;
```
---
### **Graf 4: Popularita Rock žánru počas rokov**
Z dát je možné vyčítať popularitu (počet predaných kusov skladiem Rock žánru) počas rokov pôsobenia. Vďaka týmto grafom je možné zistiť popularitu daných žánrov, z tohoh grafu je možné vyčítať že Rock sa predáva stále, bez zvláštneho ohľadu na obdobie. Dalo by sa použiť na zistenie ideálneho času rôznych marketingových akcií.

```sql
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
```
---
### **Graf 5: Porovnanie dvoch žánrov podľa popularity počas rokov**
Tento graf poskytuje informácie o počtu predaných kusov dvoch rôznych žánrov. Opäť je pri oboch žánroch zaznamenaná stabilnosť, s vzrastami v určitej dobe. Bolo by na zamyslenie pozrieť sa či sa v daný čas nekonala nejaká akcia ako napríklad koncert alebo festival, a do budúcna navrhnúť marketingové akcie s týmyto akciami.

```sql
SELECT 
    t.genre AS genre,
    d.dim_dateid AS date,
    SUM(il.UnitPrice) AS total_sales
FROM fact_invoiceLine il
JOIN dim_track t ON t.DIM_trackID = il.DIM_trackID
JOIN dim_date d ON d.DIM_dateID = il.DIM_dateID
WHERE t.genre IN ('Metal', 'Latin')
GROUP BY t.genre, d.dim_dateid;
```
---
### **Graf 6: Najpopulárnejší Interperty podľa krajiny**
Na tomto grafe vidím najpopulárnejšie Kapely/Autorov v daných krajinách aj s výnosom ktorý z danej krajiny mali. Na základe tohoto grafu by bolo možné robiť reklamy zvlášť podľa jednotlivých krajín a tým získať nových zákazníkov, poprípade pridať viacej skladieb od daných Autorov.

```sql
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

```
Tieto vizualizácie nám dávajú prehľadnejší pohľad na inak nepriehľadné dáta, a môžu byť použité na optimalizáciu marketingových stratégii a systémov pridávania novej hudby.

---

**Autor:** IVAN JINDRA
