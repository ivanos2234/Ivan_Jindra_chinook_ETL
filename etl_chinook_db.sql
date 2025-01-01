-- Vytvorenie databázy
CREATE OR REPLACE DATABASE MAGPIE_CHINOOK_DB;

-- Vytvorenie schémy pre staging tabuľky
CREATE OR REPLACE SCHEMA MAGPIE_CHINOOK_DB.staging;

CREATE OR REPLACE STAGE MAGPIE_chinook_stage;

-- Vytvorenie File Formatu pre csv súbory
CREATE OR REPLACE FILE FORMAT MYPIPEFORMAT
  TYPE = CSV
  COMPRESSION = NONE
  FIELD_DELIMITER = ','
  FILE_EXTENSION = 'csv'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1;

CREATE or REPLACE TABLE album (
    AlbumID INT,
    Title STRING,
    ArtistID INT
);

CREATE or REPLACE TABLE artist (
    ArtistID INT,
    Name STRING
);

CREATE or REPLACE TABLE customer (
    CustomerId INT,
    FirstName STRING,
    LastName STRING,
    Company STRING,
    Address STRING,
    City STRING,
    State STRING,
    Country STRING,
    PostalCode STRING,
    Phone STRING,
    Fax STRING,
    Email STRING,
    SupportRepId INT
);

CREATE or REPLACE TABLE employee (
    EmployeeId INT,
    LastName STRING,
    FirstName STRING,
    Title STRING,
    ReportsTo STRING,
    BirthDate DATE,
    HireDate DATE,
    Address STRING,
    City STRING,
    State STRING,
    Country STRING,
    PostalCode STRING,
    Phone STRING,
    Fax STRING,
    Email STRING
);

CREATE or REPLACE TABLE genre (
    GenreID INT,
    Name STRING
);

CREATE or REPLACE TABLE invoice (
    InvoiceId INT,
    CustomerId INT,
    InvoiceDate DATE,
    BillingAddress STRING,
    BillingCity STRING,
    BillingState STRING,
    BillingCountry STRING,
    BillingPostalCode STRING,
    Total DECIMAL(10,2)
);

CREATE or REPLACE TABLE invoiceline (
    InvoiceLineId INT,
    InvoiceId INT,
    TrackId INT,
    UnitPrice DECIMAL(10,2),
    Quantity INT
);

CREATE or REPLACE TABLE mediatype (
    MediaTypeId INT,
    Name STRING
);

CREATE or REPLACE TABLE playlist (
    PlaylistId INT,
    Name STRING
);

CREATE or REPLACE TABLE playlisttrack (
    PlaylistId INT,
    TrackId INT
);

CREATE or REPLACE TABLE track (
    TrackId INT,
    Name STRING,
    AlbumId INT,
    MediaTypeId INT,
    GenreId INT,
    Composer STRING,
    Milliseconds INT,
    Bytes INT,
    UnitPrice DECIMAL(10,2)
);

COPY INTO album
FROM @MAGPIE_chinook_stage
FILES = ('album.csv')
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
ON_ERROR = CONTINUE;

COPY INTO artist
FROM @MAGPIE_chinook_stage
FILES = ('artist.csv')
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
ON_ERROR = CONTINUE;

COPY INTO customer
FROM @MAGPIE_chinook_stage
FILES = ('customer.csv')
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
ON_ERROR = CONTINUE;

COPY INTO employee
FROM @MAGPIE_chinook_stage
FILES = ('employee.csv')
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
ON_ERROR = CONTINUE;

COPY INTO genre
FROM @MAGPIE_chinook_stage
FILES = ('genre.csv')
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
ON_ERROR = CONTINUE;

COPY INTO invoice
FROM @MAGPIE_chinook_stage
FILES = ('invoice.csv')
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
ON_ERROR = CONTINUE;

COPY INTO invoiceline
FROM @MAGPIE_chinook_stage
FILES = ('invoiceline.csv')
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
ON_ERROR = CONTINUE;


COPY INTO mediatype
FROM @MAGPIE_chinook_stage
FILES = ('mediatype.csv')
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
ON_ERROR = CONTINUE;

COPY INTO playlist
FROM @MAGPIE_chinook_stage
FILES = ('playlist.csv')
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
ON_ERROR = CONTINUE;


COPY INTO playlisttrack
FROM @MAGPIE_chinook_stage
FILES = ('playlisttrack.csv')
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
ON_ERROR = CONTINUE;


COPY INTO track
FROM @MAGPIE_chinook_stage
FILES = ('track_fixed.csv')
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
ON_ERROR = CONTINUE;

// inserting the incosistent values manually here

// 3412
INSERT INTO track (
    TrackId,
    Name,
    AlbumId,
    MediaTypeId,
    GenreId,
    Composer,
    Milliseconds,
    Bytes,
    UnitPrice
)
VALUES (
    3412,
    '"Eine Kleine Nachtmusik" Serenade In G, K. 525: I. Allegro',
    281,
    2,
    24,
    'Wolfgang Amadeus Mozart',
    348971,
    5760129,
    0.99
);

// 3417
INSERT INTO track (
    TrackId,
    Name,
    AlbumId,
    MediaTypeId,
    GenreId,
    Composer,
    Milliseconds,
    Bytes,
    UnitPrice
)
VALUES (
    3417,
    'Nabucco: Chorus, "Va, Pensiero, Sull\'ali Dorate"',
    286,
    2,
    24,
    'Giuseppe Verdi',
    274504,
    4498583,
    0.99
);

// 3431
INSERT INTO track (
    TrackId,
    Name,
    AlbumId,
    MediaTypeId,
    GenreId,
    Composer,
    Milliseconds,
    Bytes,
    UnitPrice
)
VALUES (
    3431,
    'Symphony No.1 in D Major, Op.25 "Classical", Allegro Con Brio',
    298,
    2,
    24,
    'Sergei Prokofiev',
    254001,
    4195542,
    0.99
);

// 3475
INSERT INTO track (
    TrackId,
    Name,
    AlbumId,
    MediaTypeId,
    GenreId,
    Composer,
    Milliseconds,
    Bytes,
    UnitPrice
)
VALUES (
    3475,
    'What Is It About Men',
    322,
    2,
    9,
    'Delroy "Chris" Cooper, Donovan Jackson, Earl Chinna Smith, Felix Howard, Gordon Williams, Luke Smith, Paul Watson & Wilburn Squiddley Cole',
    209573,
    3426106,
    0.99
);

// 3477
INSERT INTO track (
    TrackId,
    Name,
    AlbumId,
    MediaTypeId,
    GenreId,
    Composer,
    Milliseconds,
    Bytes,
    UnitPrice
)
VALUES (
    3477,
    'Amy Amy Amy (Outro)',
    322,
    2,
    9,
    'Astor Campbell, Delroy "Chris" Cooper, Donovan Jackson, Dorothy Fields, Earl Chinna Smith, Felix Howard, Gordon Williams, James Moody, Jimmy McHugh, Matt Rowe, Salaam Remi & Stefan Skarbek',
    663426,
    10564704,
    0.99
);


// 3488
INSERT INTO track (
    TrackId,
    Name,
    AlbumId,
    MediaTypeId,
    GenreId,
    Composer,
    Milliseconds,
    Bytes,
    UnitPrice
)
VALUES (
    3488,
    'Music for the Funeral of Queen Mary: VI. "Thou knowest, Lord, the Secrets of Our Hearts"',
    333,
    2,
    24,
    'Henry Purcell',
    142081,
    2365930,
    0.99
);

// TRANSFORMATION OF DATA

// DIM_DATE
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

SELECT * FROM dim_date;

// DIM_CUSTOMERS
CREATE OR REPLACE TABLE dim_customer AS
SELECT
    c.customerid AS dim_customerID,
    c.city AS city,
    c.state AS state,
    c.country AS country,
    c.supportrepid AS SupportRep_ID
FROM customer c
ORDER BY c.customerid;

SELECT * FROM dim_customer;

// DIM_TRACK
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

SELECT * FROM dim_track;

// FACT_TABLE_INVOICELINE
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

SELECT * FROM fact_invoiceLine;

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