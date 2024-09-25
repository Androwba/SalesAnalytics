DROP DATABASE IF EXISTS retailanalitycs;
CREATE DATABASE retailanalitycs;
CREATE SCHEMA IF NOT EXISTS public;
SET DATESTYLE to iso, DMY;

DROP TABLE IF EXISTS Personal_Information CASCADE;
DROP TABLE IF EXISTS Cards CASCADE;
DROP TABLE IF EXISTS Transactions CASCADE;
DROP TABLE IF EXISTS SKU_Group CASCADE;
DROP TABLE IF EXISTS Product_Grid CASCADE;
DROP TABLE IF EXISTS Stores;
DROP TABLE IF EXISTS Checks;
DROP TABLE IF EXISTS Date_Of_Analysis_Formation;

CREATE TABLE IF NOT EXISTS Personal_Information (
    Customer_ID SERIAL PRIMARY KEY,
    Customer_Name VARCHAR(50) CHECK (Customer_Name ~ '^([A-Z][a-z\s-]*|[А-ЯЁЙ][а-яйё\s-]*|[A-Z][a-z\s-]*-[A-Z][a-z\s-]*|[А-ЯЁЙ][а-яйё\s-]*-[А-ЯЁЙ][а-яйё\s-]*|[A-Z][a-z\s-]* [A-Z][a-z\s-]*|[А-ЯЁЙ][а-яйё\s-]* [А-ЯЁЙ][а-яйё\s-]*)$'),
    Customer_Surname VARCHAR(50) CHECK (Customer_Surname ~ '^([A-Z][a-z\s-]*|[А-ЯЁЙ][а-яйё\s-]*|[A-Z][a-z\s-]*-[A-Z][a-z\s-]*|[А-ЯЁЙ][а-яйё\s-]*-[А-ЯЁЙ][а-яйё\s-]*|[A-Z][a-z\s-]* [A-Z][a-z\s-]*|[А-ЯЁЙ][а-яйё\s-]* [А-ЯЁЙ][а-яйё\s-]*)$'),
    Customer_Primary_Email VARCHAR(50) UNIQUE CHECK (Customer_Primary_Email ~ '^[-\w\.]+@([\w]+\.)+[\w]{2,4}$'),
    Customer_Primary_Phone VARCHAR(12) UNIQUE CHECK (Customer_Primary_Phone  ~ '^\+7\d{10}$')
);

CREATE TABLE IF NOT EXISTS Cards (
    Customer_Card_ID SERIAL PRIMARY KEY,
    Customer_ID INT NOT NULL,
    FOREIGN KEY (Customer_ID) REFERENCES Personal_Information(Customer_ID)
);

CREATE TABLE IF NOT EXISTS Transactions (
    Transaction_ID SERIAL PRIMARY KEY,
    Customer_Card_ID INT REFERENCES Cards(Customer_Card_ID),
    Transaction_Summ NUMERIC CHECK (Transaction_Summ > 0),
    Transaction_DateTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    Transaction_Store_ID INT CHECK (Transaction_Store_ID > 0)
);

CREATE TABLE IF NOT EXISTS SKU_Group (
    Group_ID SERIAL PRIMARY KEY,
    Group_Name VARCHAR(100) NOT NULL CHECK (Group_Name ~ '^[A-Za-zА-ЯЙЁа-яйё0-9\s\-\.\,\;\!\@\#\$\%\^\&\*\(\)\_\+\=\[\]\{\}\|\\\\\:\''\"\?\<\>\`\~\/]+$')
);

CREATE TABLE IF NOT EXISTS Product_Grid (
    SKU_ID SERIAL PRIMARY KEY,
    SKU_Name VARCHAR(100) NOT NULL CHECK (SKU_Name ~ '^[A-Za-zА-ЯЙЁа-яйё0-9\s\-\.\,\;\!\@\#\$\%\^\&\*\(\)\_\+\=\[\]\{\}\|\\\\\:\''\"\?\<\>\`\~\/]+$'),
    Group_ID INT NOT NULL,
    FOREIGN KEY (Group_ID) REFERENCES SKU_Group (Group_ID)
);

CREATE TABLE IF NOT EXISTS Stores (
    Transaction_Store_ID INTEGER,
    SKU_ID INT REFERENCES Product_Grid(SKU_ID),
    SKU_Purchase_Price NUMERIC NOT NULL CHECK (SKU_Purchase_Price >= 0),
    SKU_Retail_Price NUMERIC NOT NULL CHECK (SKU_Retail_Price >= 0)
);

CREATE TABLE IF NOT EXISTS Checks (
    Transaction_ID INT REFERENCES Transactions(Transaction_ID),
    SKU_ID INT REFERENCES Product_Grid(SKU_ID),
    SKU_Amount NUMERIC NOT NULL,
    SKU_Summ NUMERIC NOT NULL,
    SKU_Summ_Paid NUMERIC NOT NULL,
    SKU_Discount NUMERIC NOT NULL
);

CREATE TABLE IF NOT EXISTS Date_Of_Analysis_Formation (
    Analysis_Formation TIMESTAMP
);

-- Import-Export procedures

create or replace procedure import_from_csv (table_name text, path text, delim char(1) default ',')
as $$
begin
execute 'COPY '||$1||' FROM '''||$2||''' DELIMITER '''||$3||''' CSV header';
end;
$$ language plpgsql;

create or replace procedure export_to_csv (table_name text, path text, delim char(1) default ',')
as $$
begin
	execute 'COPY '||$1||' TO '''||$2||''' DELIMITER '''||$3||''' CSV HEADER';
end;
$$ language plpgsql;

call import_from_csv('Personal_Information', '/Users/username/SQL3_RetailAnalitycs_v1.0-1/src/csv/personal_information.csv', ','); 
call import_from_csv('Cards', '/Users/username/SQL3_RetailAnalitycs_v1.0-1/src/csv/cards.csv', ',');
call import_from_csv('transactions', '/Users/username/SQL3_RetailAnalitycs_v1.0-1/src/csv/transactions.csv', ',');
call import_from_csv('Sku_group', '/Users/username/SQL3_RetailAnalitycs_v1.0-1/src/csv/sku_groups.csv', ',');
call import_from_csv('Product_grid', '/Users/username/SQL3_RetailAnalitycs_v1.0-1/src/csv/product_grid.csv', ',');
call import_from_csv('Stores', '/Users/username/SQL3_RetailAnalitycs_v1.0-1/src/csv/sales_places.csv', ',');
call import_from_csv('Checks', '/Users/username/SQL3_RetailAnalitycs_v1.0-1/src/csv/tickets.csv', ',');
call import_from_csv('Date_Of_Analysis_Formation', '/Users/username/SQL3_RetailAnalitycs_v1.0-1/src/csv/date_analysis.csv', ',');
