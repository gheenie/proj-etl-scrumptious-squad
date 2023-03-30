DROP DATABASE IF EXISTS test_warehouse;

CREATE DATABASE test_warehouse;

\c test_warehouse


CREATE TABLE dim_date (
  date_id date primary key not null,
  year int not null,
  month int not null,
  day int not null,
  day_of_week int not null,
  day_name varchar not null,
  month_name varchar not null,
  quarter int not null
);

CREATE TABLE dim_staff (
  staff_id int primary key not null,
  first_name varchar not null,
  last_name varchar not null,
  department_name varchar not null,
  location varchar not null,
  email_address varchar not null
);

CREATE TABLE dim_location (
  location_id int primary key not null,
  address_line_1 varchar not null,
  address_line_2 varchar,
  district varchar,
  city varchar not null,
  postal_code varchar not null,
  country varchar not null,
  phone varchar not null
);

CREATE TABLE dim_currency (
  currency_id int primary key not null,
  currency_code varchar not null,
  currency_name varchar not null
);

CREATE TABLE dim_design (
  design_id int primary key not null,
  design_name varchar not null,
  file_location varchar not null,
  file_name varchar not null
);

CREATE TABLE dim_counterparty (
  counterparty_id int primary key not null,
  counterparty_legal_name varchar not null,
  counterparty_legal_address_line_1 varchar not null,
  counterparty_legal_address_line2 varchar,
  counterparty_legal_district varchar,
  counterparty_legal_city varchar not null,
  counterparty_legal_postal_code varchar not null,
  counterparty_legal_country varchar not null,
  counterparty_legal_phone_number varchar not null
);

CREATE TABLE fact_sales_order (
  sales_record_id SERIAL PRIMARY KEY,
  sales_order_id int not null,
  created_date date not null,
  created_time time not null,
  last_updated_date date not null,
  last_updated_time time not null,
  sales_staff_id int not null,
  counterparty_id int not null,
  units_sold int not null,
  unit_price numeric(10, 2) not null,
  currency_id int not null,
  design_id int not null,
  agreed_payment_date date not null,
  agreed_delivery_date date not null,
  agreed_delivery_location_id int not null
);



SELECT * FROM dim_date;
SELECT * FROM dim_staff;
SELECT * FROM dim_location;
SELECT * FROM dim_currency;
SELECT * FROM dim_design;
SELECT * FROM dim_counterparty;
SELECT * FROM fact_sales_order;


