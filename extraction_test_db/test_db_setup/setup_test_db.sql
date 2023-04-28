DROP DATABASE IF EXISTS test_totesys;

CREATE DATABASE test_totesys;

\c test_totesys

CREATE TABLE address (
    address_id integer,
    address_line_1 text,
    address_line_2 text,
    district text,
    city text, 
    postal_code text,
    country text,
    phone text,
    created_at timestamp without time zone,
    last_updated timestamp without time zone
);

CREATE TABLE counterparty (
  counterparty_id integer,
  counterparty_legal_name text,
  legal_address_id integer,
  commercial_contact text,
  delivery_contact text,
  created_at timestamp without time zone,
  last_updated timestamp without time zone
);

CREATE TABLE currency (
  currency_id integer,
  currency_code varchar(3),
  created_at timestamp without time zone,
  last_updated timestamp without time zone
);

CREATE TABLE department (
  department_id integer,
  department_name text,
  location text,
  manager text,
  created_at timestamp without time zone,
  last_updated timestamp without time zone
);

CREATE TABLE design (
   design_id integer,
   created_at timestamp without time zone,
   design_name text,
   file_location text,
   file_name text,
   last_updated timestamp without time zone
);

CREATE TABLE payment_type (
  payment_type_id integer,
  payment_type_name text,
  created_at timestamp without time zone,
  last_updated timestamp without time zone
);

CREATE TABLE payment (
  payment_id integer,
  created_at timestamp without time zone,
  last_updated timestamp without time zone,
  transaction_id integer,
  counterparty_id integer,
  payment_amount numeric,
  currency_id integer,
  payment_type_id integer,
  paid boolean,
  payment_date text,
  company_ac_number integer,
  counterparty_ac_number integer
);

CREATE TABLE purchase_order (
  purchase_order_id integer,
  created_at timestamp without time zone,
  last_updated timestamp without time zone,
  staff_id integer,
  counterparty_id integer,
  item_code text,
  item_quantity integer,
  item_unit_price numeric,
  currency_id integer,
  agreed_delivery_date text,
  agreed_payment_date text,
  agreed_delivery_location_id integer
);

CREATE TABLE sales_order (
  sales_order_id integer,
  created_at timestamp without time zone,
  last_updated timestamp without time zone,
  design_id integer,
  staff_id integer,
  counterparty_id integer,
  units_sold integer,
  unit_price numeric,
  currency_id integer,
  agreed_delivery_date text,
  agreed_payment_date text,
  agreed_delivery_location_id integer
);

CREATE TABLE staff (
  staff_id integer,
  first_name text,
 last_name text,
 department_id integer,
 email_address text,
 created_at timestamp without time zone,
 last_updated timestamp without time zone
);

CREATE TABLE transaction (
  transaction_id integer,
  transaction_type text,
  sales_order_id integer,
  purchase_order_id integer,
  created_at timestamp without time zone,
  last_updated timestamp without time zone
);


INSERT INTO address
(address_id,address_line_1,address_line_2,district,city,postal_code,country,phone,created_at,last_updated)
VALUES
(1,'al1-a','al2-a','district-a','city-a','11111','country-a','0000 000001','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(2,'al1-b','al2-b','district-b','city-b','22222-2222','country-b','0000 000002','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(3,'al1-c','al2-c','district-c','city-c','33333','country-c','0000 000003','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(4,'al1-d','al2-d','district-d','city-d','44444-4444','country-d','0000 000004','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(5,'al1-e','al2-e','district-e','city-e','55555-5555','country-e','0000 000005','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000');

INSERT INTO counterparty
(counterparty_id,counterparty_legal_name,legal_address_id,commercial_contact,delivery_contact,created_at,last_updated)
VALUES
(1,'cp-a',1,'cc-a','dc-a','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(2,'cp-b',2,'cc-b','dc-b','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(3,'cp-c',3,'cc-c','dc-c','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(4,'cp-d',3,'cc-d','dc-d','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000');

INSERT INTO currency
(currency_id,currency_code,created_at,last_updated)
VALUES
(1,'AAA','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(2,'BBB','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(3,'CCC','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000');

INSERT INTO department
(department_id,department_name,location,manager,created_at,last_updated)
VALUES
(1,'dept-a','loc-a','man-a','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(2,'dept-b','loc-b','man-b','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(3,'dept-c','loc-c','man-c','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000');

INSERT INTO design 
(design_id,created_at,design_name,file_location,file_name,last_updated)
VALUES
(1,'2023-01-01 10:00:00.000000','design-a','/aa','file-a.json','2023-01-01 10:00:00.000000'),
(2,'2023-01-01 10:00:00.000000','design-b','/bb','file-b.json','2023-01-01 10:00:00.000000'),
(3,'2023-01-01 10:00:00.000000','design-c','/cc','file-c.json','2023-01-01 10:00:00.000000'),
(4,'2023-01-01 10:00:00.000000','design-d','/dd','file-d.json','2023-01-01 10:00:00.000000'),
(5,'2023-01-01 10:00:00.000000','design-e','/ee','file-e.json','2023-01-01 10:00:00.000000'),
(6,'2023-01-01 10:00:00.000000','design-f','/ff','file-f.json','2023-01-01 10:00:00.000000');

INSERT INTO payment_type
(payment_type_id,payment_type_name,created_at,last_updated)
VALUES
(1,'SALES_RECEIPT','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(2,'SALES_REFUND','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(3,'PURCHASE_PAYMENT','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(4,'PURCHASE_REFUND','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000');

INSERT INTO payment
(payment_id,created_at,last_updated,transaction_id,counterparty_id,payment_amount,currency_id,payment_type_id,paid,payment_date,company_ac_number,counterparty_ac_number)
VALUES 
(1,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',1,1,10.00,1,1,False,'2023-01-01',10000011,10000012),
(2,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',2,2,20.00,2,2,True,'2023-01-01',10000021,10000022),
(3,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',3,3,30.00,3,3,True,'2023-01-01',10000031,10000032),
(4,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',4,1,10.00,1,4,False,'2023-01-01',10000041,10000042),
(5,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',5,2,20.00,2,1,True,'2023-01-01',10000051,10000052),
(6,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',6,3,30.00,3,2,True,'2023-01-01',10000061,10000062);

-- NOT SURE HOW TO DEAL WITH ACCOuNT NUMBERS THAT HAVE 0s AT THE BEGINING 

INSERT INTO purchase_order
(purchase_order_id,created_at,last_updated,staff_id,counterparty_id,item_code,item_quantity,item_unit_price,currency_id,agreed_delivery_date,agreed_payment_date,agreed_delivery_location_id)
VALUES
(1,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',1,1,'AAAAAAA',1,10.00,1,'2023-01-01','2023-01-01',1),
(2,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',2,2,'AAAAAAA',2,10.00,2,'2023-01-01','2023-01-01',2),
(3,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',3,3,'AAAAAAA',3,10.00,3,'2023-01-01','2023-01-01',3),
(4,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',1,1,'AAAAAAA',4,10.00,1,'2023-01-01','2023-01-01',1),
(5,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',2,2,'AAAAAAA',5,10.00,2,'2023-01-01','2023-01-01',2),
(6,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',3,3,'AAAAAAA',6,10.00,3,'2023-01-01','2023-01-01',3);


INSERT INTO sales_order
(sales_order_id,created_at,last_updated,design_id,staff_id,counterparty_id,units_sold,unit_price,currency_id,agreed_delivery_date,agreed_payment_date,agreed_delivery_location_id)
VALUES
(1,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',1,1,1,10,1.00,1,'2023-01-01','2023-01-01',1),
(2,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',2,2,2,20,2.00,2,'2023-01-01','2023-01-01',2),
(3,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',3,3,3,30,3.00,3,'2023-01-01','2023-01-01',3),
(4,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',4,1,1,40,4.00,1,'2023-01-01','2023-01-01',4),
(5,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',5,2,2,50,5.00,2,'2023-01-01','2023-01-01',5),
(6,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000',6,3,3,60,6.00,3,'2023-01-01','2023-01-01',1);



INSERT INTO staff
(staff_id,first_name,last_name,department_id,email_address,created_at,last_updated)
VALUES
(1,'fn-a','ln-a',1,'fna.lna@terrifictotes.com','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(2,'fn-b','ln-b',2,'fnb.lnb@terrifictotes.com','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(3,'fn-c','ln-c',3,'fnc.lnc@terrifictotes.com','2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000');


INSERT INTO transaction
(transaction_id,transaction_type,sales_order_id,purchase_order_id,created_at,last_updated)
VALUES
(1,'PURCHASE',null,1,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(2,'SALE',1,null,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(3,'PURCHASE',null,2,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(4,'SALE',2,null,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(5,'PURCHASE',null,3,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000'),
(6,'SALE',3,null,'2023-01-01 10:00:00.000000','2023-01-01 10:00:00.000000');


SELECT * FROM address;
SELECT * FROM counterparty;
SELECT * FROM currency;
SELECT * FROM department;
SELECT * FROM design;
SELECT * FROM payment_type;
SELECT * FROM payment;
SELECT * FROM purchase_order;
SELECT * FROM sales_order;
SELECT * FROM staff;
SELECT * FROM transaction;

