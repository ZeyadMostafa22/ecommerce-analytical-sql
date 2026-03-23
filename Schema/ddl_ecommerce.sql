CREATE DATABASE IF NOT EXISTS ECOM_DB;
CREATE SCHEMA IF NOT EXISTS ECOM_DB.ECOM_DW;
USE SCHEMA ECOM_DB.ECOM_DW;

CREATE OR REPLACE TABLE DIM_DATE (
    date_key        INT             NOT NULL,
    full_date       DATE            NOT NULL,
    day             INT             NOT NULL,
    day_name        VARCHAR(15)     NOT NULL,
    month           INT             NOT NULL,
    month_name      VARCHAR(10)     NOT NULL,
    quarter         INT             NOT NULL,
    year            INT             NOT NULL,
    week_number     INT             NOT NULL,
    is_weekend      BOOLEAN         NOT NULL,
    is_holiday      BOOLEAN         NOT NULL,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);

CREATE OR REPLACE TABLE DIM_CUSTOMER (
    customer_key        INT             NOT NULL,
    customer_id         VARCHAR(20)     NOT NULL,
    gender              VARCHAR(25)     NOT NULL,
    first_name          VARCHAR(50)     NOT NULL,
    last_name           VARCHAR(50)     NOT NULL,
    age_group           VARCHAR(20)     NOT NULL,
    city                VARCHAR(50)     NOT NULL,
    state               VARCHAR(50)     NOT NULL,
    country             VARCHAR(50)     NOT NULL,
    email               VARCHAR(100)    NOT NULL,
    phone               VARCHAR(30)     NOT NULL,
    region              VARCHAR(50)     NOT NULL,
    registration_date   DATE            NOT NULL,
    customer_segment    VARCHAR(30)     NOT NULL,
    loyalty_points      INT             NOT NULL,
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key)
);

CREATE OR REPLACE TABLE DIM_CATEGORY (
    category_key        INT             NOT NULL,
    category_name       VARCHAR(50)     NOT NULL,
    parent_category     VARCHAR(50),
    seasonal_flag       BOOLEAN         NOT NULL DEFAULT FALSE,
    CONSTRAINT pk_dim_category PRIMARY KEY (category_key)
);

CREATE OR REPLACE TABLE DIM_PRODUCT (
    product_key         INT             NOT NULL,
    product_id          VARCHAR(20)     NOT NULL,
    product_name        VARCHAR(100)    NOT NULL,
    brand               VARCHAR(50)     NOT NULL,
    subcategory         VARCHAR(50)     NOT NULL,
    category_key        INT             NOT NULL,
    launch_date         DATE            NOT NULL,
    base_price          NUMBER(10,2)    NOT NULL,
    cost_price          NUMBER(10,2)    NOT NULL,
    stock_quantity      INT             NOT NULL DEFAULT 0,
    is_active           BOOLEAN         NOT NULL,
    weight_kg           NUMBER(10,2)    NOT NULL,
    rating              NUMBER(3,1)     NOT NULL,
    review_count        INT             NOT NULL,
    CONSTRAINT pk_dim_product PRIMARY KEY (product_key)
);

CREATE OR REPLACE TABLE DIM_PAYMENT (
    payment_key         INT             NOT NULL,
    payment_method      VARCHAR(30)     NOT NULL,
    is_digital          BOOLEAN         NOT NULL,
    CONSTRAINT pk_dim_payment PRIMARY KEY (payment_key)
);

CREATE OR REPLACE TABLE DIM_SHIPPING (
    shipping_key        INT             NOT NULL,
    shipping_type       VARCHAR(30)     NOT NULL,
    delivery_days       INT             NOT NULL,
    base_cost           NUMBER(10,2)    NOT NULL,
    is_trackable        BOOLEAN         NOT NULL,
    CONSTRAINT pk_dim_shipping PRIMARY KEY (shipping_key)
);
CREATE OR REPLACE TABLE FACT_ORDER_LINE (
    order_id            INT             NOT NULL,
    date_key            INT             NOT NULL,
    customer_key        INT             NOT NULL,
    product_key         INT             NOT NULL,
    category_key        INT             NOT NULL,
    payment_key         INT             NOT NULL,
    shipping_key        INT             NOT NULL,
    quantity            INT             NOT NULL,
    unit_price          NUMBER(12,2)    NOT NULL,
    gross_amount        NUMBER(12,2)    NOT NULL,
    discount_amount     NUMBER(12,2)    NOT NULL DEFAULT 0,
    net_amount          NUMBER(12,2)    NOT NULL,
    cost_amount         NUMBER(12,2)    NOT NULL,
    profit_amount       NUMBER(12,2)    NOT NULL,
    order_status        VARCHAR(20)     NOT NULL,
    CONSTRAINT pk_fact_order_id PRIMARY KEY (order_id),
    CONSTRAINT fk_fact_date     FOREIGN KEY (date_key)     REFERENCES DIM_DATE(date_key),
    CONSTRAINT fk_fact_customer FOREIGN KEY (customer_key) REFERENCES DIM_CUSTOMER(customer_key),
    CONSTRAINT fk_fact_product  FOREIGN KEY (product_key)  REFERENCES DIM_PRODUCT(product_key),
    CONSTRAINT fk_fact_category FOREIGN KEY (category_key) REFERENCES DIM_CATEGORY(category_key),
    CONSTRAINT fk_fact_payment  FOREIGN KEY (payment_key)  REFERENCES DIM_PAYMENT(payment_key),
    CONSTRAINT fk_fact_shipping FOREIGN KEY (shipping_key) REFERENCES DIM_SHIPPING(shipping_key)
);

SHOW TABLES IN SCHEMA ECOM_DB.ECOM_DW;