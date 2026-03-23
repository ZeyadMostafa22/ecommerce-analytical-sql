-- ============================================================
--  E-Commerce Analytical Data Warehouse
--  Schema: ECOM_DW
-- ============================================================

-- ------------------------------------------------------------
-- 0. ENVIRONMENT SETUP
-- ------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS ECOM_DB;
CREATE SCHEMA  IF NOT EXISTS ECOM_DB.ECOM_DW;
USE SCHEMA ECOM_DB.ECOM_DW;


-- ============================================================
-- DIMENSION TABLES  (always before the fact table)
-- ============================================================

-- ------------------------------------------------------------
-- 1. DIM_DATE
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_DATE (
    date_key        INT             NOT NULL,          -- surrogate key  e.g. 20220101
    full_date       DATE            NOT NULL,
    day             TINYINT         NOT NULL,          -- 1–31
    month           TINYINT         NOT NULL,          -- 1–12
    month_name      VARCHAR(10)     NOT NULL,          -- 'January' … 'December'
    quarter         TINYINT         NOT NULL,          -- 1–4
    year            SMALLINT        NOT NULL,
    week_number     TINYINT         NOT NULL,          -- ISO week  1–53

    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);


-- ------------------------------------------------------------
-- 2. DIM_CUSTOMER
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    customer_key        INT             NOT NULL,       -- surrogate key
    customer_id         VARCHAR(20)     NOT NULL,       -- natural / business key
    gender              VARCHAR(10)     NOT NULL,       -- 'Male' | 'Female' | 'Other'
    age_group           VARCHAR(20)     NOT NULL,       -- '18-24' | '25-34' | '35-44' | '45-54' | '55+'
    city                VARCHAR(50)     NOT NULL,
    region              VARCHAR(50)     NOT NULL,
    registration_date   DATE            NOT NULL,
    customer_segment    VARCHAR(30)     NOT NULL,       -- 'New' | 'Regular' | 'VIP' | 'At-Risk'

    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key)
);


-- ------------------------------------------------------------
-- 3. DIM_CATEGORY
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_CATEGORY (
    category_key        INT             NOT NULL,
    category_name       VARCHAR(50)     NOT NULL,
    parent_category     VARCHAR(50),                   -- NULL for top-level categories
    seasonal_flag       BOOLEAN         NOT NULL DEFAULT FALSE,

    CONSTRAINT pk_dim_category PRIMARY KEY (category_key)
);


-- ------------------------------------------------------------
-- 4. DIM_PRODUCT
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_PRODUCT (
    product_key         INT             NOT NULL,
    product_id          VARCHAR(20)     NOT NULL,
    product_name        VARCHAR(100)    NOT NULL,
    brand               VARCHAR(50)     NOT NULL,
    subcategory         VARCHAR(50)     NOT NULL,
    launch_date         DATE            NOT NULL,
    stock_quantity      INT             NOT NULL DEFAULT 0,   -- needed for Recommendation System

    CONSTRAINT pk_dim_product PRIMARY KEY (product_key),
    CONSTRAINT chk_stock CHECK (stock_quantity >= 0)
);


-- ------------------------------------------------------------
-- 5. DIM_PAYMENT
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_PAYMENT (
    payment_key         INT             NOT NULL,
    payment_method      VARCHAR(30)     NOT NULL,      -- 'Credit Card' | 'Debit Card' | 'PayPal' | 'COD' | 'Wallet'

    CONSTRAINT pk_dim_payment PRIMARY KEY (payment_key)
);


-- ------------------------------------------------------------
-- 6. DIM_SHIPPING
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_SHIPPING (
    shipping_key        INT             NOT NULL,
    shipping_type       VARCHAR(30)     NOT NULL,      -- 'Standard' | 'Express' | 'Same-Day' | 'Free'
    delivery_days       TINYINT         NOT NULL,      -- average number of days

    CONSTRAINT pk_dim_shipping PRIMARY KEY (shipping_key),
    CONSTRAINT chk_delivery_days CHECK (delivery_days > 0)
);


-- ============================================================
-- FACT TABLE
-- ============================================================

-- ------------------------------------------------------------
-- 7. FACT_ORDER_LINE
--    Grain: one row = one order line item
--    NOTE: order_id & order_line_id were added to the original
--          spec — required for AOV and Repeat Purchase Rate.
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE FACT_ORDER_LINE (

    -- Grain identifiers
    order_line_id       INT             NOT NULL,       -- unique row identifier
    order_id            VARCHAR(20)     NOT NULL,       -- groups lines belonging to the same order

    -- Foreign Keys → Dimensions
    date_key            INT             NOT NULL,
    customer_key        INT             NOT NULL,
    product_key         INT             NOT NULL,
    category_key        INT             NOT NULL,
    payment_key         INT             NOT NULL,
    shipping_key        INT             NOT NULL,

    -- Measures
    quantity            INT             NOT NULL,
    gross_amount        NUMBER(12,2)    NOT NULL,       -- before discount  (unit_price × qty)
    discount_amount     NUMBER(12,2)    NOT NULL DEFAULT 0,
    net_amount          NUMBER(12,2)    NOT NULL,       -- gross − discount  (= revenue)
    cost_amount         NUMBER(12,2)    NOT NULL,       -- COGS
    profit_amount       NUMBER(12,2)    NOT NULL,       -- net − cost

    -- Constraints
    CONSTRAINT pk_fact_order_line   PRIMARY KEY (order_line_id),

    CONSTRAINT fk_fact_date         FOREIGN KEY (date_key)      REFERENCES DIM_DATE     (date_key),
    CONSTRAINT fk_fact_customer     FOREIGN KEY (customer_key)  REFERENCES DIM_CUSTOMER (customer_key),
    CONSTRAINT fk_fact_product      FOREIGN KEY (product_key)   REFERENCES DIM_PRODUCT  (product_key),
    CONSTRAINT fk_fact_category     FOREIGN KEY (category_key)  REFERENCES DIM_CATEGORY (category_key),
    CONSTRAINT fk_fact_payment      FOREIGN KEY (payment_key)   REFERENCES DIM_PAYMENT  (payment_key),
    CONSTRAINT fk_fact_shipping     FOREIGN KEY (shipping_key)  REFERENCES DIM_SHIPPING (shipping_key),

    CONSTRAINT chk_quantity         CHECK (quantity > 0),
    CONSTRAINT chk_gross            CHECK (gross_amount >= 0),
    CONSTRAINT chk_discount         CHECK (discount_amount >= 0),
    CONSTRAINT chk_net              CHECK (net_amount >= 0),
    CONSTRAINT chk_cost             CHECK (cost_amount >= 0)
);


-- ============================================================
-- QUICK VERIFICATION  — run after creating tables
-- ============================================================
SHOW TABLES IN SCHEMA ECOM_DB.ECOM_DW;

-- Expected output: 7 tables
-- DIM_DATE | DIM_CUSTOMER | DIM_CATEGORY | DIM_PRODUCT
-- DIM_PAYMENT | DIM_SHIPPING | FACT_ORDER_LINE
