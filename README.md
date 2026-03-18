# 🛒 E-Commerce Platform — Analytical SQL & KPI Project

## 📌 Project Overview
An analytical data warehouse built for an E-Commerce platform.

---

## 🗂️ Project Structure
```
ecommerce-analytical-sql/
├── schema/          # DDL scripts and ERD diagram
├── data/            # Sample dataset
├── queries/         # All analytical SQL queries
├── docs/            # Documentation and analytical reasoning
└── README.md
```

---

## 🏗️ Schema Design
Star Schema consisting of:
- **Fact Table:** Fact_Order_Line
- **Dimensions:** Dim_Date, Dim_Customer, Dim_Product,
  Dim_Category, Dim_Payment, Dim_Shipping
