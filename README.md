<div align="center">

<br/>

```
███████╗      ██████╗ ██████╗ ███╗   ███╗███╗   ███╗███████╗██████╗  ██████╗███████╗
██╔════╝     ██╔════╝██╔═══██╗████╗ ████║████╗ ████║██╔════╝██╔══██╗██╔════╝██╔════╝
█████╗       ██║     ██║   ██║██╔████╔██║██╔████╔██║█████╗  ██████╔╝██║     █████╗  
██╔══╝       ██║     ██║   ██║██║╚██╔╝██║██║╚██╔╝██║██╔══╝  ██╔══██╗██║     ██╔══╝  
███████╗     ╚██████╗╚██████╔╝██║ ╚═╝ ██║██║ ╚═╝ ██║███████╗██║  ██║╚██████╗███████╗
╚══════╝      ╚═════╝ ╚═════╝ ╚═╝     ╚═╝╚═╝     ╚═╝╚══════╝╚═╝  ╚═╝ ╚═════╝╚══════╝
```

# 🛒 E-Commerce Analytical Platform

### *A production-grade Data Warehouse built on Snowflake — with a live AI-powered Recommendation Engine deployed via Streamlit*

<br/>

[![Snowflake](https://img.shields.io/badge/Data%20Warehouse-Snowflake-%2329B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Streamlit](https://img.shields.io/badge/App-Streamlit-%23FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io/)
[![SQL](https://img.shields.io/badge/Language-Advanced%20SQL-%23336791?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Schema](https://img.shields.io/badge/Schema-Star%20Schema-%23F7DF1E?style=for-the-badge&logo=databricks&logoColor=black)](https://en.wikipedia.org/wiki/Star_schema)
[![License](https://img.shields.io/badge/License-MIT-%2300C851?style=for-the-badge)](LICENSE)

<br/>

> *"Data is the new oil. But raw oil is worthless — this project is the refinery."*

<br/>

---

</div>

## 📌 Table of Contents

- [🧠 Project Overview](#-project-overview)
- [🏗️ Architecture & Schema Design](#️-architecture--schema-design)
- [📂 Repository Structure](#-repository-structure)
- [📊 Core Business KPIs](#-core-business-kpis)
- [🔍 Analytical SQL Modules](#-analytical-sql-modules)
- [🤖 AI-Powered Recommendation System](#-ai-powered-recommendation-system)
- [❄️ Snowflake Data Warehouse Setup](#️-snowflake-data-warehouse-setup)
- [🖥️ Streamlit App Deployment](#️-streamlit-app-deployment)
- [🚀 Getting Started](#-getting-started)
- [👥 Team](#-team)

---

## 🧠 Project Overview

This project is a **fully engineered, end-to-end analytical data warehouse** for a simulated E-Commerce platform. It was designed from the ground up to answer hard, real-world business questions using nothing but the power of **advanced analytical SQL**.

No dashboards. No drag-and-drop. Pure engineering.

### What makes this project different?

| Capability | Description |
|---|---|
| 🏛️ **Star Schema DWH** | Purpose-built fact + dimension model optimized for OLAP queries |
| 🪟 **Window Function Mastery** | 21 business questions solved exclusively with window logic |
| 📈 **KPI Engine** | 7 core business KPIs computed from scratch with pure SQL logic |
| 🤖 **Recommendation Engine** | A scoring-based SQL recommendation system, live on Streamlit |
| ❄️ **Snowflake Native** | Warehouse, schemas, stages, and compute all managed in Snowflake |
| 🖥️ **Live App** | Streamlit-in-Snowflake web interface — click a product, get 4 smart recommendations |

---

## 🏗️ Architecture & Schema Design

### Star Schema

```
                          ┌─────────────────┐
                          │    Dim_Date     │
                          │─────────────────│
                          │ date_key (PK)   │
                          │ full_date       │
                          │ day / month     │
                          │ quarter / year  │
                          │ week_number     │
                          └────────┬────────┘
                                   │
┌──────────────────┐    ┌──────────┴──────────┐    ┌──────────────────┐
│   Dim_Customer   │    │   Fact_Order_Line   │    │   Dim_Product    │
│──────────────────│    │─────────────────────│    │──────────────────│
│ customer_key(PK) ├────┤ date_key      (FK)  ├────┤ product_key (PK) │
│ customer_id      │    │ customer_key  (FK)  │    │ product_id       │
│ gender           │    │ product_key   (FK)  │    │ product_name     │
│ age_group        │    │ category_key  (FK)  │    │ brand            │
│ city / region    │    │ payment_key   (FK)  │    │ subcategory      │
│ registration_date│    │ shipping_key  (FK)  │    │ launch_date      │
│ customer_segment │    │─────────────────────│    └──────────────────┘
└──────────────────┘    │ quantity            │
                        │ gross_amount        │    ┌──────────────────┐
┌──────────────────┐    │ discount_amount     │    │   Dim_Category   │
│   Dim_Payment    │    │ net_amount          │    │──────────────────│
│──────────────────│    │ cost_amount         ├────┤ category_key(PK) │
│ payment_key (PK) ├────┤ profit_amount       │    │ category_name    │
│ payment_method   │    └──────────┬──────────┘    │ parent_category  │
└──────────────────┘               │               │ seasonal_flag    │
                        ┌──────────┴──────────┐    └──────────────────┘
                        │   Dim_Shipping      │
                        │─────────────────────│
                        │ shipping_key (PK)   │
                        │ shipping_type       │
                        │ delivery_days       │
                        └─────────────────────┘
```

> **Grain**: One row per order line item — the most granular level of e-commerce transaction data.

---

## 📂 Repository Structure

```
📦 E-Commerce-Analytical-Platform
│
├── 📁 Schema/                              # DDL scripts & ERD
│   ├── Ecommerce_DDL.sql                   # DDL scripts
│   └── erd_diagram.png                     # Star schema ERD
│
├── 📁 Scripts/                             # Data generation & loading            
│   └──  data_generation.sql                # Synthetic dataset population
│
├── 📁 Core Business KPIs/                  # Core metric definitions
│   ├── KPIs_Logic.sql
│   └── Mertics_Logic.md
│
├── 📁 A-Time-Based-Performance-Analysis/   # Module A: 6 window queries
│   ├── Q1_cumulative_revenue.sql
│   ├── Q2_month_to_date.sql
│   ├── Q3_year_to_date_profit.sql
│   ├── Q4_moving_average.sql
│   ├── Q5_mom_comparison.sql
│   └── Q6_revenue_acceleration.sql
│
├── 📁 B-Ranking-and-Contribution-Analysis/ # Module B: 5 window queries
│   ├── Q7_product_rank_by_category.sql
│   ├── Q8_product_contribution.sql
│   ├── Q9_pareto_analysis.sql
│   ├── Q10_region_profitability_rank.sql
│   └── Q11_brand_rank_by_category.sql
│
├── 📁 C-Customer-Behavior-Analysis/        # Module C: 5 window queries
│   ├── Q12_cumulative_customer_spend.sql
│   ├── Q13_purchase_intervals.sql
│   ├── Q14_recency_rank.sql
│   ├── Q15_spending_tier_segmentation.sql
│   └── Q16_top_percentile_customers.sql
│
├── 📁 D-Advanced-Product-Category-Analysis/# Module D: 5 window queries
│   ├── Q17_revenue_volatility.sql
│   ├── Q18_trending_categories.sql
│   ├── Q19_seasonality_analysis.sql
│   ├── Q20_profit_consistency.sql
│   └── Q21_sustained_decline_detection.sql
│
├── 📁 Recommendation-System/               # The crown jewel 👑
│   ├── recommendation_engine.sql           # Core scoring logic
│   └── streamlit_app.py                    # Live Streamlit-in-Snowflake app
│
├── .gitignore
└── README.md
```

---

## 📊 Core Business KPIs

Seven business KPIs were defined from business logic alone — **no formulas were given**. Each was reverse-engineered from real-world e-commerce analytics practices.

| # | KPI | Business Meaning |
|---|-----|-----------------|
| 1 | **Revenue** | Total net monetary value from completed transactions |
| 2 | **Gross Profit** | Revenue minus total product cost — operational health signal |
| 3 | **AOV** (Avg Order Value) | Revenue divided by distinct orders — measures basket size |
| 4 | **CLV** (Customer Lifetime Value) | Total revenue attributed to a single customer, all-time |
| 5 | **Repeat Purchase Rate** | % of customers with more than one order — loyalty indicator |
| 6 | **Profit Margin** | Profit as a % of revenue — efficiency of each dollar earned |
| 7 | **Revenue Growth Rate** | Period-over-period % change in revenue — momentum metric |

---

## 🔍 Analytical SQL Modules

All 21 business questions are answered using **window functions only** — no subquery-only hacks, no application-side aggregation.

### Module A — Time-Based Performance Analysis

| Q# | Question | Key Window Technique |
|----|----------|----------------------|
| Q1 | Cumulative revenue over time | `SUM() OVER (ORDER BY date)` |
| Q2 | Month-to-Date performance | `SUM() OVER (PARTITION BY year, month ORDER BY date)` |
| Q3 | Year-to-Date profit | `SUM() OVER (PARTITION BY year ORDER BY date)` |
| Q4 | Moving average (smoothing) | `AVG() OVER (ORDER BY date ROWS BETWEEN N PRECEDING AND CURRENT ROW)` |
| Q5 | Month-over-Month comparison | `LAG()` on monthly aggregates |
| Q6 | Revenue acceleration detection | `LAG()` applied twice — delta of deltas |

### Module B — Ranking & Contribution Analysis

| Q# | Question | Key Window Technique |
|----|----------|----------------------|
| Q7 | Product rank within category | `RANK() OVER (PARTITION BY category ORDER BY revenue DESC)` |
| Q8 | Product revenue contribution % | `SUM() OVER (PARTITION BY category)` as denominator |
| Q9 | Pareto (80/20) analysis | Cumulative % with `SUM() OVER` + threshold filtering |
| Q10 | Region profitability ranking | `DENSE_RANK() OVER (ORDER BY profit DESC)` |
| Q11 | Brand rank within category | `RANK() OVER (PARTITION BY category ORDER BY profit DESC)` |

### Module C — Customer Behavior Analytics

| Q# | Question | Key Window Technique |
|----|----------|----------------------|
| Q12 | Cumulative spend per customer | `SUM() OVER (PARTITION BY customer ORDER BY date)` |
| Q13 | Days between purchases | `LAG(order_date) OVER (PARTITION BY customer ORDER BY date)` |
| Q14 | Recency ranking | `RANK() OVER (ORDER BY last_order_date DESC)` |
| Q15 | Spending tier segmentation | `NTILE(4) OVER (ORDER BY total_spend DESC)` |
| Q16 | Top-percentile customers | `PERCENT_RANK() OVER (ORDER BY spend DESC)` |

### Module D — Advanced Product & Category Analytics

| Q# | Question | Key Window Technique |
|----|----------|----------------------|
| Q17 | Revenue volatility per product | `STDDEV() OVER (PARTITION BY product)` |
| Q18 | Trending categories detection | Recent vs historical avg with `AVG() OVER` frames |
| Q19 | Seasonality (same month, multi-year) | `PARTITION BY month` across years |
| Q20 | Profit consistency over time | Coefficient of variation using `STDDEV / AVG` per window |
| Q21 | Sustained decline detection | Consecutive period comparison via `LAG()` chain |

---

## 🤖 AI-Powered Recommendation System

The most sophisticated component of this project — a **pure SQL scoring engine** that ranks the 4 best companion products for any selected item, live in a Streamlit web app.

### How It Works

When a customer clicks on a product, the system evaluates **every other product** across 6 scored dimensions:

```
┌──────────────────────────────────────────────────────────┐
│              RECOMMENDATION SCORE FORMULA                │
│                                                          │
│  Score =  w₁ × Co-Purchase Frequency Score               │
│         + w₂ × Recency of Co-Purchase Score              │
│         + w₃ × Category Affinity Score                   │
│         + w₄ × Cross-Product Association Score           │
│         + w₅ × Profitability Score                       │
│         + w₆ × Stock Availability Score                  │
│                                                          │
│  → Each indicator normalized to [0, 1]                   │
│  → Products ranked by final composite score              │
│  → Top 4 surfaced as recommendations                     │
└──────────────────────────────────────────────────────────┘
```

### Scoring Dimensions Explained

| Dimension | Logic | Signal |
|-----------|-------|--------|
| 🛒 **Co-Purchase Frequency** | How often was this product bought in the same order as ours? | Revealed preference |
| 🕐 **Recency of Co-Purchase** | How recently are customers buying both together? | Demand freshness |
| 🏷️ **Category Affinity** | Is it in the same category or parent category? | Contextual relevance |
| 🔗 **Cross-Product Association** | Do these products logically complement each other? | Logical pairing |
| 💰 **Profitability Score** | Is this product high-margin, trending, or a high seller? | Business value |
| 📦 **Stock Availability** | Is this item actually available to sell right now? | Fulfillment risk |

### Live Demo — Streamlit in Snowflake

The recommendation engine is deployed as a **native Streamlit app inside Snowflake** — no external servers, no infrastructure to manage.

```
User opens the app
       │
       ▼
  Selects a product from dropdown (all products fetched from DWH)
       │
       ▼
  SQL recommendation query executes against Fact_Order_Line + Dimensions
       │
       ▼
  Scoring engine computes normalized scores for all candidate products
       │
       ▼
  Top 4 products displayed as recommendation cards with scores & details
```

> 📍 **Deployed on**: Snowflake Native Apps — Streamlit in Snowflake  
> 🔐 **Access**: Controlled via Snowflake role-based access control  
> ⚡ **Performance**: Query runs on a dedicated Snowflake virtual warehouse

---

## ❄️ Snowflake Data Warehouse Setup

### Warehouse Configuration

```sql

-- Database & Schema
CREATE DATABASE ECOMMERCE_DWH;
CREATE SCHEMA ECOMMERCE_DWH.ANALYTICS;

-- All dimension and fact tables are created within ANALYTICS schema
```

### Data Pipeline

```
Raw Data Generation (Scripts/)
          │
          ▼
   Synthetic Dataset (~50K+ order lines)
          │
          ▼
   Dimension Tables Populated First
   (Date → Customer → Product → Category → Payment → Shipping)
          │
          ▼
   Fact Table Loaded 
          │
          ▼
   Analytical Queries run on top of the warehouse
          │
          ▼
   Streamlit App queries live warehouse in real-time
```

---

## 🖥️ Streamlit App Deployment

The Recommendation System is deployed as a **Streamlit in Snowflake** app — meaning it runs entirely within Snowflake's cloud environment.

---

## 🚀 Getting Started

### Prerequisites

- A Snowflake account (free trial works)
- Basic familiarity with SQL

---

## 🧩 Technical Highlights

- **Zero application-side computation** — all analytics happen inside Snowflake SQL
- **21 window function queries** covering every major analytical pattern: `RANK`, `DENSE_RANK`, `ROW_NUMBER`, `NTILE`, `PERCENT_RANK`, `LAG`, `LEAD`, `SUM/AVG/STDDEV OVER`, sliding frames (`ROWS BETWEEN`)
- **Normalized scoring** — all recommendation indicators scaled to [0,1] before weighted aggregation
- **Star schema** with clean surrogate keys, enabling fast OLAP slicing and dicing
- **Streamlit-native deployment** — no external cloud, no Flask, no API layer needed

---

## 👥 Team

> Built with obsession over data, window functions, and the art of asking the right business questions.


---

<div align="center">

<br/>

**Built on ❄️ Snowflake · Deployed via 🖥️ Streamlit · Powered by 📐 Advanced SQL**

<br/>

*"We didn't just query data. We interrogated it."*

<br/>

⭐ **Star this repo if it helped you think bigger about analytical SQL** ⭐

</div>
