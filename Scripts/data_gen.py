from __future__ import annotations

import argparse
import csv
import io
import os
import random
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
from faker import Faker

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


# CONFIGURATION

@dataclass
class Config:
    date_start:  date = date(2021, 1, 1)
    date_end:    date = date(2024, 12, 31)
    n_customers: int  = 5_000
    n_products:  int  = 500
    n_brands:    int  = 60
    fact_rows:   int  = 200_000
    workers:     int  = max(1, os.cpu_count() or 2)
    chunk_size:  int  = 50_000
    seed:        int  = 42
    output_dir:  Path = Path("ecommerce_data")
    fmt:         str  = "csv"      
    compress:    bool = False


# STATIC REFERENCE DATA

CATEGORIES = [
    ("Electronics",  "Technology",  False),
    ("Mobiles",      "Technology",  False),
    ("Laptops",      "Technology",  False),
    ("Tablets",      "Technology",  False),
    ("Clothing",     "Fashion",     True),
    ("Footwear",     "Fashion",     True),
    ("Accessories",  "Fashion",     True),
    ("Watches",      "Fashion",     False),
    ("Home Decor",   "Home",        True),
    ("Furniture",    "Home",        False),
    ("Kitchen",      "Home",        False),
    ("Garden",       "Home",        True),
    ("Sports",       "Lifestyle",   True),
    ("Fitness",      "Lifestyle",   False),
    ("Outdoor",      "Lifestyle",   True),
    ("Books",        "Education",   False),
    ("Stationery",   "Education",   False),
    ("Toys",         "Kids",        True),
    ("Baby",         "Kids",        False),
    ("Beauty",       "Personal",    True),
    ("Health",       "Personal",    False),
    ("Groceries",    "Food",        True),
    ("Beverages",    "Food",        False),
    ("Automotive",   "Vehicles",    False),
    ("Pet Supplies", "Pets",        False),
]
N_CATEGORIES = len(CATEGORIES)

PAYMENT_METHODS = [
    "Credit Card", "Debit Card", "PayPal",
    "Cash on Delivery", "Bank Transfer", "Wallet",
    "Buy Now Pay Later", "Crypto",
]

SHIPPING_TYPES = [
    ("Standard",   7,  0.00),
    ("Express",    3,  4.99),
    ("Same Day",   1,  9.99),
    ("Economy",   14,  0.00),
    ("Overnight",  1, 14.99),
    ("Pickup",     1,  0.00),
]

AGE_GROUPS = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
GENDERS    = ["Male", "Female", "Other", "Prefer not to say"]
SEGMENTS   = ["New", "Regular", "VIP", "At-Risk"]
REGIONS    = ["North", "South", "East", "West", "Central", "Northeast", "Southwest"]
SUBCATS    = ["Entry", "Mid-range", "Premium", "Luxury", "Budget", "Professional"]

ORDER_STATUS_POOL = (
    ["Completed"] * 70 +
    ["Returned"]  * 20 +
    ["Cancelled"] * 10
)

DISC_POOL = [0] * 50 + [5] * 15 + [10] * 15 + [15] * 8 + [20] * 7 + [25] * 3 + [30] * 2
MAX_LINES_PER_ORDER = 6


# HELPERS

def _bar(done: int, total: int, width: int = 30) -> str:
    frac   = done / total if total else 1.0
    filled = int(width * frac)
    return f"[{'█' * filled}{'░' * (width - filled)}] {frac*100:5.1f}%"

def _date_range(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]

def _log(path: Path, n: int) -> None:
    print(f"  ✔  {path.name:<42} {n:>10,} rows")

def _save(rows: list[dict], path: Path, fields: list[str], cfg: Config) -> None:
    if cfg.fmt == "parquet" and HAS_PANDAS:
        df = pd.DataFrame(rows, columns=fields)
        df.to_parquet(path.with_suffix(".parquet"), index=False)
        _log(path.with_suffix(".parquet"), len(rows))
    else:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)
        _log(path, len(rows))


# DIMENSION GENERATORS

def gen_dim_date(cfg: Config) -> tuple[list[dict], dict[date, int]]:
    rows = []
    date_index: dict[date, int] = {}
    for key, d in enumerate(_date_range(cfg.date_start, cfg.date_end), start=1):
        rows.append({
            "date_key":    key,
            "full_date":   d.isoformat(),
            "day":         d.day,
            "month":       d.month,
            "month_name":  d.strftime("%B"),
            "quarter":     (d.month - 1) // 3 + 1,
            "year":        d.year,
            "week_number": d.isocalendar()[1],
        })
        date_index[d] = key
    return rows, date_index

def gen_dim_customer(cfg: Config, date_index: dict[date, int]) -> tuple[list[dict], np.ndarray]:
    rng  = random.Random(cfg.seed)
    fake = Faker(); Faker.seed(cfg.seed)
    rows: list[dict] = []
    earliest_dk = np.empty(cfg.n_customers, dtype=np.int32)

    for i in range(cfg.n_customers):
        reg_date = cfg.date_start + timedelta(days=rng.randint(0, (cfg.date_end - cfg.date_start).days))
        dk = date_index.get(reg_date, 1)
        earliest_dk[i] = dk
        rows.append({
            "customer_key":      i + 1,
            "customer_id":       f"CUST{i+1:07d}",
            "gender":            rng.choice(GENDERS),
            "age_group":         rng.choice(AGE_GROUPS),
            "city":              fake.city(),
            "region":            rng.choice(REGIONS),
            "registration_date": reg_date.isoformat(),
            "customer_segment":  rng.choice(SEGMENTS),
        })
    return rows, earliest_dk

def gen_dim_category() -> list[dict]:
    return [
        {
            "category_key":    i,
            "category_name":   name,
            "parent_category": parent,
            "seasonal_flag":   seasonal,
        }
        for i, (name, parent, seasonal) in enumerate(CATEGORIES, start=1)
    ]

def gen_dim_product(cfg: Config, date_index: dict[date, int]) -> tuple[list[dict], np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    rng  = random.Random(cfg.seed + 1)
    fake = Faker(); Faker.seed(cfg.seed + 1)
    brands = [fake.company().split()[0] for _ in range(cfg.n_brands)]

    p_cat, p_base, p_cost, p_edk = [], [], [], []
    rows, orderable = [], []

    for i in range(cfg.n_products):
        cat_key = rng.randint(1, N_CATEGORIES)
        base_price = round(rng.uniform(5, 2000), 2)
        cost_price = round(base_price * rng.uniform(0.4, 0.8), 2)
        launch_date = cfg.date_start + timedelta(days=rng.randint(0, (cfg.date_end - cfg.date_start).days))
        dk = date_index.get(launch_date, 1)
        stock = rng.randint(0, 5_000)

        p_cat.append(cat_key); p_base.append(base_price); p_cost.append(cost_price); p_edk.append(dk)
        if stock > 0: orderable.append(i)

        rows.append({
            "product_key":    i + 1,
            "product_id":     f"PROD{i+1:06d}",
            "product_name":   fake.catch_phrase(),
            "brand":          rng.choice(brands),
            "subcategory":    rng.choice(SUBCATS),
            "launch_date":    launch_date.isoformat(),
            "stock_quantity": stock,
        })
    return rows, np.array(p_cat), np.array(p_base), np.array(p_cost), np.array(p_edk), np.array(orderable)

def gen_dim_payment() -> list[dict]:
    return [{"payment_key": i + 1, "payment_method": m} for i, m in enumerate(PAYMENT_METHODS)]

def gen_dim_shipping() -> list[dict]:
    return [{"shipping_key": i + 1, "shipping_type": t, "delivery_days": d} for i, (t, d, c) in enumerate(SHIPPING_TYPES)]


# FACT TABLE GENERATOR

def _fact_chunk(chunk_id, n_rows, seed, n_dates, n_pay, n_ship, pcat, bprc, cprc, pedk, ord_idx, cedk) -> bytes:
    rng = np.random.default_rng(seed + chunk_id)
    py_rng = random.Random(seed + chunk_id)
    
    samp_pos = rng.integers(0, len(ord_idx), n_rows)
    p_keys = ord_idx[samp_pos]
    c_keys = rng.integers(0, len(cedk), n_rows)
    
    # Referential Integrity logic
    e_valid = np.minimum(np.maximum(pedk[p_keys], cedk[c_keys]), n_dates)
    d_keys = np.array([rng.integers(int(e_valid[i]), n_dates + 1) for i in range(n_rows)])

    # Order ID logic
    order_ids, i = [], 0
    curr_ord = chunk_id * 10_000_000
    while i < n_rows:
        lines = py_rng.randint(1, MAX_LINES_PER_ORDER + 1)
        lines = min(lines, n_rows - i)
        for _ in range(lines): order_ids.append(curr_ord)
        curr_ord += 1
        i += lines

    qty = rng.integers(1, 10, n_rows)
    unit_p = np.round(bprc[p_keys] * rng.uniform(0.9, 1.1, n_rows), 2)
    gross = np.round(qty * unit_p, 2)
    disc_pct = np.array(DISC_POOL)[rng.integers(0, len(DISC_POOL), n_rows)]
    discount = np.round(gross * disc_pct / 100.0, 2)
    net = np.round(gross - discount, 2)
    cost = np.round(net * (cprc[p_keys] / bprc[p_keys]), 2)
    profit = np.round(net - cost, 2)

    buf = io.StringIO()
    for j in range(n_rows):
        # Alignment with FACT_ORDER_LINE PK/FK structure
        buf.write(
            f"{(chunk_id * 1_000_000) + j + 1},ORD{order_ids[j]},{d_keys[j]},"
            f"{c_keys[j]+1},{p_keys[j]+1},{pcat[p_keys[j]]},"
            f"{rng.integers(1, n_pay+1)},{rng.integers(1, n_ship+1)},{qty[j]},"
            f"{gross[j]:.2f},{discount[j]:.2f},{net[j]:.2f},{cost[j]:.2f},{profit[j]:.2f}\n"
        )
    return buf.getvalue().encode()

FACT_FIELDS = ["order_line_id", "order_id", "date_key", "customer_key", "product_key", "category_key", "payment_key", "shipping_key", "quantity", "gross_amount", "discount_amount", "net_amount", "cost_amount", "profit_amount"]

def gen_fact_parallel(cfg, n_dates, pcat, bprc, cprc, pedk, ord_idx, cedk):
    n_chunks = (cfg.fact_rows + cfg.chunk_size - 1) // cfg.chunk_size
    with open(cfg.output_dir / "fact_order_line.csv", "wb") as f:
        f.write((",".join(FACT_FIELDS) + "\n").encode())
        with ProcessPoolExecutor(max_workers=cfg.workers) as pool:
            futures = [pool.submit(_fact_chunk, i, cfg.chunk_size if i < n_chunks-1 else cfg.fact_rows - (i*cfg.chunk_size), cfg.seed, n_dates, len(PAYMENT_METHODS), len(SHIPPING_TYPES), pcat, bprc, cprc, pedk, ord_idx, cedk) for i in range(n_chunks)]
            for fut in as_completed(futures): f.write(fut.result())
    _log(cfg.output_dir / "fact_order_line.csv", cfg.fact_rows)


# ORCHESTRATOR

DIM_FIELDS = {
    "dim_date":     ["date_key","full_date","day","month","month_name","quarter","year","week_number"],
    "dim_customer": ["customer_key","customer_id","gender","age_group","city","region","registration_date","customer_segment"],
    "dim_category": ["category_key","category_name","parent_category","seasonal_flag"],
    "dim_product":  ["product_key","product_id","product_name","brand","subcategory","launch_date","stock_quantity"],
    "dim_payment":  ["payment_key","payment_method"],
    "dim_shipping": ["shipping_key","shipping_type","delivery_days"],
}

def run(cfg: Config):
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    d_rows, d_idx = gen_dim_date(cfg)
    _save(d_rows, cfg.output_dir / "dim_date.csv", DIM_FIELDS["dim_date"], cfg)
    
    c_rows, cedk = gen_dim_customer(cfg, d_idx)
    _save(c_rows, cfg.output_dir / "dim_customer.csv", DIM_FIELDS["dim_customer"], cfg)
    
    _save(gen_dim_category(), cfg.output_dir / "dim_category.csv", DIM_FIELDS["dim_category"], cfg)
    
    p_rows, pcat, bprc, cprc, pedk, ord_idx = gen_dim_product(cfg, d_idx)
    _save(p_rows, cfg.output_dir / "dim_product.csv", DIM_FIELDS["dim_product"], cfg)
    
    _save(gen_dim_payment(), cfg.output_dir / "dim_payment.csv", DIM_FIELDS["dim_payment"], cfg)
    _save(gen_dim_shipping(), cfg.output_dir / "dim_shipping.csv", DIM_FIELDS["dim_shipping"], cfg)
    
    gen_fact_parallel(cfg, len(d_rows), pcat, bprc, cprc, pedk, ord_idx, cedk)

if __name__ == "__main__":
    run(Config(fact_rows=200_000)) # Default run