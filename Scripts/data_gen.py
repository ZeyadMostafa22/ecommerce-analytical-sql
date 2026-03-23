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
    fmt:         str  = "csv"      # "csv" | "parquet"
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
    ("Pickup",     0,  0.00),
]

AGE_GROUPS = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
GENDERS    = ["Male", "Female", "Other", "Prefer not to say"]
SEGMENTS   = ["New", "Regular", "Premium", "VIP", "Churned", "At-Risk"]
REGIONS    = ["North", "South", "East", "West", "Central", "Northeast", "Southwest"]
SUBCATS    = ["Entry", "Mid-range", "Premium", "Luxury", "Budget", "Professional"]

# Order status weights 
ORDER_STATUS_POOL = (
    ["Completed"] * 70 +
    ["Returned"]  * 20 +
    ["Cancelled"] * 10
)

# Discount % pool 
DISC_POOL = [0] * 50 + [5] * 15 + [10] * 15 + [15] * 8 + [20] * 7 + [25] * 3 + [30] * 2

# Max lines per order (realistic basket sizes)
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


def _write_csv(rows: list[dict], path: Path, fields: list[str], cfg: Config) -> None:
    if cfg.compress:
        import gzip
        opener = gzip.open(path.with_suffix(".csv.gz"), "wt", newline="", encoding="utf-8")
        final  = path.with_suffix(".csv.gz")
    else:
        opener = open(path, "w", newline="", encoding="utf-8")
        final  = path
    with opener as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    _log(final, len(rows))


def _write_parquet(rows: list[dict], path: Path, fields: list[str], cfg: Config) -> None:
    if not HAS_PANDAS:
        sys.exit("❌  Parquet output requires: pip install pandas pyarrow")
    df   = pd.DataFrame(rows, columns=fields)
    comp = "snappy" if cfg.compress else None
    out  = path.with_suffix(".parquet")
    df.to_parquet(out, index=False, compression=comp)
    _log(out, len(rows))


def _save(rows: list[dict], path: Path, fields: list[str], cfg: Config) -> None:
    if cfg.fmt == "parquet":
        _write_parquet(rows, path, fields, cfg)
    else:
        _write_csv(rows, path, fields, cfg)


# DIMENSION GENERATORS

def gen_dim_date(cfg: Config) -> tuple[list[dict], dict[date, int]]:
    """Returns rows + a {date: date_key} lookup for referential checks."""
    rows      = []
    date_index: dict[date, int] = {}
    for key, d in enumerate(_date_range(cfg.date_start, cfg.date_end), start=1):
        rows.append({
            "date_key":    key,
            "full_date":   d.isoformat(),
            "day":         d.day,
            "day_name":    d.strftime("%A"),
            "month":       d.month,
            "month_name":  d.strftime("%B"),
            "quarter":     (d.month - 1) // 3 + 1,
            "year":        d.year,
            "week_number": d.isocalendar()[1],
            "is_weekend":  int(d.weekday() >= 5),
            "is_holiday":  0,
        })
        date_index[d] = key
    return rows, date_index


def gen_dim_customer(cfg: Config, date_index: dict[date, int]) -> tuple[list[dict], np.ndarray]:
    rng  = random.Random(cfg.seed)
    fake = Faker(); Faker.seed(cfg.seed)

    reg_start = date(2018, 1, 1)
    # Registration must be within the date dimension range to be joinable
    reg_end   = cfg.date_end
    reg_span  = (reg_end - reg_start).days

    rows: list[dict] = []
    # earliest valid date_key per customer (for fact FK enforcement)
    earliest_dk = np.empty(cfg.n_customers, dtype=np.int32)

    for i in range(cfg.n_customers):
        reg_date = reg_start + timedelta(days=rng.randint(0, reg_span))
        # clamp: must exist in dim_date
        reg_date = min(reg_date, cfg.date_end)
        dk       = date_index.get(reg_date)
        if dk is None:           # fallback to first available date
            dk = 1
            reg_date = cfg.date_start
        earliest_dk[i] = dk
        rows.append({
            "customer_key":      i + 1,
            "customer_id":       f"CUST{i+1:07d}",
            "first_name":        fake.first_name(),
            "last_name":         fake.last_name(),
            "email":             fake.email(),
            "phone":             fake.phone_number(),
            "gender":            rng.choice(GENDERS),
            "age_group":         rng.choice(AGE_GROUPS),
            "city":              fake.city(),
            "state":             fake.state(),
            "country":           fake.country_code(),
            "region":            rng.choice(REGIONS),
            "registration_date": reg_date.isoformat(),
            "customer_segment":  rng.choice(SEGMENTS),
            "loyalty_points":    rng.randint(0, 50_000),
        })
    return rows, earliest_dk


def gen_dim_category() -> list[dict]:
    return [
        {
            "category_key":    i,
            "category_name":   name,
            "parent_category": parent,
            "seasonal_flag":   int(seasonal),
        }
        for i, (name, parent, seasonal) in enumerate(CATEGORIES, start=1)
    ]


def gen_dim_product(
    cfg: Config,
    date_index: dict[date, int],
) -> tuple[list[dict], np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    rng  = random.Random(cfg.seed + 1)
    fake = Faker(); Faker.seed(cfg.seed + 1)

    brands       = [fake.company().split()[0] for _ in range(cfg.n_brands)]
    launch_start = date(2015, 1, 1)
    launch_span  = (cfg.date_end - launch_start).days

    product_category_keys = np.empty(cfg.n_products, dtype=np.int32)
    product_base_prices   = np.empty(cfg.n_products, dtype=np.float64)
    product_cost_prices   = np.empty(cfg.n_products, dtype=np.float64)
    product_earliest_dk   = np.empty(cfg.n_products, dtype=np.int32)

    rows: list[dict] = []
    orderable: list[int] = []   # 0-based indices of active + in-stock products

    for i in range(cfg.n_products):
        cat_key    = rng.randint(1, N_CATEGORIES)
        base_price = round(rng.uniform(5, 2000), 2)
        margin     = rng.uniform(0.20, 0.60)
        cost_price = round(base_price * (1 - margin), 2)

        launch_date = launch_start + timedelta(days=rng.randint(0, launch_span))
        launch_date = min(launch_date, cfg.date_end)
        dk = date_index.get(launch_date, 1)

        # Active = 95% chance; stock must be > 0 to be orderable
        is_active     = int(rng.random() > 0.05)
        stock_quantity = rng.randint(0, 5_000)

        product_category_keys[i] = cat_key
        product_base_prices[i]   = base_price
        product_cost_prices[i]   = cost_price
        product_earliest_dk[i]   = dk

        # Only mark as orderable if active AND has stock
        if is_active and stock_quantity > 0:
            orderable.append(i)

        rows.append({
            "product_key":    i + 1,
            "product_id":     f"PROD{i+1:06d}",
            "product_name":   fake.catch_phrase(),
            "brand":          rng.choice(brands),
            "subcategory":    rng.choice(SUBCATS),
            "category_key":   cat_key,
            "launch_date":    launch_date.isoformat(),
            "base_price":     base_price,
            "cost_price":     cost_price,
            "stock_quantity": stock_quantity,
            "is_active":      is_active,
            "weight_kg":      round(rng.uniform(0.1, 30.0), 2),
            "rating":         round(rng.uniform(1.0, 5.0), 1),
            "review_count":   rng.randint(0, 10_000),
        })

    if not orderable:
        raise ValueError("No orderable products generated — increase n_products or relax filters")

    return rows, product_category_keys, product_base_prices, product_cost_prices, product_earliest_dk, np.array(orderable, dtype=np.int32)


def gen_dim_payment() -> list[dict]:
    return [
        {"payment_key": i + 1, "payment_method": m, "is_digital": int(m != "Cash on Delivery")}
        for i, m in enumerate(PAYMENT_METHODS)
    ]


def gen_dim_shipping() -> list[dict]:
    return [
        {
            "shipping_key":  i + 1,
            "shipping_type": t,
            "delivery_days": d,
            "base_cost":     c,
            "is_trackable":  int(t != "Pickup"),
        }
        for i, (t, d, c) in enumerate(SHIPPING_TYPES)
    ]


# FACT TABLE – vectorized + referentially correct

def _fact_chunk(
    chunk_id:              int,
    n_rows:                int,
    seed:                  int,
    n_dates:               int,
    n_payments:            int,
    n_shipping:            int,
    # full product lookup arrays (indexed by product_key - 1)
    product_category_keys: list[int],
    product_base_prices:   list[float],
    product_cost_prices:   list[float],
    product_earliest_dk:   list[int],
    # ONLY 0-based indices of active=1 AND stock>0 products
    orderable_indices:     list[int],
    # customer lookup arrays
    customer_earliest_dk:  list[int],
) -> bytes:
    rng    = np.random.default_rng(seed + chunk_id * 997)
    py_rng = random.Random(seed + chunk_id * 997)

    n_orderable = len(orderable_indices)

    # Sample positions within orderable_indices → guarantees active + in-stock
    sampled_pos   = rng.integers(0, n_orderable, n_rows)
    product_keys  = np.array(orderable_indices)[sampled_pos]   # 0-based product index
    customer_keys = rng.integers(0, len(customer_earliest_dk), n_rows)  # 0-indexed

    pcat  = np.array(product_category_keys)[product_keys]
    bprc  = np.array(product_base_prices)[product_keys]
    cprc  = np.array(product_cost_prices)[product_keys]
    p_edk = np.array(product_earliest_dk)[product_keys]
    c_edk = np.array(customer_earliest_dk)[customer_keys]

    earliest_valid = np.maximum(p_edk, c_edk)
    date_keys = np.array([
        rng.integers(int(earliest_valid[i]), n_dates + 1)
        if earliest_valid[i] <= n_dates
        else n_dates
        for i in range(n_rows)
    ], dtype=np.int64)

    order_ids = []
    current_order = chunk_id * 10_000_000   # unique across chunks
    i = 0
    while i < n_rows:
        lines = py_rng.randint(1, MAX_LINES_PER_ORDER + 1)
        lines = min(lines, n_rows - i)
        for _ in range(lines):
            order_ids.append(current_order)
        current_order += 1
        i += lines
    order_ids_arr = np.array(order_ids[:n_rows], dtype=np.int64)

    payment_keys  = rng.integers(1, n_payments  + 1, n_rows)
    shipping_keys = rng.integers(1, n_shipping  + 1, n_rows)

    qty = rng.integers(1, 21, n_rows)

    price_factor = rng.uniform(0.90, 1.10, n_rows)
    unit_price   = np.round(bprc * price_factor, 2)

    disc_idx = rng.integers(0, len(DISC_POOL), n_rows)
    disc_pct = np.array(DISC_POOL, dtype=np.float64)[disc_idx]

    gross    = np.round(qty * unit_price, 2)
    discount = np.round(gross * disc_pct / 100.0, 2)
    net      = np.round(gross - discount, 2)

    cost_ratio = np.where(unit_price > 0, cprc / bprc, 0.5)
    cost       = np.round(net * cost_ratio, 2)
    profit     = np.round(net - cost, 2)

    status_arr = np.array([
        py_rng.choice(ORDER_STATUS_POOL) for _ in range(n_rows)
    ])

    is_returned  = (status_arr == "Returned")
    is_cancelled = (status_arr == "Cancelled")

    # Returned: reverse the financials (refund)
    net[is_returned]      = -net[is_returned]
    cost[is_returned]     = -cost[is_returned]
    profit[is_returned]   = -profit[is_returned]
    discount[is_returned] = -discount[is_returned]
    gross[is_returned]    = -gross[is_returned]

    # Cancelled: zero out everything
    for arr in (gross, discount, net, cost, profit):
        arr[is_cancelled] = 0.0

    # serialize to CSV bytes
    buf = io.StringIO()
    for i in range(n_rows):
        buf.write(
            f"{order_ids_arr[i]},"
            f"{date_keys[i]},"
            f"{int(customer_keys[i]) + 1},"       # back to 1-indexed
            f"{int(product_keys[i]) + 1},"        # back to 1-indexed
            f"{int(pcat[i])},"
            f"{int(payment_keys[i])},"
            f"{int(shipping_keys[i])},"
            f"{int(qty[i])},"
            f"{unit_price[i]:.2f},"
            f"{gross[i]:.2f},"
            f"{discount[i]:.2f},"
            f"{net[i]:.2f},"
            f"{cost[i]:.2f},"
            f"{profit[i]:.2f},"
            f"{status_arr[i]}\n"
        )
    return buf.getvalue().encode()


FACT_FIELDS = [
    "order_id", "date_key", "customer_key", "product_key", "category_key",
    "payment_key", "shipping_key", "quantity", "unit_price",
    "gross_amount", "discount_amount", "net_amount", "cost_amount",
    "profit_amount", "order_status",
]
FACT_HEADER = ",".join(FACT_FIELDS) + "\n"


def gen_fact_parallel(
    cfg: Config,
    n_dates:               int,
    product_category_keys: np.ndarray,
    product_base_prices:   np.ndarray,
    product_cost_prices:   np.ndarray,
    product_earliest_dk:   np.ndarray,
    orderable_indices:     np.ndarray,
    customer_earliest_dk:  np.ndarray,
) -> None:
    total    = cfg.fact_rows
    cs       = cfg.chunk_size
    n_chunks = (total + cs - 1) // cs
    sizes    = [cs] * n_chunks
    sizes[-1] = total - cs * (n_chunks - 1)

    out_path = cfg.output_dir / "fact_order_line.csv"
    if cfg.compress:
        import gzip
        opener = gzip.open(cfg.output_dir / "fact_order_line.csv.gz", "wb")
        final  = cfg.output_dir / "fact_order_line.csv.gz"
    else:
        opener = open(out_path, "wb")
        final  = out_path

    # Convert numpy arrays to plain lists for pickling across processes
    pcat_list  = product_category_keys.tolist()
    bprc_list  = product_base_prices.tolist()
    cprc_list  = product_cost_prices.tolist()
    p_edk_list = product_earliest_dk.tolist()
    ord_list   = orderable_indices.tolist()
    c_edk_list = customer_earliest_dk.tolist()
    print(f"  ℹ  Orderable products (active + in-stock): {len(ord_list):,} / {len(pcat_list):,}")

    t0      = time.perf_counter()
    written = 0

    with opener as fout:
        fout.write(FACT_HEADER.encode())
        futures_map: dict = {}
        with ProcessPoolExecutor(max_workers=cfg.workers) as pool:
            for cid, n in enumerate(sizes):
                fut = pool.submit(
                    _fact_chunk,
                    cid, n, cfg.seed,
                    n_dates,
                    len(PAYMENT_METHODS), len(SHIPPING_TYPES),
                    pcat_list, bprc_list, cprc_list, p_edk_list,
                    ord_list,
                    c_edk_list,
                )
                futures_map[fut] = n

            for fut in as_completed(futures_map):
                fout.write(fut.result())
                written  += futures_map[fut]
                elapsed   = time.perf_counter() - t0
                rate      = written / elapsed if elapsed else 0
                print(
                    f"\r  {_bar(written, total)}  {written:>10,}/{total:,}  "
                    f"{rate:,.0f} rows/s   ",
                    end="", flush=True,
                )

    print()
    _log(final, total)


# ORCHESTRATOR

DIM_FIELDS = {
    "dim_date":     ["date_key","full_date","day","day_name","month","month_name",
                     "quarter","year","week_number","is_weekend","is_holiday"],
    "dim_customer": ["customer_key","customer_id","first_name","last_name","email",
                     "phone","gender","age_group","city","state","country","region",
                     "registration_date","customer_segment","loyalty_points"],
    "dim_category": ["category_key","category_name","parent_category","seasonal_flag"],
    "dim_product":  ["product_key","product_id","product_name","brand","subcategory",
                     "category_key","launch_date","base_price","cost_price",
                     "stock_quantity","is_active","weight_kg","rating","review_count"],
    "dim_payment":  ["payment_key","payment_method","is_digital"],
    "dim_shipping": ["shipping_key","shipping_type","delivery_days","base_cost","is_trackable"],
}


def run(cfg: Config) -> None:
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    t_total = time.perf_counter()

    print(f"\n{'─'*62}")
    print(f"  E-Commerce DWH Data Generator  (logically correct + RI)")
    print(f"  seed={cfg.seed}  workers={cfg.workers}  fmt={cfg.fmt}  compress={cfg.compress}")
    print(f"  customers={cfg.n_customers:,}  products={cfg.n_products:,}  "
          f"fact_rows={cfg.fact_rows:,}")
    print(f"{'─'*62}\n")

    print("📐  Dimensions")

    # 1. Date
    dim_date_rows, date_index = gen_dim_date(cfg)
    _save(dim_date_rows, cfg.output_dir / "dim_date.csv", DIM_FIELDS["dim_date"], cfg)
    n_dates = len(dim_date_rows)

    # 2. Customer  (needs date_index for registration clamping)
    dim_customer_rows, customer_earliest_dk = gen_dim_customer(cfg, date_index)
    _save(dim_customer_rows, cfg.output_dir / "dim_customer.csv", DIM_FIELDS["dim_customer"], cfg)

    # 3. Category
    dim_category_rows = gen_dim_category()
    _save(dim_category_rows, cfg.output_dir / "dim_category.csv", DIM_FIELDS["dim_category"], cfg)

    # 4. Product  (needs date_index for launch date clamping)
    dim_product_rows, prod_cat_keys, prod_base, prod_cost, prod_earliest_dk, orderable_idx = gen_dim_product(cfg, date_index)
    _save(dim_product_rows, cfg.output_dir / "dim_product.csv", DIM_FIELDS["dim_product"], cfg)

    # 5. Payment / Shipping
    dim_payment_rows  = gen_dim_payment()
    dim_shipping_rows = gen_dim_shipping()
    _save(dim_payment_rows,  cfg.output_dir / "dim_payment.csv",  DIM_FIELDS["dim_payment"],  cfg)
    _save(dim_shipping_rows, cfg.output_dir / "dim_shipping.csv", DIM_FIELDS["dim_shipping"], cfg)

    # 6. Fact
    print(f"\n⚡  Fact table  ({cfg.fact_rows:,} rows, {cfg.workers} workers)")
    gen_fact_parallel(
        cfg, n_dates,
        prod_cat_keys, prod_base, prod_cost, prod_earliest_dk,
        orderable_idx,
        customer_earliest_dk,
    )

    elapsed = time.perf_counter() - t_total
    print(f"\n✅  Done in {elapsed:.2f}s  ({cfg.fact_rows / elapsed:,.0f} fact rows/s)")
    print(f"   Output → {cfg.output_dir.resolve()}\n")


# CLI

def _cli() -> Config:
    p = argparse.ArgumentParser(
        description="E-Commerce DWH generator — logically correct + referential integrity",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--date-start",  default="2021-01-01")
    p.add_argument("--date-end",    default="2024-12-31")
    p.add_argument("--customers",   type=int, default=5_000)
    p.add_argument("--products",    type=int, default=500)
    p.add_argument("--brands",      type=int, default=60)
    p.add_argument("--fact-rows",   type=int, default=200_000)
    p.add_argument("--workers",     type=int, default=max(1, os.cpu_count() or 2))
    p.add_argument("--chunk-size",  type=int, default=50_000)
    p.add_argument("--seed",        type=int, default=42)
    p.add_argument("--output-dir",  default="ecommerce_data")
    p.add_argument("--fmt",         choices=["csv", "parquet"], default="csv")
    p.add_argument("--compress",    action="store_true")
    a = p.parse_args()
    return Config(
        date_start  = date.fromisoformat(a.date_start),
        date_end    = date.fromisoformat(a.date_end),
        n_customers = a.customers,
        n_products  = a.products,
        n_brands    = a.brands,
        fact_rows   = a.fact_rows,
        workers     = a.workers,
        chunk_size  = a.chunk_size,
        seed        = a.seed,
        output_dir  = Path(a.output_dir),
        fmt         = a.fmt,
        compress    = a.compress,
    )


if __name__ == "__main__":
    run(_cli())