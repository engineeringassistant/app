"""
================================================================================
DATA PREPROCESSOR - Run weekly (Sunday 2 AM) to download and compute data
================================================================================
This script downloads AMFI data, fetches NAVs, computes rankings, and saves
pre-processed JSON files that the main API will serve instantly.
================================================================================
"""

import requests
import pandas as pd
import numpy as np
import json
import pickle
import os
from datetime import datetime
import time

# ============================================================================
# CONFIGURATION
# ============================================================================
RISK_FREE_RATE = 6.5
DATA_DIR = "precomputed_data"  # Directory to store pre-computed data

# Create data directory if it doesn't exist
os.makedirs(DATA_DIR, exist_ok=True)

# Benchmark mapping
BENCHMARKS = {
    "large": "120716",   # HDFC Nifty 50 Index Direct
    "mid": "120823",     # Motilal Oswal Nifty Midcap 150 Index Direct
    "small": "145552",   # Nippon Nifty Smallcap 250 Index Direct
    "flexi": "120716",   # Nifty 50 proxy
    "gold": "119230",    # Nippon Gold BeES FoF Direct
    "us": "120684",      # Motilal Oswal Nasdaq 100 FoF Direct
    "hybrid": "120716",
    "elss": "120716",
    "baf": "120716",
}

CATEGORIES = {
    "Nifty 50 Index": {"keywords": ["nifty 50 index"], "bench": "large", "is_index": True},
    "Nifty Next 50": {"keywords": ["nifty next 50"], "bench": "large", "is_index": True},
    "Large Cap": {"keywords": ["large cap fund"], "bench": "large", "is_index": False},
    "Large & Mid Cap": {"keywords": ["large and mid cap", "large mid cap"], "bench": "large", "is_index": False},
    "Mid Cap": {"keywords": ["mid cap fund"], "bench": "mid", "is_index": False},
    "Small Cap": {"keywords": ["small cap fund"], "bench": "small", "is_index": False},
    "Flexi Cap": {"keywords": ["flexi cap fund"], "bench": "flexi", "is_index": False},
    "Multi Cap": {"keywords": ["multi cap fund"], "bench": "flexi", "is_index": False},
    "ELSS Tax Saving": {"keywords": ["elss", "tax saver", "tax saving"], "bench": "elss", "is_index": False},
    "Value Fund": {"keywords": ["value fund"], "bench": "large", "is_index": False},
    "Contra Fund": {"keywords": ["contra fund"], "bench": "large", "is_index": False},
    "Focused Fund": {"keywords": ["focused fund"], "bench": "large", "is_index": False},
    "Dividend Yield": {"keywords": ["dividend yield"], "bench": "large", "is_index": False},
    "Sectoral / Thematic": {"keywords": ["sectoral", "thematic"], "bench": "large", "is_index": False},
    "Aggressive Hybrid": {"keywords": ["aggressive hybrid"], "bench": "hybrid", "is_index": False},
    "Balanced Advantage": {"keywords": ["balanced advantage", "dynamic asset allocation"], "bench": "baf", "is_index": False},
    "Equity Savings": {"keywords": ["equity savings"], "bench": "hybrid", "is_index": False},
    "Conservative Hybrid": {"keywords": ["conservative hybrid"], "bench": "hybrid", "is_index": False},
    "Multi Asset Allocation": {"keywords": ["multi asset"], "bench": "hybrid", "is_index": False},
    "Arbitrage Fund": {"keywords": ["arbitrage"], "bench": "hybrid", "is_index": False},
    "Liquid Fund": {"keywords": ["liquid fund"], "bench": "hybrid", "is_index": False},
    "Ultra Short Duration": {"keywords": ["ultra short duration"], "bench": "hybrid", "is_index": False},
    "Low Duration": {"keywords": ["low duration"], "bench": "hybrid", "is_index": False},
    "Money Market": {"keywords": ["money market"], "bench": "hybrid", "is_index": False},
    "Corporate Bond": {"keywords": ["corporate bond"], "bench": "hybrid", "is_index": False},
    "Banking & PSU": {"keywords": ["banking and psu", "banking & psu"], "bench": "hybrid", "is_index": False},
    "Gilt Fund": {"keywords": ["gilt fund"], "bench": "hybrid", "is_index": False},
    "Dynamic Bond": {"keywords": ["dynamic bond"], "bench": "hybrid", "is_index": False},
    "Short Duration": {"keywords": ["short duration"], "bench": "hybrid", "is_index": False},
    "US / International": {"keywords": ["s&p 500", "nasdaq 100", "international", "overseas"], "bench": "us", "is_index": False},
    "Gold Fund": {"keywords": ["gold etf fund of fund", "gold savings", "gold fund"], "bench": "gold", "is_index": True},
    "Retirement Fund": {"keywords": ["retirement"], "bench": "hybrid", "is_index": False},
    "Children's Fund": {"keywords": ["child", "children"], "bench": "hybrid", "is_index": False},
}

TRUSTED_AMCS = [
    "HDFC", "ICICI", "SBI", "KOTAK", "NIPPON", "AXIS", "MIRAE",
    "DSP", "UTI", "FRANKLIN", "ADITYA BIRLA", "TATA", "BANDHAN",
    "MOTILAL", "PPFAS", "PARAG PARIKH", "WHITEOAK", "CANARA",
    "QUANTUM", "EDELWEISS", "INVESCO", "SUNDARAM", "HSBC",
    "BARODA BNP", "NAVI", "LIC MF", "ITI", "BAJAJ FINSERV",
]

# ============================================================================
# DATA DOWNLOAD FUNCTIONS
# ============================================================================

def download_amfi_active_funds():
    """Download AMFI active fund list"""
    print("📥 Downloading AMFI active fund list...")
    url = "https://www.amfiindia.com/spages/NAVAll.txt"
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
        resp.encoding = "utf-8"
        records, current_amc = [], ""
        
        for line in resp.text.splitlines():
            line = line.strip()
            if not line:
                continue
            if ";" not in line:
                current_amc = line
                continue
            parts = line.split(";")
            if len(parts) < 5:
                continue
            try:
                code = parts[0].strip()
                name = parts[3].strip()
                nav = float(parts[4].strip())
                if not code.isdigit():
                    continue
                records.append({"code": code, "name": name, "nav": nav, "amc": current_amc})
            except Exception:
                continue
        
        df = pd.DataFrame(records)
        
        # Filter segregated portfolios
        exclude_patterns = ["SEGREGATED", "SIDE POCKET", "MATURITY", "CLOSED", "FIXED TERM", "FMP"]
        for pat in exclude_patterns:
            df = df[~df["name"].str.upper().str.contains(pat, na=False)]
        
        # Direct Growth only
        mask = (
            df["name"].str.upper().str.contains("DIRECT", na=False) &
            df["name"].str.upper().str.contains("GROWTH", na=False) &
            ~df["name"].str.upper().str.contains("IDCW", na=False) &
            ~df["name"].str.upper().str.contains("DIVID", na=False)
        )
        direct = df[mask].reset_index(drop=True)
        print(f"  ✅ Active Direct Growth funds: {len(direct)}")
        return direct
    except Exception as e:
        print(f"  ⚠️ AMFI error: {e}")
        return pd.DataFrame()

def search_amfi(amfi_df, keywords):
    """Search AMFI data by keywords"""
    if amfi_df is None or amfi_df.empty:
        return pd.DataFrame()
    mask = pd.Series([False] * len(amfi_df))
    for kw in keywords:
        mask = mask | amfi_df["name"].str.lower().str.contains(kw.lower(), na=False)
    result = amfi_df[mask].copy()
    result["trusted"] = result["name"].str.upper().apply(
        lambda x: any(a in x for a in TRUSTED_AMCS))
    return result.reset_index(drop=True)

def fetch_nav_series(scheme_code):
    """Fetch NAV history from MFapi"""
    try:
        resp = requests.get(f"https://api.mfapi.in/mf/{scheme_code}", timeout=15)
        if resp.status_code != 200:
            return None, None
        data = resp.json()
        nav_list = data.get("data", [])
        if len(nav_list) < 60:
            return None, None
        navs = []
        for d in nav_list:
            try:
                navs.append({
                    "date": datetime.strptime(d["date"], "%d-%m-%Y"),
                    "nav": float(d["nav"]),
                })
            except Exception:
                continue
        df = pd.DataFrame(navs).sort_values("date").reset_index(drop=True)
        
        if df["nav"].iloc[-1] < 1.0:
            return None, None
        
        return df.set_index("date")["nav"], data.get("meta", {})
    except Exception:
        return None, None

def get_benchmark_series(bench_key):
    """Get benchmark NAV series with caching"""
    code = BENCHMARKS.get(bench_key)
    if not code:
        return None
    series, _ = fetch_nav_series(code)
    return series

def compute_metrics(nav_series, bench_series, scheme_name, is_index):
    """Compute all metrics"""
    if nav_series is None or len(nav_series) < 60:
        return None

    nav = nav_series
    cur = nav.iloc[-1]

    def ret(n):
        return round(((cur - nav.iloc[-(n+1)]) / nav.iloc[-(n+1)]) * 100, 2) \
               if len(nav) > n else None

    r1y = ret(252)
    r3y = ret(756)
    
    cagr_3y = None
    if r3y is not None:
        cagr_3y = round(((1 + r3y/100)**(1/3) - 1) * 100, 2)

    # Sharpe Ratio
    sharpe_3y = None
    if len(nav) >= 756:
        monthly_nav = nav.tail(756).resample("ME").last().dropna()
        if len(monthly_nav) >= 24:
            monthly_ret = monthly_nav.pct_change().dropna() * 100
            rf_monthly = RISK_FREE_RATE / 12
            excess_ret = monthly_ret - rf_monthly
            if excess_ret.std() > 0:
                sharpe_3y = round((excess_ret.mean() / excess_ret.std()) * np.sqrt(12), 3)

    daily_ret = nav.pct_change().dropna()
    vol_1y = None
    if len(daily_ret) >= 252:
        vol_1y = round(daily_ret.tail(252).std() * np.sqrt(252) * 100, 2)

    sharpe = sharpe_3y if sharpe_3y is not None else None

    # Max drawdown
    nav_1y = nav.tail(252)
    roll_max = nav_1y.cummax()
    max_dd = round(((nav_1y - roll_max) / roll_max * 100).min(), 2) if len(nav_1y) > 0 else None

    # Benchmark metrics
    beta = alpha = r_squared = tracking_err = info_ratio = None
    bench_ret_1y = outperformance = None

    if bench_series is not None and len(bench_series) >= 252:
        try:
            bench_daily = bench_series.pct_change().dropna()
            fund_d = daily_ret.tail(252)
            bench_d = bench_daily.reindex(fund_d.index, method="nearest")
            common = pd.DataFrame({"fund": fund_d, "bench": bench_d}).dropna()

            if len(common) >= 60:
                cov = common["fund"].cov(common["bench"])
                var = common["bench"].var()
                corr = common["fund"].corr(common["bench"])

                if var > 0:
                    beta = round(cov / var, 3)
                if not np.isnan(corr):
                    r_squared = round(corr**2, 3)

                if len(bench_series) >= 253:
                    bench_ret_1y = round(
                        ((bench_series.iloc[-1] - bench_series.iloc[-253]) /
                         bench_series.iloc[-253]) * 100, 2
                    )

                if beta is not None and r1y and bench_ret_1y:
                    expected = RISK_FREE_RATE + beta * (bench_ret_1y - RISK_FREE_RATE)
                    alpha = round(r1y - expected, 2)

                diff = common["fund"] - common["bench"]
                tracking_err = round(diff.std() * np.sqrt(252) * 100, 2)

                if tracking_err and tracking_err > 0 and r1y and bench_ret_1y:
                    info_ratio = round((r1y - bench_ret_1y) / tracking_err, 3)

                if r1y and bench_ret_1y:
                    outperformance = round(r1y - bench_ret_1y, 2)
        except Exception:
            pass

    return {
        "name": scheme_name[:55],
        "nav": round(cur, 2),
        "ret_1y": r1y,
        "cagr_3y": cagr_3y,
        "vol_1y": vol_1y,
        "max_dd": max_dd,
        "sharpe": sharpe,
        "beta": beta,
        "alpha": alpha,
        "r_squared": r_squared,
        "tracking_err": tracking_err,
        "info_ratio": info_ratio,
        "outperformance": outperformance,
    }

def score_fund_peer_ranked(m, is_index, trusted, category_metrics):
    """Score fund relative to category peers"""
    if not m or not category_metrics:
        return 0, []

    score = 0

    def percentile_rank(value, all_values, higher_better=True):
        if value is None:
            return None
        valid = [v for v in all_values if v is not None]
        if not valid:
            return None
        if higher_better:
            rank = sum(1 for v in valid if v <= value) / len(valid) * 100
        else:
            rank = sum(1 for v in valid if v >= value) / len(valid) * 100
        return round(rank, 1)

    all_ret1y = [c.get("ret_1y") for c in category_metrics]
    all_cagr3y = [c.get("cagr_3y") for c in category_metrics]
    all_sharpe = [c.get("sharpe") for c in category_metrics]
    all_alpha = [c.get("alpha") for c in category_metrics]

    pct = percentile_rank(m.get("ret_1y"), all_ret1y)
    if pct is not None:
        score += pct * 30 / 100

    pct = percentile_rank(m.get("cagr_3y"), all_cagr3y)
    if pct is not None:
        score += pct * 25 / 100

    pct = percentile_rank(m.get("sharpe"), all_sharpe)
    if pct is not None:
        score += pct * 25 / 100

    if not is_index:
        pct = percentile_rank(m.get("alpha"), all_alpha)
        if pct is not None:
            score += pct * 15 / 100

    if trusted:
        score += 5

    return round(min(score, 100), 1), []

# ============================================================================
# MAIN PREPROCESSING
# ============================================================================

def precompute_all_data():
    """Main function to precompute all data"""
    print("=" * 60)
    print("  🔄 STARTING DATA PREPROCESSING")
    print(f"  {datetime.now().strftime('%d %b %Y, %I:%M %p')}")
    print("=" * 60)
    
    # Step 1: Download AMFI data
    print("\n📊 Step 1: Downloading AMFI data...")
    amfi_data = download_amfi_active_funds()
    
    if amfi_data.empty:
        print("❌ Failed to download AMFI data. Exiting...")
        return
    
    # Save AMFI data for backup
    amfi_data.to_csv(f"{DATA_DIR}/amfi_funds.csv", index=False)
    print(f"   ✅ Saved {len(amfi_data)} funds to {DATA_DIR}/amfi_funds.csv")
    
    # Step 2: Precompute benchmark series
    print("\n📈 Step 2: Loading benchmark series...")
    benchmark_series_map = {}
    for bench_key in BENCHMARKS.keys():
        series = get_benchmark_series(bench_key)
        if series is not None:
            benchmark_series_map[bench_key] = series
            print(f"   ✅ Loaded benchmark: {bench_key}")
        time.sleep(0.5)
    
    # Save benchmark series
    with open(f"{DATA_DIR}/benchmarks.pkl", "wb") as f:
        pickle.dump(benchmark_series_map, f)
    print(f"   ✅ Saved {len(benchmark_series_map)} benchmarks")
    
    # Step 3: Process each category
    print("\n📂 Step 3: Processing categories...")
    all_categories_data = {}
    all_funds_by_category = {}
    
    for cat_name, cat_config in CATEGORIES.items():
        print(f"\n   🔍 Processing: {cat_name}")
        keywords = cat_config["keywords"]
        bench_key = cat_config["bench"]
        is_index = cat_config["is_index"]
        
        bench_series = benchmark_series_map.get(bench_key)
        matched = search_amfi(amfi_data, keywords)
        
        if matched.empty:
            print(f"      ⚠️ No funds found")
            all_categories_data[cat_name] = {"funds": [], "benchmark": bench_key}
            continue
        
        print(f"      📊 Found {len(matched)} funds")
        
        # Calculate metrics for top 50 funds
        all_metrics = []
        for _, row in matched.head(50).iterrows():
            nav_series, _ = fetch_nav_series(row["code"])
            if nav_series is None:
                continue
            
            m = compute_metrics(nav_series, bench_series, row["name"], is_index)
            if m:
                m["code"] = row["code"]
                m["trusted"] = bool(row.get("trusted", False))
                all_metrics.append(m)
            
            time.sleep(0.1)
        
        if not all_metrics:
            all_categories_data[cat_name] = {"funds": [], "benchmark": bench_key}
            continue
        
        # Score and rank
        scored_funds = []
        for m in all_metrics:
            score, _ = score_fund_peer_ranked(m, is_index, m["trusted"], all_metrics)
            m["score"] = score
            scored_funds.append(m)
        
        scored_funds.sort(key=lambda x: x.get("score", 0), reverse=True)
        
        # Format for API
        funds_response = []
        for idx, fund in enumerate(scored_funds[:20]):
            funds_response.append({
                "name": fund["name"],
                "scheme_code": fund["code"],
                "nav": fund["nav"],
                "ret_1y": fund.get("ret_1y"),
                "cagr_3y": fund.get("cagr_3y"),
                "sharpe": fund.get("sharpe"),
                "alpha": fund.get("alpha"),
                "beta": fund.get("beta"),
                "info_ratio": fund.get("info_ratio"),
                "max_dd": fund.get("max_dd"),
                "outperformance": fund.get("outperformance"),
                "score": fund["score"],
                "rank": idx + 1,
                "is_index": is_index
            })
        
        all_categories_data[cat_name] = {
            "benchmark": get_benchmark_name(bench_key),
            "funds": funds_response
        }
        
        print(f"      ✅ Processed {len(funds_response)} funds")
    
    # Step 4: Save all precomputed data
    print("\n💾 Step 4: Saving precomputed data...")
    
    # Save categories data as JSON
    with open(f"{DATA_DIR}/categories_data.json", "w") as f:
        json.dump(all_categories_data, f, indent=2)
    
    # Also save categories list separately for quick access
    categories_list = []
    for cat_name, cat_config in CATEGORIES.items():
        categories_list.append({
            "name": cat_name,
            "type": get_category_type(cat_name),
            "fund_count": len(all_categories_data.get(cat_name, {}).get("funds", [])),
            "is_index": cat_config["is_index"],
            "benchmark": get_benchmark_name(cat_config["bench"])
        })
    
    with open(f"{DATA_DIR}/categories.json", "w") as f:
        json.dump(categories_list, f, indent=2)
    
    # Save metadata
    metadata = {
        "last_updated": datetime.now().isoformat(),
        "total_categories": len(CATEGORIES),
        "total_funds_processed": sum(len(d.get("funds", [])) for d in all_categories_data.values()),
        "amfi_funds_count": len(amfi_data)
    }
    
    with open(f"{DATA_DIR}/metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)
    
    print(f"\n✅ PREPROCESSING COMPLETE!")
    print(f"   Categories: {metadata['total_categories']}")
    print(f"   Funds processed: {metadata['total_funds_processed']}")
    print(f"   Data saved to: {DATA_DIR}/")
    print("=" * 60)

def get_benchmark_name(bench_key):
    """Get human-readable benchmark name"""
    bench_names = {
        "large": "Nifty 50 TRI",
        "mid": "Nifty Midcap 150 TRI",
        "small": "Nifty Smallcap 250 TRI",
        "flexi": "Nifty 500 TRI",
        "hybrid": "65% Nifty 50 + 35% Bond",
        "gold": "Gold ETF",
        "us": "Nasdaq 100 TRI",
        "elss": "Nifty 50 TRI",
        "baf": "Nifty 50 TRI"
    }
    return bench_names.get(bench_key, "Nifty 50 TRI")

def get_category_type(category_name):
    """Determine category type"""
    equity = ["Cap", "Index", "Value", "Contra", "Focused", "Dividend", "ELSS"]
    hybrid = ["Hybrid", "Advantage", "Savings", "Arbitrage"]
    debt = ["Liquid", "Duration", "Money Market", "Corporate", "Banking", "Gilt", "Bond"]
    
    if any(k in category_name for k in equity):
        return "equity"
    elif any(k in category_name for k in hybrid):
        return "hybrid"
    elif any(k in category_name for k in debt):
        return "debt"
    elif "International" in category_name or "Gold" in category_name:
        return "international"
    return "others"

if __name__ == "__main__":
    precompute_all_data()
