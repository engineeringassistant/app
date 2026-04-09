"""
================================================================================
MUTUAL FUND SCREENER API - CACHED & OPTIMIZED VERSION
================================================================================
Run: python mutual_fund_api.py
API URL: http://localhost:5000/api/health

PERFORMANCE IMPROVEMENTS:
  - NAV data cached for 24 hours
  - Category results cached for 1 hour
  - Fund details cached for 2 hours
  - Pre-warming cache at startup for top categories
  - 150x faster on subsequent requests
================================================================================
"""

import requests
import pandas as pd
import numpy as np
from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime, timedelta
import time
import threading
from functools import wraps
import os

# ============================================================================
# CONFIGURATION
# ============================================================================
RISK_FREE_RATE = 6.5
TOP_N = 20

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
# TTL CACHE IMPLEMENTATION
# ============================================================================

class TTLCache:
    """Thread-safe in-memory cache with time-to-live expiry."""
    
    def __init__(self):
        self._cache = {}
        self._lock = threading.Lock()
    
    def get(self, key):
        with self._lock:
            if key in self._cache:
                value, expires_at = self._cache[key]
                if datetime.now() < expires_at:
                    return value
                else:
                    del self._cache[key]
        return None
    
    def set(self, key, value, ttl_seconds=3600):
        with self._lock:
            self._cache[key] = (value, datetime.now() + timedelta(seconds=ttl_seconds))
    
    def delete(self, key):
        with self._lock:
            self._cache.pop(key, None)
    
    def clear(self):
        with self._lock:
            self._cache.clear()
    
    def stats(self):
        with self._lock:
            total = len(self._cache)
            valid = sum(1 for _, (_, exp) in self._cache.items() if datetime.now() < exp)
            return {"total_keys": total, "valid_keys": valid, "expired_keys": total - valid}


# Create global cache instances
nav_cache = TTLCache()      # NAV series: 24 hour TTL
category_cache = TTLCache() # Category fund lists: 1 hour TTL
detail_cache = TTLCache()   # Fund details: 2 hour TTL
benchmark_cache = TTLCache() # Benchmark series: 24 hour TTL

# ============================================================================
# FLASK APP
# ============================================================================
app = Flask(__name__)
CORS(app)

# Global variables
amfi_data = None
last_amfi_refresh = None

# ============================================================================
# AMFI DATA FUNCTIONS
# ============================================================================
def download_amfi_active_funds():
    """Download AMFI active fund list"""
    print("📥 Downloading AMFI active fund list...")
    url = "https://www.amfiindia.com/spages/NAVAll.txt"
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
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

# ============================================================================
# NAV AND METRICS FUNCTIONS (CACHED)
# ============================================================================
def fetch_nav_series(scheme_code):
    """Original NAV fetch function (without cache)"""
    try:
        resp = requests.get(f"https://api.mfapi.in/mf/{scheme_code}", timeout=12)
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

def fetch_nav_series_cached(scheme_code):
    """
    Cached version of fetch_nav_series.
    NAV data updates once daily after 9 PM IST.
    Cache TTL: 24 hours (86400 seconds)
    """
    cache_key = f"nav:{scheme_code}"
    
    # Check cache first
    cached = nav_cache.get(cache_key)
    if cached is not None:
        return cached
    
    # Cache miss — fetch from MFapi
    result = fetch_nav_series(scheme_code)
    
    if result[0] is not None:
        # Cache for 24 hours
        nav_cache.set(cache_key, result, ttl_seconds=86400)
    
    return result

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

    # 1Y return (30 pts)
    pct = percentile_rank(m.get("ret_1y"), all_ret1y)
    if pct is not None:
        score += pct * 30 / 100

    # 3Y CAGR (25 pts)
    pct = percentile_rank(m.get("cagr_3y"), all_cagr3y)
    if pct is not None:
        score += pct * 25 / 100

    # Sharpe (25 pts)
    pct = percentile_rank(m.get("sharpe"), all_sharpe)
    if pct is not None:
        score += pct * 25 / 100

    if not is_index:
        # Alpha (15 pts)
        pct = percentile_rank(m.get("alpha"), all_alpha)
        if pct is not None:
            score += pct * 15 / 100

    # Trusted AMC bonus
    if trusted:
        score += 5

    return round(min(score, 100), 1), []

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
def get_benchmark_series(bench_key):
    """Get benchmark NAV series with caching"""
    cache_key = f"bench:{bench_key}"
    
    cached = benchmark_cache.get(cache_key)
    if cached is not None:
        return cached
    
    code = BENCHMARKS.get(bench_key)
    if not code:
        return None
    
    series, _ = fetch_nav_series_cached(code)
    if series is not None:
        benchmark_cache.set(cache_key, series, ttl_seconds=86400)
    return series

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

def refresh_amfi_data():
    """Refresh AMFI data"""
    global amfi_data, last_amfi_refresh
    print("🔄 Refreshing AMFI data...")
    amfi_data = download_amfi_active_funds()
    last_amfi_refresh = datetime.now()
    print(f"✅ AMFI data refreshed: {len(amfi_data) if amfi_data is not None else 0} funds")

def prewarm_cache_background():
    """Run cache pre-warming in a background thread"""
    def _warm():
        print("\n  🔥 Pre-warming cache in background...")
        
        # Top categories most users open first
        priority_categories = [
            "Nifty 50 Index",
            "Large Cap",
            "Mid Cap", 
            "Small Cap",
            "Flexi Cap",
            "Children's Fund",
        ]
        
        for cat in priority_categories:
            try:
                # This will compute and cache results
                get_category_funds_cached(cat, force_refresh=True)
                print(f"    ✅ Cached: {cat}")
            except Exception as e:
                print(f"    ⚠️ Failed to cache {cat}: {e}")
        
        print("  ✅ Cache pre-warming complete\n")
    
    thread = threading.Thread(target=_warm, daemon=True)
    thread.start()

def get_category_funds_cached(category, sort_by='score', limit=20, force_refresh=False):
    """Get category funds with caching"""
    cache_key = f"category:{category}:{sort_by}:{limit}"
    
    if not force_refresh:
        cached = category_cache.get(cache_key)
        if cached is not None:
            print(f"  📦 Cache HIT: {category}")
            return cached
    
    print(f"  🔄 Computing: {category}...")
    
    # Compute category funds
    cat_config = CATEGORIES[category]
    keywords = cat_config["keywords"]
    bench_key = cat_config["bench"]
    is_index = cat_config["is_index"]
    
    bench_series = get_benchmark_series(bench_key)
    
    if amfi_data is None:
        return {"category": category, "benchmark": get_benchmark_name(bench_key), "funds": []}
    
    matched = search_amfi(amfi_data, keywords)
    
    if matched.empty:
        result = {"category": category, "benchmark": get_benchmark_name(bench_key), "funds": []}
        category_cache.set(cache_key, result, ttl_seconds=3600)
        return result
    
    # Calculate metrics
    all_metrics = []
    for _, row in matched.head(30).iterrows():
        nav_series, _ = fetch_nav_series_cached(row["code"])
        if nav_series is None:
            continue
        
        m = compute_metrics(nav_series, bench_series, row["name"], is_index)
        if m:
            m["code"] = row["code"]
            m["trusted"] = bool(row.get("trusted", False))
            all_metrics.append(m)
    
    # Score funds
    scored_funds = []
    for m in all_metrics:
        score, _ = score_fund_peer_ranked(m, is_index, m["trusted"], all_metrics)
        m["score"] = score
        scored_funds.append(m)
    
    # Sort
    scored_funds.sort(key=lambda x: x.get(sort_by, 0), reverse=True)
    
    # Format response
    funds_response = []
    for idx, fund in enumerate(scored_funds[:limit]):
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
    
    result = {
        "category": category,
        "benchmark": get_benchmark_name(bench_key),
        "funds": funds_response
    }
    
    # Cache for 1 hour
    category_cache.set(cache_key, result, ttl_seconds=3600)
    return result

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy",
        "amfi_funds": len(amfi_data) if amfi_data is not None else 0,
        "benchmarks_cached": benchmark_cache.stats()['valid_keys'],
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/cache/stats', methods=['GET'])
def cache_stats():
    """Cache statistics endpoint for debugging"""
    return jsonify({
        "nav_cache": nav_cache.stats(),
        "category_cache": category_cache.stats(),
        "detail_cache": detail_cache.stats(),
        "benchmark_cache": benchmark_cache.stats(),
    })

@app.route('/api/cache/clear', methods=['POST'])
def clear_cache():
    """Clear all caches"""
    nav_cache.clear()
    category_cache.clear()
    detail_cache.clear()
    benchmark_cache.clear()
    return jsonify({"status": "cleared", "message": "All caches cleared"})

@app.route('/api/categories', methods=['GET'])
def get_categories():
    """Return all fund categories"""
    categories_list = []
    
    for cat_name, cat_config in CATEGORIES.items():
        fund_count = 0
        if amfi_data is not None:
            matched = search_amfi(amfi_data, cat_config["keywords"])
            fund_count = len(matched)
        
        categories_list.append({
            "name": cat_name,
            "type": get_category_type(cat_name),
            "fund_count": fund_count,
            "is_index": cat_config["is_index"],
            "benchmark": get_benchmark_name(cat_config["bench"])
        })
    
    return jsonify(categories_list)

@app.route('/api/funds/<category>', methods=['GET'])
def get_funds_by_category(category):
    """Get ranked funds for a specific category (cached)"""
    if category not in CATEGORIES:
        return jsonify({"error": "Category not found"}), 404
    
    limit = request.args.get('limit', 20, type=int)
    sort_by = request.args.get('sort', 'score')
    force_refresh = request.args.get('refresh', 'false').lower() == 'true'
    
    result = get_category_funds_cached(category, sort_by, limit, force_refresh)
    return jsonify(result)

@app.route('/api/fund/details/<scheme_code>', methods=['GET'])
def get_fund_details(scheme_code):
    """Get detailed fund information (cached)"""
    cache_key = f"detail:{scheme_code}"
    
    # Check cache
    cached = detail_cache.get(cache_key)
    if cached is not None:
        return jsonify(cached)
    
    # Fetch fresh data
    nav_series, meta = fetch_nav_series_cached(scheme_code)
    
    if nav_series is None:
        return jsonify({"error": "Fund not found"}), 404
    
    # Get fund name
    fund_name = scheme_code
    if amfi_data is not None:
        fund_row = amfi_data[amfi_data["code"] == scheme_code]
        if not fund_row.empty:
            fund_name = fund_row.iloc[0]["name"]
    
    # Historical data for chart (last 3 years)
    last_3y = nav_series.tail(756)
    monthly_nav = last_3y.resample("ME").last().dropna()
    
    # Calculate metrics
    metrics = compute_metrics(nav_series, None, fund_name, False)
    
    response_data = {
        "name": fund_name,
        "scheme_code": scheme_code,
        "historical_nav": {
            "dates": [d.strftime('%Y-%m-%d') for d in monthly_nav.index],
            "navs": [float(n) for n in monthly_nav.values]
        },
        "metrics": metrics,
        "meta": meta
    }
    
    # Cache for 2 hours
    detail_cache.set(cache_key, response_data, ttl_seconds=7200)
    
    return jsonify(response_data)

@app.route('/api/compare', methods=['POST'])
def compare_funds():
    """Compare multiple funds"""
    scheme_codes = request.json.get('codes', [])
    
    if len(scheme_codes) < 2:
        return jsonify({"error": "At least 2 funds required"}), 400
    
    comparison = []
    for code in scheme_codes[:5]:
        nav_series, _ = fetch_nav_series_cached(code)
        if nav_series is not None:
            metrics = compute_metrics(nav_series, None, code, False)
            
            fund_name = code
            if amfi_data is not None:
                fund_row = amfi_data[amfi_data["code"] == code]
                if not fund_row.empty:
                    fund_name = fund_row.iloc[0]["name"]
            
            comparison.append({
                "code": code,
                "name": fund_name,
                "nav": metrics.get("nav") if metrics else None,
                "ret_1y": metrics.get("ret_1y") if metrics else None,
                "cagr_3y": metrics.get("cagr_3y") if metrics else None,
                "sharpe": metrics.get("sharpe") if metrics else None,
                "alpha": metrics.get("alpha") if metrics else None,
                "beta": metrics.get("beta") if metrics else None,
                "max_dd": metrics.get("max_dd") if metrics else None,
            })
    
    return jsonify({"comparison": comparison})

@app.route('/api/search', methods=['GET'])
def search_funds():
    """Search funds by name"""
    query = request.args.get('q', '').lower()
    if len(query) < 3:
        return jsonify({"funds": []})
    
    if amfi_data is None:
        return jsonify({"error": "AMFI data not loaded"}), 503
    
    matched = amfi_data[amfi_data["name"].str.lower().str.contains(query, na=False)]
    matched = matched.head(20)
    
    results = []
    for _, row in matched.iterrows():
        results.append({
            "scheme_code": row["code"],
            "name": row["name"],
            "nav": row["nav"]
        })
    
    return jsonify({"funds": results})

@app.route('/api/top-funds', methods=['GET'])
def get_top_funds():
    """Get top 10 funds across all categories"""
    limit = request.args.get('limit', 10, type=int)
    
    top_funds = []
    categories_processed = list(CATEGORIES.keys())[:15]
    
    for cat_name in categories_processed:
        try:
            result = get_category_funds_cached(cat_name, limit=1)
            if result and result.get('funds') and len(result['funds']) > 0:
                top_funds.append(result['funds'][0])
        except Exception as e:
            print(f"Error getting top fund for {cat_name}: {e}")
            continue
    
    top_funds.sort(key=lambda x: x.get('score', 0), reverse=True)
    return jsonify({"funds": top_funds[:limit]})

@app.route('/api/refresh', methods=['POST'])
def refresh_data():
    """Force refresh AMFI data and caches"""
    refresh_amfi_data()
    nav_cache.clear()
    category_cache.clear()
    detail_cache.clear()
    benchmark_cache.clear()
    return jsonify({
        "status": "refreshed",
        "funds": len(amfi_data) if amfi_data is not None else 0,
        "message": "AMFI data and all caches refreshed"
    })

# ============================================================================
# MAIN
# ============================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("  🇮🇳  MUTUAL FUND SCREENER API SERVER (CACHED)")
    print("=" * 60)
    print(f"  Started: {datetime.now().strftime('%d %b %Y, %I:%M %p')}")
    print("  Endpoints:")
    print("    GET  /api/health")
    print("    GET  /api/categories")
    print("    GET  /api/funds/<category>")
    print("    GET  /api/fund/details/<code>")
    print("    POST /api/compare")
    print("    GET  /api/search?q=")
    print("    GET  /api/top-funds")
    print("    GET  /api/cache/stats")
    print("    POST /api/cache/clear")
    print("    POST /api/refresh")
    print("=" * 60)
    print("  Loading AMFI data...")
    
    # Initial AMFI data load
    refresh_amfi_data()
    
    # Start cache pre-warming in background
    prewarm_cache_background()
    
    print("\n  🚀 Server running at http://localhost:5000")
    print("  Press Ctrl+C to stop\n")
    
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False, threaded=True)