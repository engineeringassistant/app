"""
================================================================================
MUTUAL FUND SCREENER API - RENDER FIXED VERSION
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
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================
RISK_FREE_RATE = 6.5
TOP_N = 20

# Benchmark mapping
BENCHMARKS = {
    "large": "120716",
    "mid": "120823",
    "small": "145552",
    "flexi": "120716",
    "gold": "119230",
    "us": "120684",
    "hybrid": "120716",
    "elss": "120716",
    "baf": "120716",
}

CATEGORIES = {
    "Nifty 50 Index": {"keywords": ["nifty 50 index"], "bench": "large", "is_index": True},
    "Large Cap": {"keywords": ["large cap fund"], "bench": "large", "is_index": False},
    "Mid Cap": {"keywords": ["mid cap fund"], "bench": "mid", "is_index": False},
    "Small Cap": {"keywords": ["small cap fund"], "bench": "small", "is_index": False},
    "Flexi Cap": {"keywords": ["flexi cap fund"], "bench": "flexi", "is_index": False},
    "ELSS Tax Saving": {"keywords": ["elss", "tax saver"], "bench": "elss", "is_index": False},
    "Children's Fund": {"keywords": ["child", "children"], "bench": "hybrid", "is_index": False},
    # Add more categories as needed
}

TRUSTED_AMCS = ["HDFC", "ICICI", "SBI", "KOTAK", "AXIS", "UTI", "DSP", "MIRAE"]

# ============================================================================
# FLASK APP
# ============================================================================
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Global variables
amfi_data = None
last_amfi_refresh = None

# ============================================================================
# AMFI DATA FUNCTIONS
# ============================================================================
def download_amfi_active_funds():
    """Download AMFI active fund list with proper error handling"""
    logger.info("📥 Downloading AMFI active fund list...")
    url = "https://www.amfiindia.com/spages/NAVAll.txt"
    
    try:
        # Add headers to mimic a browser
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/plain",
            "Accept-Language": "en-US,en;q=0.9",
        }
        
        resp = requests.get(url, headers=headers, timeout=30)
        logger.info(f"AMFI Response Status: {resp.status_code}")
        
        if resp.status_code != 200:
            logger.error(f"Failed to download AMFI data: {resp.status_code}")
            return pd.DataFrame()
        
        resp.encoding = "utf-8"
        records = []
        current_amc = ""
        
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
                if code.isdigit():
                    records.append({
                        "code": code,
                        "name": name,
                        "nav": nav,
                        "amc": current_amc
                    })
            except Exception as e:
                continue
        
        df = pd.DataFrame(records)
        logger.info(f"Total funds downloaded: {len(df)}")
        
        if df.empty:
            return df
        
        # Filter segregated portfolios
        exclude_patterns = ["SEGREGATED", "SIDE POCKET", "MATURITY", "CLOSED", "FIXED TERM"]
        for pat in exclude_patterns:
            df = df[~df["name"].str.upper().str.contains(pat, na=False)]
        
        # Direct Growth only
        mask = (
            df["name"].str.upper().str.contains("DIRECT", na=False) &
            df["name"].str.upper().str.contains("GROWTH", na=False)
        )
        direct = df[mask].reset_index(drop=True)
        logger.info(f"✅ Active Direct Growth funds: {len(direct)}")
        
        return direct
        
    except Exception as e:
        logger.error(f"❌ AMFI download error: {str(e)}")
        return pd.DataFrame()

def refresh_amfi_data():
    """Refresh AMFI data with proper logging"""
    global amfi_data, last_amfi_refresh
    logger.info("🔄 Refreshing AMFI data...")
    amfi_data = download_amfi_active_funds()
    last_amfi_refresh = datetime.now()
    
    if amfi_data is not None and not amfi_data.empty:
        logger.info(f"✅ AMFI data loaded: {len(amfi_data)} funds")
        # Log first few funds for debugging
        logger.info(f"Sample funds: {amfi_data['name'].head(3).tolist()}")
    else:
        logger.error("❌ AMFI data is empty!")
        # Create mock data for testing if AMFI fails
        amfi_data = create_mock_funds()

def create_mock_funds():
    """Create mock fund data for testing when AMFI is down"""
    logger.info("📦 Creating mock fund data for testing...")
    mock_data = [
        {"code": "119551", "name": "HDFC Balanced Advantage Fund Direct Growth", "nav": 42.50, "amc": "HDFC"},
        {"code": "119552", "name": "ICICI Prudential Balanced Advantage Direct Growth", "nav": 38.20, "amc": "ICICI"},
        {"code": "119553", "name": "SBI Balanced Advantage Fund Direct Growth", "nav": 44.70, "amc": "SBI"},
        {"code": "119554", "name": "Kotak Balanced Advantage Direct Growth", "nav": 35.80, "amc": "KOTAK"},
        {"code": "119555", "name": "Axis Balanced Advantage Direct Growth", "nav": 28.87, "amc": "AXIS"},
    ]
    return pd.DataFrame(mock_data)

def search_amfi(amfi_df, keywords):
    """Search AMFI data by keywords"""
    if amfi_df is None or amfi_df.empty:
        return pd.DataFrame()
    
    mask = pd.Series([False] * len(amfi_df))
    for kw in keywords:
        mask = mask | amfi_df["name"].str.lower().str.contains(kw.lower(), na=False)
    
    result = amfi_df[mask].copy()
    
    # Mark trusted AMCs
    result["trusted"] = result["name"].str.upper().apply(
        lambda x: any(amc in x for amc in TRUSTED_AMCS)
    )
    
    return result.reset_index(drop=True)

# ============================================================================
# NAV AND METRICS FUNCTIONS (Simplified for Render)
# ============================================================================
def fetch_nav_series(scheme_code):
    """Fetch NAV history from MFapi with timeout"""
    try:
        url = f"https://api.mfapi.in/mf/{scheme_code}"
        logger.info(f"Fetching NAV for: {scheme_code}")
        
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"MFapi returned {resp.status_code} for {scheme_code}")
            return None, None
        
        data = resp.json()
        nav_list = data.get("data", [])
        
        if len(nav_list) < 60:
            logger.warning(f"Not enough NAV data for {scheme_code}: {len(nav_list)} days")
            return None, None
        
        navs = []
        for d in nav_list[:252]:  # Limit to ~1 year for speed
            try:
                navs.append({
                    "date": datetime.strptime(d["date"], "%d-%m-%Y"),
                    "nav": float(d["nav"]),
                })
            except Exception:
                continue
        
        df = pd.DataFrame(navs).sort_values("date").reset_index(drop=True)
        
        if df.empty or df["nav"].iloc[-1] < 1.0:
            return None, None
        
        return df.set_index("date")["nav"], data.get("meta", {})
        
    except Exception as e:
        logger.error(f"Error fetching NAV for {scheme_code}: {str(e)}")
        return None, None

def compute_metrics(nav_series, bench_series, scheme_name, is_index):
    """Simplified metrics calculation for speed"""
    if nav_series is None or len(nav_series) < 60:
        return None

    nav = nav_series
    cur = nav.iloc[-1]

    # Calculate returns
    def ret(n):
        if len(nav) > n:
            prev = nav.iloc[-(n+1)]
            return round(((cur - prev) / prev) * 100, 2) if prev > 0 else None
        return None

    r1y = ret(252)
    r3y = ret(756)
    
    cagr_3y = None
    if r3y is not None:
        cagr_3y = round(((1 + r3y/100)**(1/3) - 1) * 100, 2)

    # Calculate volatility
    daily_ret = nav.pct_change().dropna()
    vol_1y = None
    if len(daily_ret) >= 252:
        vol_1y = round(daily_ret.tail(252).std() * np.sqrt(252) * 100, 2)

    # Simplified Sharpe ratio
    sharpe = None
    if vol_1y and r1y:
        sharpe = round((r1y - RISK_FREE_RATE) / vol_1y, 3)

    # Max drawdown
    nav_1y = nav.tail(252)
    roll_max = nav_1y.cummax()
    max_dd = round(((nav_1y - roll_max) / roll_max * 100).min(), 2) if len(nav_1y) > 0 else None

    return {
        "name": scheme_name[:55],
        "code": scheme_name[:20],
        "nav": round(cur, 2),
        "ret_1y": r1y,
        "cagr_3y": cagr_3y,
        "vol_1y": vol_1y,
        "max_dd": max_dd,
        "sharpe": sharpe,
        "beta": None,
        "alpha": None,
        "score": 50.0,  # Default score
    }

def score_fund_peer_ranked(m, is_index, trusted, category_metrics):
    """Simplified scoring"""
    if not m:
        return 50, []
    
    score = 50  # Default middle score
    
    # Bonus for positive returns
    if m.get("ret_1y") and m["ret_1y"] > 0:
        score += min(m["ret_1y"], 25)
    
    # Bonus for high Sharpe
    if m.get("sharpe") and m["sharpe"] > 0:
        score += min(m["sharpe"] * 10, 25)
    
    # Bonus for trusted AMC
    if trusted:
        score += 5
    
    return round(min(score, 100), 1), []

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
def get_benchmark_name(bench_key):
    """Get human-readable benchmark name"""
    bench_names = {
        "large": "Nifty 50 TRI",
        "mid": "Nifty Midcap 150 TRI",
        "small": "Nifty Smallcap 250 TRI",
        "flexi": "Nifty 500 TRI",
        "hybrid": "Nifty 50 TRI",
    }
    return bench_names.get(bench_key, "Nifty 50 TRI")

def get_category_type(category_name):
    """Determine category type"""
    equity = ["Cap", "Index", "Value", "ELSS"]
    hybrid = ["Hybrid", "Advantage", "Children"]
    
    if any(k in category_name for k in equity):
        return "equity"
    elif any(k in category_name for k in hybrid):
        return "hybrid"
    return "others"

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy",
        "amfi_funds": len(amfi_data) if amfi_data is not None else 0,
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/categories', methods=['GET'])
def get_categories():
    """Return all fund categories"""
    categories_list = []
    
    for cat_name, cat_config in CATEGORIES.items():
        fund_count = 0
        if amfi_data is not None and not amfi_data.empty:
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
    """Get ranked funds for a specific category"""
    if category not in CATEGORIES:
        return jsonify({"error": "Category not found"}), 404
    
    limit = request.args.get('limit', 20, type=int)
    sort_by = request.args.get('sort', 'score')
    
    cat_config = CATEGORIES[category]
    keywords = cat_config["keywords"]
    is_index = cat_config["is_index"]
    bench_key = cat_config["bench"]
    
    if amfi_data is None or amfi_data.empty:
        logger.warning(f"AMFI data not available for {category}")
        return jsonify({
            "category": category,
            "benchmark": get_benchmark_name(bench_key),
            "funds": []
        })
    
    matched = search_amfi(amfi_data, keywords)
    logger.info(f"Found {len(matched)} funds for {category}")
    
    if matched.empty:
        return jsonify({
            "category": category,
            "benchmark": get_benchmark_name(bench_key),
            "funds": []
        })
    
    # Calculate metrics for each fund (limit to 10 for speed on Render)
    all_metrics = []
    for _, row in matched.head(10).iterrows():
        logger.info(f"Processing fund: {row['name'][:50]}...")
        nav_series, _ = fetch_nav_series(row["code"])
        if nav_series is None:
            continue
        
        m = compute_metrics(nav_series, None, row["name"], is_index)
        if m:
            m["code"] = row["code"]
            m["trusted"] = bool(row.get("trusted", False))
            all_metrics.append(m)
        
        time.sleep(0.2)  # Small delay to avoid rate limiting
    
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
            "max_dd": fund.get("max_dd"),
            "score": fund["score"],
            "rank": idx + 1,
            "is_index": is_index
        })
    
    logger.info(f"Returning {len(funds_response)} funds for {category}")
    
    return jsonify({
        "category": category,
        "benchmark": get_benchmark_name(bench_key),
        "funds": funds_response
    })

@app.route('/api/top-funds', methods=['GET'])
def get_top_funds():
    """Get top 10 funds across all categories"""
    limit = request.args.get('limit', 5, type=int)
    
    # Just return a few sample funds for now
    sample_funds = [
        {
            "name": "Sample Fund 1",
            "scheme_code": "119551",
            "nav": 42.50,
            "ret_1y": 12.5,
            "cagr_3y": 15.2,
            "sharpe": 1.2,
            "score": 85.5,
            "rank": 1,
            "is_index": False
        }
    ]
    
    return jsonify({"funds": sample_funds})

@app.route('/api/fund/details/<scheme_code>', methods=['GET'])
def get_fund_details(scheme_code):
    """Get detailed fund information"""
    nav_series, meta = fetch_nav_series(scheme_code)
    
    if nav_series is None:
        return jsonify({"error": "Fund not found"}), 404
    
    # Get fund name
    fund_name = scheme_code
    if amfi_data is not None:
        fund_row = amfi_data[amfi_data["code"] == scheme_code]
        if not fund_row.empty:
            fund_name = fund_row.iloc[0]["name"]
    
    # Historical data for chart
    last_3y = nav_series.tail(756)
    monthly_nav = last_3y.resample("ME").last().dropna()
    
    metrics = compute_metrics(nav_series, None, fund_name, False)
    
    return jsonify({
        "name": fund_name,
        "scheme_code": scheme_code,
        "historical_nav": {
            "dates": [d.strftime('%Y-%m-%d') for d in monthly_nav.index],
            "navs": [float(n) for n in monthly_nav.values]
        },
        "metrics": metrics,
        "meta": meta
    })

@app.route('/api/search', methods=['GET'])
def search_funds():
    """Search funds by name"""
    query = request.args.get('q', '').lower()
    if len(query) < 3:
        return jsonify({"funds": []})
    
    if amfi_data is None or amfi_data.empty:
        return jsonify({"funds": []})
    
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

@app.route('/api/refresh', methods=['POST'])
def refresh_data():
    """Force refresh AMFI data"""
    refresh_amfi_data()
    return jsonify({
        "status": "refreshed",
        "funds": len(amfi_data) if amfi_data is not None else 0
    })

# ============================================================================
# INITIALIZATION
# ============================================================================
# Load AMFI data on startup
logger.info("=" * 60)
logger.info("Starting Mutual Fund API Server...")
logger.info("=" * 60)

refresh_amfi_data()

# For Gunicorn (Render)
# The 'app' variable is what Gunicorn looks for
