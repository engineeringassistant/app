"""
================================================================================
LIGHTWEIGHT API - Serves precomputed data instantly
================================================================================
This API loads precomputed JSON files and serves them with NO heavy calculations.
Perfect for free tiers on Render/Oracle Cloud.
================================================================================
"""

import os
import json
import time
from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime
import threading

app = Flask(__name__)
CORS(app)

# Global cache for precomputed data
precomputed_data = {}
data_last_loaded = None
data_file_path = "precomputed_data"  # Path to precomputed data directory

def load_precomputed_data():
    """Load all precomputed data from JSON files"""
    global precomputed_data, data_last_loaded
    
    print("📂 Loading precomputed data...")
    
    try:
        # Load categories list
        with open(f"{data_file_path}/categories.json", "r") as f:
            precomputed_data["categories"] = json.load(f)
        
        # Load categories data (funds by category)
        with open(f"{data_file_path}/categories_data.json", "r") as f:
            precomputed_data["categories_data"] = json.load(f)
        
        # Load metadata
        with open(f"{data_file_path}/metadata.json", "r") as f:
            precomputed_data["metadata"] = json.load(f)
        
        data_last_loaded = datetime.now()
        print(f"✅ Loaded {len(precomputed_data['categories'])} categories")
        print(f"   Last updated: {precomputed_data['metadata']['last_updated']}")
        
    except FileNotFoundError as e:
        print(f"⚠️ Data files not found: {e}")
        print("   Run data_preprocessor.py first to generate data")
        precomputed_data = {"categories": [], "categories_data": {}, "metadata": {}}
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        precomputed_data = {"categories": [], "categories_data": {}, "metadata": {}}

# Load data on startup
load_precomputed_data()

# Optional: Auto-reload every hour (for long-running servers)
def auto_reload():
    """Auto-reload precomputed data periodically"""
    while True:
        time.sleep(3600)  # Reload every hour
        print("🔄 Auto-reloading precomputed data...")
        load_precomputed_data()

# Start auto-reload thread (optional)
reload_thread = threading.Thread(target=auto_reload, daemon=True)
reload_thread.start()

# ============================================================================
# API ENDPOINTS (All serve from memory - FAST!)
# ============================================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "data_loaded": len(precomputed_data.get("categories", [])) > 0,
        "last_updated": precomputed_data.get("metadata", {}).get("last_updated"),
        "server_time": datetime.now().isoformat()
    })

@app.route('/api/categories', methods=['GET'])
def get_categories():
    """Return all fund categories (from precomputed data)"""
    categories = precomputed_data.get("categories", [])
    return jsonify(categories)

@app.route('/api/funds/<category>', methods=['GET'])
def get_funds_by_category(category):
    """Get ranked funds for a specific category (from precomputed data)"""
    categories_data = precomputed_data.get("categories_data", {})
    
    if category not in categories_data:
        return jsonify({"error": "Category not found"}), 404
    
    category_data = categories_data[category]
    
    limit = request.args.get('limit', 20, type=int)
    sort_by = request.args.get('sort', 'score')
    
    funds = category_data.get("funds", [])
    
    # Sort if needed (precomputed data is already sorted by score)
    if sort_by != 'score':
        funds = sorted(funds, key=lambda x: x.get(sort_by, 0), reverse=True)
    
    return jsonify({
        "category": category,
        "benchmark": category_data.get("benchmark", "Nifty 50 TRI"),
        "funds": funds[:limit]
    })

@app.route('/api/fund/details/<scheme_code>', methods=['GET'])
def get_fund_details(scheme_code):
    """
    Get detailed fund information
    Note: This still requires fetching NAV data as it's dynamic
    But we can add caching here
    """
    # Try to get from cache first
    cache_key = f"fund_details_{scheme_code}"
    cached_data = fund_details_cache.get(cache_key)
    if cached_data:
        return jsonify(cached_data)
    
    # If not cached, fetch from MFapi (this is the only heavy call)
    import requests
    try:
        resp = requests.get(f"https://api.mfapi.in/mf/{scheme_code}", timeout=10)
        if resp.status_code != 200:
            return jsonify({"error": "Fund not found"}), 404
        
        data = resp.json()
        nav_list = data.get("data", [])
        
        if len(nav_list) < 60:
            return jsonify({"error": "Insufficient data"}), 404
        
        navs = []
        for d in nav_list[-100:]:  # Last 100 entries for chart
            try:
                navs.append({
                    "date": d["date"],
                    "nav": float(d["nav"])
                })
            except Exception:
                continue
        
        # Get fund name from precomputed data
        fund_name = scheme_code
        for cat_data in precomputed_data.get("categories_data", {}).values():
            for fund in cat_data.get("funds", []):
                if fund.get("scheme_code") == scheme_code:
                    fund_name = fund.get("name", scheme_code)
                    break
        
        response_data = {
            "name": fund_name,
            "scheme_code": scheme_code,
            "historical_nav": {
                "dates": [n["date"] for n in navs],
                "navs": [n["nav"] for n in navs]
            },
            "meta": data.get("meta", {})
        }
        
        # Cache for 1 hour
        fund_details_cache[cache_key] = response_data
        # Clean old cache
        if len(fund_details_cache) > 100:
            fund_details_cache.clear()
        
        return jsonify(response_data)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/search', methods=['GET'])
def search_funds():
    """Search funds by name (from precomputed data)"""
    query = request.args.get('q', '').lower()
    if len(query) < 3:
        return jsonify({"funds": []})
    
    results = []
    for cat_name, cat_data in precomputed_data.get("categories_data", {}).items():
        for fund in cat_data.get("funds", []):
            if query in fund.get("name", "").lower():
                results.append({
                    "scheme_code": fund.get("scheme_code"),
                    "name": fund.get("name"),
                    "nav": fund.get("nav")
                })
                if len(results) >= 20:
                    break
        if len(results) >= 20:
            break
    
    return jsonify({"funds": results})

@app.route('/api/top-funds', methods=['GET'])
def get_top_funds():
    """Get top funds across all categories (from precomputed data)"""
    limit = request.args.get('limit', 10, type=int)
    
    all_funds = []
    for cat_data in precomputed_data.get("categories_data", {}).values():
        all_funds.extend(cat_data.get("funds", []))
    
    all_funds.sort(key=lambda x: x.get("score", 0), reverse=True)
    
    return jsonify({"funds": all_funds[:limit]})

@app.route('/api/compare', methods=['POST'])
def compare_funds():
    """Compare multiple funds"""
    scheme_codes = request.json.get('codes', [])
    
    if len(scheme_codes) < 2:
        return jsonify({"error": "At least 2 funds required"}), 400
    
    # Find funds in precomputed data
    funds_data = {}
    for cat_data in precomputed_data.get("categories_data", {}).values():
        for fund in cat_data.get("funds", []):
            if fund.get("scheme_code") in scheme_codes:
                funds_data[fund.get("scheme_code")] = fund
    
    comparison = []
    for code in scheme_codes:
        fund = funds_data.get(code, {})
        comparison.append({
            "code": code,
            "name": fund.get("name", code),
            "nav": fund.get("nav"),
            "ret_1y": fund.get("ret_1y"),
            "cagr_3y": fund.get("cagr_3y"),
            "sharpe": fund.get("sharpe"),
            "alpha": fund.get("alpha"),
            "beta": fund.get("beta"),
            "max_dd": fund.get("max_dd"),
            "score": fund.get("score")
        })
    
    return jsonify({"comparison": comparison})

@app.route('/api/refresh', methods=['POST'])
def refresh_data():
    """Force reload precomputed data"""
    load_precomputed_data()
    global fund_details_cache
    fund_details_cache = {}
    return jsonify({
        "status": "refreshed",
        "last_updated": precomputed_data.get("metadata", {}).get("last_updated")
    })

# Simple cache for fund details
fund_details_cache = {}

# ============================================================================
# MAIN
# ============================================================================
if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    print("=" * 60)
    print("  🚀 LIGHTWEIGHT API SERVER")
    print("=" * 60)
    print(f"  Started: {datetime.now().strftime('%d %b %Y, %I:%M %p')}")
    print(f"  Data loaded: {len(precomputed_data.get('categories', []))} categories")
    print("  Endpoints:")
    print("    GET  /api/health")
    print("    GET  /api/categories")
    print("    GET  /api/funds/<category>")
    print("    GET  /api/fund/details/<code>")
    print("    POST /api/compare")
    print("    GET  /api/search?q=")
    print("    GET  /api/top-funds")
    print("=" * 60)
    print("\n  🚀 Server running at http://0.0.0.0:5000")
    print("  Press Ctrl+C to stop\n")
    
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
