"""
================================================================================
LIGHTWEIGHT API - No pandas version (pure Python)
================================================================================
"""

import os
import json
import time
from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime
import requests

app = Flask(__name__)
CORS(app)

# Global cache
precomputed_data = {}
data_file_path = "precomputed_data"

def load_precomputed_data():
    """Load all precomputed data from JSON files (no pandas needed)"""
    global precomputed_data
    
    print("📂 Loading precomputed data...")
    
    try:
        with open(f"{data_file_path}/categories.json", "r") as f:
            precomputed_data["categories"] = json.load(f)
        
        with open(f"{data_file_path}/categories_data.json", "r") as f:
            precomputed_data["categories_data"] = json.load(f)
        
        with open(f"{data_file_path}/metadata.json", "r") as f:
            precomputed_data["metadata"] = json.load(f)
        
        print(f"✅ Loaded {len(precomputed_data['categories'])} categories")
        
    except FileNotFoundError as e:
        print(f"⚠️ Data files not found: {e}")
        precomputed_data = {"categories": [], "categories_data": {}, "metadata": {}}
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        precomputed_data = {"categories": [], "categories_data": {}, "metadata": {}}

# Load data on startup
load_precomputed_data()

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy",
        "data_loaded": len(precomputed_data.get("categories", [])) > 0,
        "last_updated": precomputed_data.get("metadata", {}).get("last_updated"),
        "server_time": datetime.now().isoformat()
    })

@app.route('/api/categories', methods=['GET'])
def get_categories():
    return jsonify(precomputed_data.get("categories", []))

@app.route('/api/funds/<category>', methods=['GET'])
def get_funds_by_category(category):
    categories_data = precomputed_data.get("categories_data", {})
    
    if category not in categories_data:
        return jsonify({"error": "Category not found"}), 404
    
    category_data = categories_data[category]
    limit = request.args.get('limit', 20, type=int)
    funds = category_data.get("funds", [])[:limit]
    
    return jsonify({
        "category": category,
        "benchmark": category_data.get("benchmark", "Nifty 50 TRI"),
        "funds": funds
    })

@app.route('/api/fund/details/<scheme_code>', methods=['GET'])
def get_fund_details(scheme_code):
    """Fetch NAV details from MFapi (requires requests only)"""
    try:
        resp = requests.get(f"https://api.mfapi.in/mf/{scheme_code}", timeout=10)
        if resp.status_code != 200:
            return jsonify({"error": "Fund not found"}), 404
        
        data = resp.json()
        nav_list = data.get("data", [])
        
        if len(nav_list) < 60:
            return jsonify({"error": "Insufficient data"}), 404
        
        navs = []
        for d in nav_list[-100:]:
            try:
                navs.append({
                    "date": d["date"],
                    "nav": float(d["nav"])
                })
            except Exception:
                continue
        
        return jsonify({
            "name": scheme_code,
            "scheme_code": scheme_code,
            "historical_nav": {
                "dates": [n["date"] for n in navs],
                "navs": [n["nav"] for n in navs]
            },
            "meta": data.get("meta", {})
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/search', methods=['GET'])
def search_funds():
    query = request.args.get('q', '').lower()
    if len(query) < 3:
        return jsonify({"funds": []})
    
    results = []
    for cat_data in precomputed_data.get("categories_data", {}).values():
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
    limit = request.args.get('limit', 10, type=int)
    
    all_funds = []
    for cat_data in precomputed_data.get("categories_data", {}).values():
        all_funds.extend(cat_data.get("funds", []))
    
    all_funds.sort(key=lambda x: x.get("score", 0), reverse=True)
    
    return jsonify({"funds": all_funds[:limit]})

@app.route('/api/compare', methods=['POST'])
def compare_funds():
    scheme_codes = request.json.get('codes', [])
    
    if len(scheme_codes) < 2:
        return jsonify({"error": "At least 2 funds required"}), 400
    
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
            "max_dd": fund.get("max_dd"),
            "score": fund.get("score")
        })
    
    return jsonify({"comparison": comparison})

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
