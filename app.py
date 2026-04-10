"""
NiftyPaper API - Yahoo Finance Version (Fixed)
Works in Jupyter notebooks and standalone
"""

import os
import asyncio
import logging
import pandas as pd
import yfinance as yf
import requests
from io import StringIO  # Fixed import
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import threading
import uvicorn

# ─────────────────────────────────────────────
# Logging Setup
# ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("niftypaper_yf")

app = FastAPI(title="NiftyPaper Yahoo Finance API Proxy")

# ─────────────────────────────────────────────
# Instrument Cache
# ─────────────────────────────────────────────
instrument_df = None

# NSE Stock Symbol Mapping
NSE_SUFFIX = ".NS"
BSE_SUFFIX = ".BO"

def load_nse_instruments():
    """Load NSE instruments from CSV or API"""
    global instrument_df
    try:
        # Try to load from NSE CSV
        url = "https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            # Fixed: Use StringIO from io module
            df = pd.read_csv(StringIO(response.text))
            df['Symbol'] = df['Symbol'].astype(str)
            df['Name'] = df['Company Name'].astype(str) if 'Company Name' in df.columns else df['Symbol']
            df['Exchange'] = 'NSE'
            df['yf_symbol'] = df['Symbol'] + NSE_SUFFIX
            instrument_df = df
            logger.info(f"✅ Loaded {len(df)} Nifty 50 instruments")
            return
        
        # Fallback to common Nifty 50 list
        nifty_50 = [
            "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR", 
            "ITC", "BHARTIARTL", "KOTAKBANK", "SBIN", "BAJFINANCE", "LT", 
            "WIPRO", "AXISBANK", "TITAN", "ASIANPAINT", "MARUTI", "SUNPHARMA", 
            "HCLTECH", "ULTRACEMCO", "NTPC", "POWERGRID", "M&M", "TATAMOTORS",
            "TATASTEEL", "JSWSTEEL", "TECHM", "HDFCLIFE", "SBILIFE", "NESTLEIND"
        ]
        instrument_df = pd.DataFrame({
            'Symbol': nifty_50,
            'Name': nifty_50,
            'Exchange': 'NSE',
            'yf_symbol': [s + NSE_SUFFIX for s in nifty_50]
        })
        logger.info(f"✅ Loaded {len(nifty_50)} Nifty 50 instruments (fallback)")
            
    except Exception as e:
        logger.error(f"❌ Instrument load failed: {e}")
        # Create minimal fallback
        instrument_df = pd.DataFrame({
            'Symbol': ["RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK"],
            'Name': ["Reliance", "TCS", "HDFC Bank", "Infosys", "ICICI Bank"],
            'Exchange': ["NSE", "NSE", "NSE", "NSE", "NSE"],
            'yf_symbol': ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS"]
        })
        logger.info("✅ Using minimal instrument list")

# Load instruments on startup
load_nse_instruments()

# ─────────────────────────────────────────────
# Helper Functions
# ─────────────────────────────────────────────

def get_yahoo_symbol(symbol: str, exchange: str = "NSE") -> str:
    """Convert NSE/BSE symbol to Yahoo Finance format"""
    symbol = symbol.upper().strip()
    
    if exchange.upper() == "NSE":
        return symbol + NSE_SUFFIX
    elif exchange.upper() == "BSE":
        return symbol + BSE_SUFFIX
    else:
        if symbol.endswith(".NS") or symbol.endswith(".BO"):
            return symbol
        return symbol + NSE_SUFFIX

def get_nifty_index() -> Dict:
    """Get Nifty 50 index data"""
    try:
        ticker = yf.Ticker("^NSEI")
        data = ticker.history(period="1d")
        
        if data.empty:
            return None
        
        last = data.iloc[-1]
        open_price = data.iloc[0]['Open']
        
        return {
            "symbol": "NIFTY 50",
            "ltp": float(last['Close']),
            "change": float(last['Close'] - open_price),
            "percent_change": float(((last['Close'] - open_price) / open_price) * 100),
            "high": float(last['High']),
            "low": float(last['Low']),
            "volume": int(last['Volume']),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error fetching Nifty: {e}")
        return None

def get_banknifty_index() -> Dict:
    """Get Bank Nifty index data"""
    try:
        ticker = yf.Ticker("^NSEBANK")
        data = ticker.history(period="1d")
        
        if data.empty:
            return None
        
        last = data.iloc[-1]
        open_price = data.iloc[0]['Open']
        
        return {
            "symbol": "BANK NIFTY",
            "ltp": float(last['Close']),
            "change": float(last['Close'] - open_price),
            "percent_change": float(((last['Close'] - open_price) / open_price) * 100),
            "high": float(last['High']),
            "low": float(last['Low']),
            "volume": int(last['Volume']),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error fetching Bank Nifty: {e}")
        return None

# ─────────────────────────────────────────────
# API Endpoints
# ─────────────────────────────────────────────

@app.get("/")
def health_check():
    return {
        "status": "ok", 
        "message": "NiftyPaper Yahoo Finance API running",
        "endpoints": [
            "/quote/{symbol}",
            "/nifty",
            "/banknifty",
            "/search",
            "/historical/{symbol}",
            "/indicators/{symbol}",
            "/market-status"
        ]
    }

@app.get("/nifty")
def get_nifty():
    """Get Nifty 50 index data"""
    data = get_nifty_index()
    if data:
        return data
    raise HTTPException(status_code=404, detail="Nifty data not available")

@app.get("/banknifty")
def get_banknifty():
    """Get Bank Nifty index data"""
    data = get_banknifty_index()
    if data:
        return data
    raise HTTPException(status_code=404, detail="Bank Nifty data not available")

@app.get("/quote/{symbol}")
def get_quote(symbol: str, exchange: str = Query("NSE", description="NSE or BSE")):
    """Get live price for NSE/BSE stocks"""
    try:
        # Handle indices
        if symbol.upper() == "NIFTY" or symbol.upper() == "NIFTY50":
            return get_nifty_index()
        if symbol.upper() == "BANKNIFTY":
            return get_banknifty_index()
        
        yf_symbol = get_yahoo_symbol(symbol, exchange)
        logger.info(f"📊 Quote request: {symbol} -> {yf_symbol}")
        
        ticker = yf.Ticker(yf_symbol)
        data = ticker.history(period="1d")
        
        if data.empty:
            raise HTTPException(status_code=404, detail=f"No data found for {symbol}")
        
        last = data.iloc[-1]
        open_price = data.iloc[0]['Open']
        
        return {
            "symbol": symbol,
            "yf_symbol": yf_symbol,
            "exchange": exchange,
            "ltp": float(last['Close']),
            "open": float(open_price),
            "high": float(last['High']),
            "low": float(last['Low']),
            "volume": int(last['Volume']),
            "change": float(last['Close'] - open_price),
            "percent_change": float(((last['Close'] - open_price) / open_price) * 100),
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Quote error for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/indicators/{symbol}")
def get_indicators(
    symbol: str,
    exchange: str = Query("NSE"),
    ema_fast: int = Query(9),
    ema_slow: int = Query(54)
):
    """Calculate EMA and other indicators"""
    try:
        yf_symbol = get_yahoo_symbol(symbol, exchange)
        ticker = yf.Ticker(yf_symbol)
        
        data = ticker.history(period="60d")
        
        if data.empty:
            raise HTTPException(status_code=404, detail="No data found")
        
        # Calculate EMAs
        data['EMA9'] = data['Close'].ewm(span=ema_fast, adjust=False).mean()
        data['EMA54'] = data['Close'].ewm(span=ema_slow, adjust=False).mean()
        
        # Calculate RSI
        delta = data['Close'].diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = -delta.clip(upper=0).rolling(14).mean()
        rs = gain / loss
        data['RSI'] = 100 - (100 / (1 + rs))
        
        last = data.iloc[-1]
        prev = data.iloc[-2]
        
        ema_cross_above = (prev['EMA9'] <= prev['EMA54']) and (last['EMA9'] > last['EMA54'])
        ema_cross_below = (prev['EMA9'] >= prev['EMA54']) and (last['EMA9'] < last['EMA54'])
        
        signal = None
        if ema_cross_above:
            signal = "BUY"
        elif ema_cross_below:
            signal = "SELL"
        
        return {
            "symbol": symbol,
            "exchange": exchange,
            "current_price": float(last['Close']),
            "ema9": float(last['EMA9']),
            "ema54": float(last['EMA54']),
            "rsi": float(last['RSI']) if not pd.isna(last['RSI']) else None,
            "signal": signal,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Indicator error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/search")
def search_instruments(q: str = Query(..., min_length=2)):
    """Search for NSE/BSE stocks"""
    global instrument_df
    
    if instrument_df is None or instrument_df.empty:
        return [{"symbol": "RELIANCE", "name": "Reliance Industries", "exchange": "NSE"}]
    
    q_lower = q.lower()
    results = instrument_df[
        instrument_df['Symbol'].str.lower().str.contains(q_lower, na=False) |
        instrument_df['Name'].str.lower().str.contains(q_lower, na=False)
    ].head(20)
    
    return [
        {
            "symbol": row['Symbol'],
            "name": row['Name'],
            "exchange": row['Exchange']
        }
        for _, row in results.iterrows()
    ]

@app.get("/market-status")
def get_market_status():
    """Check if market is open"""
    now = datetime.now()
    
    is_weekday = now.weekday() < 5
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    is_trading_hours = market_open <= now <= market_close
    
    return {
        "is_open": is_weekday and is_trading_hours,
        "is_weekday": is_weekday,
        "is_trading_hours": is_trading_hours,
        "current_time": now.isoformat(),
        "market_open": market_open.isoformat(),
        "market_close": market_close.isoformat()
    }

# ─────────────────────────────────────────────
# Run Server (Fixed for Jupyter/Notebook)
# ─────────────────────────────────────────────

def run_server():
    """Run the server in a separate thread (for Jupyter)"""
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")

def start_server():
    """Start the server (works in any environment)"""
    print("\n" + "="*60)
    print("🚀 NiftyPaper Yahoo Finance API Server")
    print("="*60)
    print("\n📱 Access from mobile: http://YOUR_IP:8000")
    print("\nAvailable endpoints:")
    print("  GET  /                  - Health check")
    print("  GET  /nifty             - Nifty 50 index")
    print("  GET  /banknifty         - Bank Nifty index")
    print("  GET  /quote/{symbol}    - Get stock price")
    print("  GET  /indicators/{symbol} - EMA indicators")
    print("  GET  /search?q=reliance - Search stocks")
    print("  GET  /market-status     - Market status")
    print("="*60 + "\n")
    
    # Run directly (works in terminal)
    uvicorn.run(app, host="0.0.0.0", port=8000)

# For Jupyter notebooks - run in background thread
def start_server_background():
    """Start server in background thread (for Jupyter)"""
    import threading
    
    def run():
        uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
    
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    print("✅ Server started in background on port 8000")
    return thread

# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    # Check if running in Jupyter
    try:
        from IPython import get_ipython
        if get_ipython() is not None:
            # Running in Jupyter - start in background
            start_server_background()
        else:
            # Running in terminal - start normally
            start_server()
    except ImportError:
        # Not in Jupyter
        start_server()
