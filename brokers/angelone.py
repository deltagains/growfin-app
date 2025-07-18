import sqlite3
import pandas as pd
import pyotp
from flask import jsonify
from SmartApi.smartConnect import SmartConnect  # adjust if SmartConnect path differs
from sqlalchemy import create_engine, MetaData, Table, insert, text
import requests
import os
from config import DB_PATH
from helpers import DaysToExpiry, calculate_greeks, sanitize
from decimal import Decimal


def get_user_credentials():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT username, password, api_key, totp_token FROM user LIMIT 1")
    result = cursor.fetchone()
    conn.close()
    if not result:
        raise Exception("No user credentials found in user table.")
    return result

def get_connection():
    return create_engine("sqlite:///{DB_PATH}")

def login():
    username, password, api_key, token = get_user_credentials()
    otp = pyotp.TOTP(token).now()
    obj = SmartConnect(api_key=api_key)
    obj.generateSession(username, password, otp)
    return obj, username


def logout(obj, username):
    obj.terminateSession(username)

def stock_holdings():
    try:
        obj, username = login()
        ds = obj.holding()
        logout(obj, username)

        position_data = pd.DataFrame.from_dict(ds)
        stock_data = []

        for stock in position_data['data']:
            avg_price = float(stock['averageprice'])
            ltp = float(stock['ltp'])
            qty = int(stock['quantity'])
            unrealised = (ltp - avg_price) * qty

            # Correcting data format, tuples for executemany()
            stock_data.append((
                stock['tradingsymbol'][:-3],  # symbolname
                ltp,                          # underlying_ltp
                qty,                          # netqty
                avg_price,                    # buyprice
                unrealised                    # unrealised
            ))

        # Connect using sqlite3 and insert data
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Clear existing records
        cursor.execute("DELETE FROM stockpositions")

        # Insert new records
        cursor.executemany("""
            INSERT INTO stockpositions (symbolname, underlying_ltp, netqty, buyprice, unrealised)
            VALUES (?, ?, ?, ?, ?)
        """, stock_data)

        conn.commit()
        conn.close()

        return jsonify({"message": f"{len(stock_data)} rows inserted into stockpositions."})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

def insert_positions():
    try:
        # Step 1: Login and fetch data
        obj, username = login()
        ds = obj.position()
        logout(obj, username)

        position_data = pd.DataFrame.from_dict(ds)
        if position_data.empty:
            return jsonify({"message": "No position data found."}), 200

        # Step 2: Prepare LTP data before DB connection
        symbols = list(set(pos['symbolname'] for pos in position_data['data']))
        ltp_cache = {}
        for symbol in symbols:
            try:
                payload = {'symbol_name': symbol}
                response = requests.post("http://thetagains.pythonanywhere.com/check_underlying_token", json=payload)
                underlying_token = response.json()
                ltp_data = obj.ltpData('NSE', symbol, underlying_token)
                ltp_cache[symbol] = float(ltp_data['data'].get('ltp', 0))
            except:
                ltp_cache[symbol] = 0.0

        # Step 3: Prepare data for DB insert
        insert_values = []
        for pos in position_data['data']:
            symbol = pos.get('symbolname', '')
            ltp_underlying = ltp_cache.get(symbol, 0.0)
            strike = pos.get('strikeprice', 0)
            opt_type = pos.get('optiontype', '')
            ltp_option = float(pos.get('ltp', 0))
            lotsize = int(pos.get('lotsize', 1))
            netqty = int(pos.get('netqty', 0))
            totallots = abs(netqty / lotsize) if lotsize else 0
            days_to_expiry = DaysToExpiry(pos.get('expirydate')) if len(pos.get('expirydate')) > 0 else 0
            days_to_expiry = max(float(days_to_expiry), 0.5)
            delta, theta, implied_vol = calculate_greeks(ltp_underlying, strike, days_to_expiry, ltp_option, opt_type)

            insert_values.append(tuple(sanitize(v) for v in (
                ltp_underlying,
                strike,
                opt_type,
                pos.get('totalsellavgprice', 0),
                pos.get('totalbuyavgprice', 0),
                ltp_option,
                netqty,
                totallots,
                lotsize,
                pos.get('unrealised', 0),
                pos.get('realised', 0),
                symbol,
                pos.get('expirydate', ''),
                delta,
                theta,
                implied_vol
            )))

        # Step 4: Connect to DB with WAL mode and insert
        conn = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()

        cursor.execute("DELETE FROM optionpositions")

        cursor.executemany("""
            INSERT INTO optionpositions (
                underlying_ltp, strikeprice, optiontype, totalsellavgprice,
                totalbuyavgprice, ltp, netqty, totallots, lotsize,
                unrealised, realised, symbolname, expirydate,
                delta, theta, implied_volatility
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, insert_values)

        conn.commit()
        conn.close()

        return jsonify({"message": f"{len(insert_values)} option positions inserted successfully."}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500



def get_trade_book():
    try:
        obj, username = login()
        tradebook_response = obj.tradeBook()
        logout(obj, username)

        if tradebook_response['status'] != True:
            return jsonify({"error": "Failed to fetch tradeBook", "details": tradebook_response}), 400

        trades = tradebook_response.get('data') or []  # fixes NoneType issue
        return jsonify({"trades": trades, "count": len(trades)})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

def get_order_book():
    try:
        obj, username = login()
        orderbook_response = obj.orderBook()
        logout(obj, username)

        if orderbook_response['status'] != True:
            return jsonify({"error": "Failed to fetch orderBook", "details": orderbook_response}), 400

        orders = orderbook_response.get('data') or []  # fixes NoneType issue
        return jsonify({"orders": orders, "count": len(orders)})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def get_strike_data(stockname, expiry, strike, option_type, underlyingLtp):
    try:
        # Step 1: Get Option Token
        expiry_input = request.args.get("expiry")  # e.g. '31JUL2025'
        expiry = datetime.strptime(expiry_input, "%d%b%Y").strftime("%Y-%m-%d")
        token_url = f"http://82.208.20.218:5000/get_option_token?name={stockname}&expiry={expiry}&strike={strike}&pe_ce={option_type.lower()}"
        token_res = requests.get(token_url)
        token = token_res.json().get("token")

        if not token:
            return jsonify({"error": "Option token not found"}), 400

        # Step 2: Format expiry to DDMMMYY
        expiry_dt = datetime.strptime(expiry, "%d%b%Y")
        formatted_expiry = expiry_dt.strftime("%d%b%y").upper()  # e.g., 31JUL25

        # Step 3: Login and get LTP
        obj, _ = login()
        symbol = f"{stockname}{formatted_expiry}{int(strike)}{option_type.upper()}"
        ltp_response = obj.ltpData("NFO", symbol, token)
        ltp_option = float(ltp_response['data']['ltp'])

        # Step 4: Delta/Theta computation
        days_to_expiry = DaysToExpiry(expiry) if len(expiry) > 0 else 0
        days_to_expiry = max(float(days_to_expiry), 0.5)
        delta, theta, implied_vol = calculate_greeks(underlyingLtp, strike, days_to_expiry, ltp_option, option_type)
        
        return jsonify({
            "ltp": ltp_option,
            "delta": delta,
            "theta": theta
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


