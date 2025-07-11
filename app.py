from flask import Flask, jsonify, request, send_file
import requests
import importlib
import sqlite3
from config import DB_PATH
from decimal import Decimal, ROUND_HALF_UP
from helpers import fetch_table_data, calculate_greeks, sanitize, get_last_expiry_date
from config import DB_PATH
from sqlalchemy import create_engine, MetaData, Table, insert, text
from datetime import datetime
import re
import os
from flask_cors import CORS

app = Flask(__name__)
CORS(app, origins=["http://82.208.20.218:3001"])

app.secret_key = "secretkey"
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='None',   # Use 'None' for cross-origin co
    SESSION_COOKIE_SECURE=True,       # True is required when SAMESITE='None' (and you using HTTPS)
)


def sync_buying_schedule_test():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT stock_name, quantity_per_buy, no_of_lots,
                   target_stock_price, strikeprice, totalsellavgprice, total_stocks
            FROM buying_schedule
        ''')
        schedules = cursor.fetchall()

        details = []

        for sched in schedules:
            stock_name, quantity_per_buy, no_of_lots, target_stock_price, strikeprice, totalsellavgprice, total_stocks = sched

            # Fetch stock data
            cursor.execute('''
                SELECT netqty, underlying_ltp, buyPrice
                FROM stockpositions
                WHERE symbolname = ?
                ORDER BY id DESC LIMIT 1
            ''', (stock_name,))
            stock_data = cursor.fetchone()

            if not stock_data:
                details.append({
                    "stock_name": stock_name,
                    "status": "skipped",
                    "reason": "stock not found in stockpositions"
                })
                continue

            quantity, underlying_ltp, buyprice = stock_data

            if total_stocks == quantity:
                details.append({
                    "stock_name": stock_name,
                    "status": "skipped",
                    "reason": "total_stocks equals netqty",
                    "netqty": quantity,
                    "total_stocks": total_stocks
                })
                continue

            try:
                if total_stocks is None or no_of_lots in (0, None):
                    return jsonify({"error": "Missing total_stocks or invalid no_of_lots"}), 400

                lot_size = round(total_stocks / no_of_lots) if no_of_lots > 0 else 0
                lots = round(quantity / lot_size, 2) if lot_size > 0 else 0

                payload = {
                    "stock_name": stock_name,
                    "quantity_per_buy": quantity_per_buy,
                    "no_of_lots": no_of_lots,
                    "target_stock_price": target_stock_price,
                    "strikeprice": strikeprice,
                    "totalsellavgprice": totalsellavgprice,
                    "lastTradedPrice": underlying_ltp,
                    "quantity": quantity,
                    "buyPrice": buyprice,
                    "lots": lots
                }

                api_resp = requests.post(
                    "http://82.208.20.218:5000/buying-schedule-direct",
                    json=payload,
                    timeout=10
                )
                api_resp.raise_for_status()
                api_data = api_resp.json()

                if not isinstance(api_data, list) or not api_data:
                    details.append({
                        "stock_name": stock_name,
                        "status": "error",
                        "reason": "Invalid or empty response from buying-schedule-direct"
                    })
                    continue

                buy_steps_difference = api_data[-1].get('buy_steps_difference')
                if buy_steps_difference is None:
                    details.append({
                        "stock_name": stock_name,
                        "status": "error",
                        "reason": "buy_steps_difference missing in response"
                    })
                    continue

                alert_value = underlying_ltp + buy_steps_difference

                cursor.execute('''
                    DELETE FROM alerts
                    WHERE stockname = ? AND conditions = 'buy_steps_diff_trigger'
                ''', (stock_name,))

                cursor.execute('''
                    INSERT INTO alerts (stockname, conditions, value)
                    VALUES (?, ?, ?)
                ''', (stock_name, 'buy_steps_diff_trigger', alert_value))

                cursor.execute('''
                    UPDATE buying_schedule
                    SET total_stocks = ?, buy_steps_difference = ?
                    WHERE stock_name = ?
                ''', (quantity, buy_steps_difference, stock_name))

                details.append({
                    "stock_name": stock_name,
                    "status": "success",
                    "buy_steps_difference": buy_steps_difference,
                    "alert_value": alert_value
                })

            except Exception as api_error:
                details.append({
                    "stock_name": stock_name,
                    "status": "error",
                    "reason": "API call failed",
                    "error": str(api_error)
                })
                continue

        conn.commit()
        conn.close()

        return jsonify({
            "message": "Sync completed",
            "details": details
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_connection():
    return create_engine("sqlite:///{DB_PATH}")

def get_order_book():
    return [
        {"tradingsymbol": "TATAPOWER26JUN25430CE", "transactiontype": "SELL", "averageprice": 1.5, "filledshares": "1350"},
        {"tradingsymbol": "KOTAKBANK", "transactiontype": "SELL", "averageprice": 2150.0, "filledshares": "40"},
    ]

def get_broker_module():
    # Connect using the absolute path
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT broker FROM user LIMIT 1")
    result = cursor.fetchone()
    conn.close()

    if not result:
        raise Exception("No broker found in user table.")

    broker_name = result[0].lower()
    return importlib.import_module(f"brokers.{broker_name}")

def get_lot_sizes(symbols):
    """Fetch lot sizes for all symbols from external API"""
    lot_sizes = {}
    api_url = "https://thetagains.pythonanywhere.com/get_lotsize"

    for symbol in symbols:
        try:
            payload = {"stockname": symbol}
            print(f"Sending to API: {payload}")
            response = requests.post(api_url, json=payload)

            print(f"Response [{symbol}] = {response.status_code} | {response.text.strip()}")

            if response.status_code == 200:
                lot_size_str = response.text.strip()
                try:
                    lot_size = Decimal(lot_size_str)
                    if lot_size > 0:
                        lot_sizes[symbol] = lot_size
                    else:
                        raise ValueError("Non-positive lot size")
                except:
                    print(f"Invalid lot size for {symbol}: {lot_size_str}")
                    lot_sizes[symbol] = Decimal(1)
            else:
                print(f"Failed HTTP fetch for {symbol}, status: {response.status_code}")
                lot_sizes[symbol] = Decimal(1)

        except Exception as e:
            print(f" Exception fetching lot size for {symbol}: {e}")
            lot_sizes[symbol] = Decimal(1)


    return lot_sizes

def insert_pnl_entry(conn, entry):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO pnl (
            symbolname, instrumenttype, optiontype, strikeprice, expirydate,
            quantity, entry_price, exit_price, direction, realized_pnl,
            exit_time, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        entry['symbolname'], entry['instrumenttype'], entry.get('optiontype'), entry.get('strikeprice'),
        entry.get('expirydate'), entry['quantity'], entry['entry_price'], entry['exit_price'],
        entry['direction'], entry['realized_pnl'], entry['exit_time'], entry['source']
    ))
    conn.commit()

def match_orders(tradingsymbol, direction, qty_needed, orders):
    matched = []
    qty_accumulated = 0

    for order in orders:
        if order['tradingsymbol'] != tradingsymbol or order['transactiontype'] != direction:
            continue
        filled_qty = int(order['filledshares'])
        if filled_qty == 0:
            continue

        matched.append(order)
        qty_accumulated += filled_qty
        if qty_accumulated >= qty_needed:
            break

    return matched[:], qty_accumulated

def weighted_avg_price(matched_orders):
    total_val = sum(float(o['averageprice']) * int(o['filledshares']) for o in matched_orders)
    total_qty = sum(int(o['filledshares']) for o in matched_orders)
    return total_val / total_qty if total_qty else 0.0

def update_expiry_pnl(conn):
    cursor = conn.cursor()
    last_expiry = get_last_expiry_date()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Step 1: Clear old values
    cursor.execute("DELETE FROM expiry_pnl")

    # Step 2: Calculate current-cycle PnL
    cursor.execute("""
        SELECT symbolname, SUM(realized_pnl) as total_pnl
        FROM pnl
        WHERE exit_time > ?
        GROUP BY symbolname
    """, (last_expiry,))

    rows = cursor.fetchall()

    # Step 3: Insert refreshed PnL values
    for row in rows:
        cursor.execute("""
            INSERT INTO expiry_pnl (symbolname, realized_pnl, record_time)
            VALUES (?, ?, ?)
        """, (row[0], row[1], now_str))

    conn.commit()


@app.route("/compute_pnl", methods=['POST', 'GET'])
def compute_pnl():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Step 1: Fetch latest and previous snapshots
    curr_options = fetch_table_data(cursor, "optionpositions")
    prev_options = fetch_table_data(cursor, "optionpositions_prev")
    curr_stocks = fetch_table_data(cursor, "stockpositions")
    prev_stocks = fetch_table_data(cursor, "stockpositions_prev")
    orders = get_order_book()  # today's orders from broker

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Step 2: Process Options PnL
    for curr in curr_options:
        key = (curr['symbolname'], curr['optiontype'], curr['strikeprice'], curr['expirydate'])
        prev = next((p for p in prev_options if
                     (p['symbolname'], p['optiontype'], p['strikeprice'], p['expirydate']) == key), None)
        if not prev:
            continue

        prev_qty = abs(prev['netqty'])
        curr_qty = abs(curr['netqty'])

        if prev_qty > curr_qty:
            exited_qty = prev_qty - curr_qty
            direction = 'SELL' if prev['netqty'] > 0 else 'BUY'

            # Ensure strikeprice has no decimal .0 if not needed
            strike_str = str(int(curr['strikeprice'])) if curr['strikeprice'].is_integer() else str(curr['strikeprice'])
            expiry_str = curr['expirydate'].replace('-', '')

            expiry_fmt = datetime.strptime(curr['expirydate'], "%Y-%m-%d").strftime("%d%b%y").upper()
            strike_fmt = str(int(curr['strikeprice'])) if curr['strikeprice'].is_integer() else str(curr['strikeprice'])
            tradingsymbol = f"{curr['symbolname']}{expiry_fmt}{strike_fmt}{curr['optiontype']}"


            matched_orders, total_filled = match_orders(tradingsymbol, direction, exited_qty, orders)
            if total_filled == 0:
                continue

            exit_price = weighted_avg_price(matched_orders)
            entry_price = prev['totalbuyavgprice'] if direction == 'SELL' else prev['totalsellavgprice']
            realized_pnl = exited_qty * (exit_price - entry_price) * (1 if direction == 'SELL' else -1)

            insert_pnl_entry(conn, {
                'symbolname': curr['symbolname'],
                'instrumenttype': 'OPTION',
                'optiontype': curr['optiontype'],
                'strikeprice': curr['strikeprice'],
                'expirydate': curr['expirydate'],
                'quantity': exited_qty,
                'entry_price': entry_price,
                'exit_price': exit_price,
                'direction': direction,
                'realized_pnl': realized_pnl,
                'exit_time': now_str,
                'source': 'option'
            })
            update_expiry_pnl(conn)

    # Step 3: Process Stock PnL
    for curr in curr_stocks:
        prev = next((p for p in prev_stocks if p['symbolname'] == curr['symbolname']), None)
        if not prev:
            continue

        prev_qty = abs(prev['netqty'])
        curr_qty = abs(curr['netqty'])

        if prev_qty > curr_qty:
            exited_qty = prev_qty - curr_qty
            direction = 'SELL' if prev['netqty'] > 0 else 'BUY'
            tradingsymbol = curr['symbolname']

            matched_orders, total_filled = match_orders(tradingsymbol, direction, exited_qty, orders)
            if total_filled == 0:
                continue

            exit_price = weighted_avg_price(matched_orders)
            entry_price = prev['buyprice']
            realized_pnl = exited_qty * (exit_price - entry_price) * (1 if direction == 'SELL' else -1)

            insert_pnl_entry(conn, {
                'symbolname': curr['symbolname'],
                'instrumenttype': 'STOCK',
                'optiontype': None,
                'strikeprice': None,
                'expirydate': None,
                'quantity': exited_qty,
                'entry_price': entry_price,
                'exit_price': exit_price,
                'direction': direction,
                'realized_pnl': realized_pnl,
                'exit_time': now_str,
                'source': 'stock'
            })
            update_expiry_pnl(conn)

    # Step 4: Refresh snapshot tables ONLY after all processing
    cursor.execute("DELETE FROM optionpositions_prev")
    cursor.execute("INSERT INTO optionpositions_prev SELECT * FROM optionpositions")

    cursor.execute("DELETE FROM stockpositions_prev")
    cursor.execute("INSERT INTO stockpositions_prev SELECT * FROM stockpositions")

    conn.commit()
    conn.close()

    return jsonify({"status": "success", "message": "PnL computed and recorded."})

@app.route('/stock_holdings', methods=['GET'])
def stock_holdings():
    try:
        broker_module = get_broker_module()
        return broker_module.stock_holdings()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/insert_positions', methods=['GET'])
def insert_positions():
    try:
        broker_module = get_broker_module()
        return broker_module.insert_positions()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/broker_login', methods=['GET'])
def broker_login():
    try:
        broker_module = get_broker_module()
        return jsonify(broker_module.login())
        #return jsonify({"status": f"{broker_module.__name__} module loaded"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get_order_book1', methods=['GET'])
def get_trade_book1():
    try:
        broker_module = get_broker_module()
        return broker_module.get_order_book()
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/merge_tables', methods=['POST', 'GET'])
def merge_tables():
    """Merge stockpositions and optionpositions into all_trades using sqlite3"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Step 1: Fetch stockpositions
        cursor.execute("""
            SELECT
                symbolname AS stock_name,
                netqty AS stock_quantity,
                unrealised AS stock_pnl,
                underlying_ltp AS stock_price
            FROM stockpositions
        """)
        stock_results = cursor.fetchall()

        # Step 2: Fetch optionpositions with delta direction
        cursor.execute("""
            SELECT
                symbolname,
                optiontype,
                totallots,
                delta,
                COALESCE(totalsellavgprice, '') AS totalsellavgprice,
                COALESCE(totalbuyavgprice, '') AS totalbuyavgprice,
                COALESCE(unrealised, 0) + COALESCE(realised, 0) AS current_pnl
            FROM optionpositions
        """)
        option_raw = cursor.fetchall()

        # Step 3: Get lot sizes
        symbols = {row[0] for row in stock_results + option_raw}
        lot_sizes = get_lot_sizes(symbols)

        merged_data = {}

        # Step 4: Process stockpositions
        for row in stock_results:
            stock_name, stock_quantity, stock_pnl, stock_price = row
            lot_size = lot_sizes.get(stock_name, Decimal(1))
            stock_delta = Decimal(stock_quantity or 0) / lot_size if lot_size else Decimal(0)

            if stock_quantity == 0 or not stock_price:
                cursor.execute("""
                    SELECT underlying_ltp FROM optionpositions
                    WHERE symbolname = ?
                    ORDER BY id DESC LIMIT 1
                """, (stock_name,))
                fallback_result = cursor.fetchone()
                if fallback_result:
                    stock_price = fallback_result[0]

            merged_data[stock_name] = {
                "stock_name": stock_name,
                "stock_price": float(Decimal(stock_price or 0).quantize(Decimal('0.01'))),
                "stock_quantity": float(Decimal(stock_quantity or 0).quantize(Decimal('0.01'))),
                "put_quantity": 0,
                "call_quantity": 0,
                "long_delta": float(stock_delta.quantize(Decimal('0.01'))),
                "short_delta": 0.0,
                "current_pnl": float(Decimal(stock_pnl or 0).quantize(Decimal('0.01'))),
                "total_potential_profit": None,
                "alerts": None
            }

        # Step 5: Process optionpositions manually to determine signed delta
        for row in option_raw:
            symbolname, optiontype, totallots, delta, sell_price, buy_price, current_pnl = row

            # Determine if position is buy or sell
            is_sold = sell_price is not None and sell_price > 0
            is_bought = buy_price is not None and buy_price > 0
            direction = 0  # default neutral

            if optiontype == 'CE':
                if is_sold:
                    direction = -1  # short call
                elif is_bought:
                    direction = 1   # long call
            elif optiontype == 'PE':
                if is_sold:
                    direction = -1   # short put
                elif is_bought:
                    direction = 1  # long put

            delta_val = Decimal(delta or 0) * Decimal(totallots or 0) * direction
            current_pnl = Decimal(current_pnl or 0).quantize(Decimal('0.01'))

            if optiontype == 'CE':
                call_qty = totallots or 0
                put_qty = 0
            elif optiontype == 'PE':
                put_qty = totallots or 0
                call_qty = 0
            else:
                call_qty = put_qty = 0

            if symbolname in merged_data:
                merged_data[symbolname]["current_pnl"] += float(current_pnl)
                merged_data[symbolname]["put_quantity"] += put_qty
                merged_data[symbolname]["call_quantity"] += call_qty

                if delta_val >= 0:
                    merged_data[symbolname]["long_delta"] += float(delta_val.quantize(Decimal('0.01')))
                else:
                    merged_data[symbolname]["short_delta"] += float((delta_val).quantize(Decimal('0.01')))

            else:
                merged_data[symbolname] = {
                    "stock_name": symbolname,
                    "stock_price": 0.0,
                    "stock_quantity": 0.0,
                    "put_quantity": put_qty,
                    "call_quantity": call_qty,
                    "long_delta": float(delta_val.quantize(Decimal('0.01'))) if delta_val > 0 else 0.0,
                    "short_delta": float((-delta_val).quantize(Decimal('0.01'))) if delta_val < 0 else 0.0,
                    "current_pnl": float(current_pnl),
                    "total_potential_profit": None,
                    "alerts": None
                }

        # Step 6: Clear all_trades
        cursor.execute("DELETE FROM all_trades")

        # Step 7: Insert merged data with all values to 2 decimal places
        for data in merged_data.values():
            cursor.execute("""
                INSERT INTO all_trades (
                    stock_name, stock_price, stock_quantity,
                    put_quantity, call_quantity,
                    long_delta, short_delta,
                    current_pnl, total_potential_profit, alerts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data["stock_name"],
                round(data["stock_price"], 2),
                round(data["stock_quantity"], 2),
                round(data["put_quantity"], 2),
                round(data["call_quantity"], 2),
                round(data["long_delta"], 2),
                round(data["short_delta"], 2),
                round(data["current_pnl"], 2),
                data["total_potential_profit"],
                data["alerts"]
            ))

        conn.commit()
        conn.close()
        return "Data merged successfully into all_trades!"

    except Exception as e:
        return f"Error during merge: {e}"

@app.route('/get_all_trades', methods=['POST', 'GET'])
def get_all_trades():
    try:
        data = request.get_json() if request.method == 'POST' else None
        stock_name = data.get('stock_name') if data else request.args.get('stock_name')

        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row  # Enables dict-like access to rows
        cursor = conn.cursor()

        if stock_name:
            cursor.execute("SELECT * FROM all_trades WHERE stock_name = ?", (stock_name,))
        else:
            cursor.execute("SELECT * FROM all_trades")

        rows = cursor.fetchall()
        trades = [dict(row) for row in rows]

        conn.close()
        return jsonify(trades)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_option_positions', methods=['POST', 'GET'])
def get_option_positions():
    """Fetches options positions and returns in required JSON format"""

    # Handle POST request
    if request.method == 'POST':
        content_type = request.headers.get('Content-Type')
        if content_type != 'application/json':
            return jsonify({"error": "Invalid content type"}), 400
        data = request.get_json()
        stockname = data.get('stockname')
    elif request.method == 'GET':
        stockname = request.args.get('stockname')

    if not stockname:
        return jsonify({"error": "Missing stockname parameter"}), 400

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        query = """
            SELECT
                expirydate,
                optiontype AS type,
                totallots AS lots,
                strikeprice,
                totalbuyavgprice,
                totalsellavgprice,
                ltp,
                delta,
                theta,
                unrealised AS unrealizedPnL
            FROM optionpositions
            WHERE symbolname = ?
        """
        cursor.execute(query, (stockname,))
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return jsonify([])

        response = []
        for row in rows:
            response.append({
                "expiry": row["expirydate"] or "",
                "type": row["type"],
                "lots": int(row["lots"]) if row["lots"] else 0,
                "strikePrice": float(row["strikeprice"]) if row["strikeprice"] else 0.0,
                "buyPrice": float(row["totalbuyavgprice"]) if row["totalbuyavgprice"] else "",
                "sellPrice": float(row["totalsellavgprice"]) if row["totalsellavgprice"] else "",
                "lastTradedPrice": float(row["ltp"]) if row["ltp"] else 0.0,
                "delta": float(row["delta"]) if row["delta"] else 0.0,
                "theta": float(row["theta"]) if row["theta"] else 0.0,
                "unrealizedPnL": float(row["unrealizedPnL"]) if row["unrealizedPnL"] else 0.0
            })

        return jsonify(response)

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500


@app.route('/get_stock_positions', methods=['POST', 'GET'])
def get_stock_positions():
    """Fetches stock positions and returns in required JSON format"""

    if request.method == 'POST':
        content_type = request.headers.get('Content-Type')
        if content_type != 'application/json':
            return jsonify({"error": "Invalid content type"}), 400
        data = request.get_json()
        stockname = data.get('stockname')
    elif request.method == 'GET':
        stockname = request.args.get('stockname')

    if not stockname:
        return jsonify({"error": "Missing stockname parameter"}), 400

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        query = """
            SELECT
                netqty AS quantity,
                underlying_ltp AS lastTradedPrice,
                unrealised AS unrealizedPnL,
                buyprice AS buyPrice
            FROM stockpositions
            WHERE symbolname = ?
        """
        cursor.execute(query, (stockname,))
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return jsonify([])

        response = []
        for row in rows:
            quantity = row["quantity"] or 0
            buy_price = row["buyPrice"] or 0
            unrealized_pnl = row["unrealizedPnL"] or 0
            last_traded_price = row["lastTradedPrice"] or 0

            # Default lot size
            lot_size = 1
            try:
                api_url = "https://thetagains.pythonanywhere.com/get_lotsize"
                api_response = requests.post(api_url, json={"stockname": stockname})
                if api_response.status_code == 200:
                    lot_size_str = api_response.text.strip()
                    if lot_size_str.isdigit():
                        lot_size = max(1, int(lot_size_str))
            except Exception as e:
                print(f"Failed to fetch lot size: {e}")

            lots = round((quantity / lot_size), 2)

            response.append({
                "quantity": quantity,
                "lots": lots,
                "buyPrice": float(buy_price),
                "unrealizedPnL": round(float(unrealized_pnl), 2),
                "lastTradedPrice": float(last_traded_price),
                "lotsize": lot_size
            })

        return jsonify(response)

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500

@app.route('/add_alert', methods=['POST'])
def add_alert():
    data = request.get_json()
    stockname = data.get('stockname')
    conditions = data.get('conditions')
    value = data.get('value')

    if not all([stockname, conditions, value]):
        return jsonify({'error': 'Missing one or more required fields'}), 400

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Check for duplicate
        check_query = """
            SELECT 1 FROM alerts
            WHERE stockname = ? AND conditions = ? AND value = ?
            LIMIT 1
        """
        cursor.execute(check_query, (stockname, conditions, float(value)))
        result = cursor.fetchone()

        if result:
            return jsonify({'message': 'Alert already exists'}), 200

        # Insert new alert
        insert_query = """
            INSERT INTO alerts (stockname, conditions, value)
            VALUES (?, ?, ?)
        """
        cursor.execute(insert_query, (stockname, conditions, float(value)))
        conn.commit()

        return jsonify({'message': 'Alert added successfully'}), 200

    except Exception as e:
        print("Database Error:", e)
        return jsonify({'error': 'Database error'}), 500

    finally:
        conn.close()

@app.route('/get_alerts', methods=['GET'])
def get_alerts():
    stockname = request.args.get('stockname')
    is_option = request.args.get('is_option', 'false').lower() == 'true'

    if not stockname:
        return jsonify({'error': 'Stockname required'}), 400

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if is_option:
            query = """
                SELECT id, stockname, conditions, value, created_at
                FROM alerts
                WHERE stockname = ? AND conditions NOT IN ('Stock Price Above', 'Stock Price Below')
                ORDER BY created_at DESC
            """
        else:
            query = """
                SELECT id, stockname, conditions, value, created_at
                FROM alerts
                WHERE stockname = ? AND conditions IN ('Stock Price Above', 'Stock Price Below')
                ORDER BY created_at DESC
            """

        cursor.execute(query, (stockname,))
        rows = cursor.fetchall()
        alerts = [dict(row) for row in rows]

        return jsonify({'alerts': alerts}), 200

    except Exception as e:
        print("Error fetching alerts:", e)
        return jsonify({'error': 'Database error'}), 500

    finally:
        conn.close()

@app.route('/delete_alert', methods=['POST'])
def delete_alert():
    data = request.get_json()
    alert_id = data.get('id')

    if not alert_id:
        return jsonify({'error': 'Missing alert ID'}), 400

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        delete_query = "DELETE FROM alerts WHERE id = ?"
        cursor.execute(delete_query, (alert_id,))
        conn.commit()

        if cursor.rowcount == 0:
            return jsonify({'message': 'Alert not found'}), 404

        return jsonify({'message': 'Alert deleted'}), 200

    except Exception as e:
        print("Error deleting alert:", e)
        return jsonify({'error': 'Database error'}), 500

    finally:
        conn.close()

@app.route('/save_schedule', methods=['POST'])
def save_schedule():
    data = request.get_json()

    required_fields = ['stock_name', 'total_buy_steps', 'no_of_lots', 'target_stock_price']
    for field in required_fields:
        if field not in data:
            return jsonify({'error': f'Missing required field: {field}'}), 400

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Step 1: Fetch total_stocks from stockpositions
        cursor.execute('''
            SELECT netqty FROM stockpositions
            WHERE symbolname = ?
            ORDER BY id DESC LIMIT 1
        ''', (data['stock_name'],))
        result = cursor.fetchone()
        total_stocks = result[0] if result else 0  # default to 0 if not found

        # Step 2: Insert into buying_schedule
        cursor.execute('''
            INSERT INTO buying_schedule (
                stock_name, total_buy_steps, no_of_lots,
                target_stock_price, strikeprice, totalsellavgprice, total_stocks
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            data['stock_name'],
            data['total_buy_steps'],
            data['no_of_lots'],
            data['target_stock_price'],
            data.get('strikeprice'),          # optional
            data.get('totalsellavgprice'),    # optional
            total_stocks
        ))

        conn.commit()
        conn.close()
        return jsonify({'message': 'Schedule saved successfully'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_saved_schedules', methods=['POST'])
def get_saved_schedules():
    try:
        data = request.get_json()
        stock_name = data.get('stock_name')

        if not stock_name:
            return jsonify({"error": "Missing stock_name"}), 400

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        query = '''
            SELECT id, stock_name, total_buy_steps, no_of_lots, target_stock_price,
                   strikeprice, totalsellavgprice
            FROM buying_schedule
            WHERE stock_name = ?
        '''
        cursor.execute(query, (stock_name,))
        rows = cursor.fetchall()

        schedules = []
        for row in rows:
            schedules.append({
                "id": row[0],
                "stock_name": row[1],
                "total_buy_steps": row[2],
                "no_of_lots": row[3],
                "target_stock_price": row[4],
                "strikeprice": row[5],
                "totalsellavgprice": row[6]
            })

        conn.close()
        return jsonify(schedules), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/delete-schedule/<int:id>', methods=['DELETE'])
def delete_schedule(id):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Check if the row exists
        cursor.execute("SELECT * FROM buying_schedule WHERE id = ?", (id,))
        row = cursor.fetchone()
        if not row:
            return jsonify({"error": f"Schedule with ID {id} not found."}), 404

        # Delete the row
        cursor.execute("DELETE FROM buying_schedule WHERE id = ?", (id,))
        conn.commit()
        conn.close()

        return jsonify({"message": f"Schedule with ID {id} deleted successfully."}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route('/sync_buying_schedule', methods=['GET', 'POST'])
def sync_buying_schedule():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Get all unique buying_schedule entries
        cursor.execute('''
            SELECT stock_name, quantity_per_buy, no_of_lots,
                   target_stock_price, strikeprice, totalsellavgprice, total_stocks
            FROM buying_schedule
        ''')
        schedules = cursor.fetchall()

        for sched in schedules:
            stock_name, quantity_per_buy, no_of_lots, target_stock_price, strikeprice, totalsellavgprice, total_stocks = sched

            # Get netqty and underlying_ltp from stockpositions for the stock
            cursor.execute('''
                SELECT netqty, underlying_ltp FROM stockpositions
                WHERE symbolname = ?
                ORDER BY id DESC LIMIT 1
            ''', (stock_name,))
            stock_data = cursor.fetchone()

            if not stock_data:
                continue  # Skip if stock not found

            netqty, underlying_ltp = stock_data

            if total_stocks != netqty:
                # Prepare payload for API call
                payload = {
                    'stock_name': stock_name,
                    'quantity_per_buy': quantity_per_buy,
                    'no_of_lots': no_of_lots,
                    'target_stock_price': target_stock_price,
                    'strikeprice': strikeprice,
                    'totalsellavgprice': totalsellavgprice
                }


                try:
                    # Call external API
                    api_resp = requests.post(
                        "http://82.208.20.218:5000/buying-schedule",
                        json=payload,
                        timeout=10
                    )
                    api_resp.raise_for_status()
                    api_data = api_resp.json()

                    buy_steps_difference = api_data.get('buy_steps_difference')
                    if buy_steps_difference is None:
                        continue  # Skip if API did not return expected field

                    # Calculate alert value
                    alert_value = underlying_ltp + buy_steps_difference

                    # Delete existing alert with same condition
                    cursor.execute('''
                        DELETE FROM alerts
                        WHERE stockname = ? AND conditions = 'buy_steps_diff_trigger'
                    ''', (stock_name,))

                    # Insert new alert
                    cursor.execute('''
                        INSERT INTO alerts (stockname, conditions, value)
                        VALUES (?, ?, ?)
                    ''', (stock_name, 'buy_steps_diff_trigger', alert_value))

                    # Update total_stocks in buying_schedule
                    cursor.execute('''
                        UPDATE buying_schedule
                        SET total_stocks = ?, buy_steps_difference = ?
                        WHERE stock_name = ?
                    ''', (netqty, buy_steps_difference, stock_name))

                except Exception as api_error:
                    print(f"API error for {stock_name}: {api_error}")
                    continue

        conn.commit()
        conn.close()
        return jsonify({'message': 'Buying schedule sync completed'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_all_positions', methods=['GET'])
def get_all_positions():
    """Fetches all unique stock names from the all_trades table."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT DISTINCT stock_name FROM all_trades ORDER BY stock_name")
        rows = cursor.fetchall()

        # Extract stock names from query result
        stock_names = [row[0] for row in rows if row[0]]

        conn.close()
        return jsonify(stock_names)

    except Exception as e:
        return jsonify({"error": f"Internal Server Error: {str(e)}"}), 500

@app.route('/sync_all1', methods=['POST','GET'])
def sync_all1():
    try:
        # Get broker module
        broker_module = get_broker_module()

        # 1. Run stock_holdings
        broker_module.stock_holdings()

        # 2. Run insert_positions
        broker_module.insert_positions()

        # 3. Run merge_tables
        merge_tables()

        # 4. Run sync_buying_schedule
        #sync_buying_schedule()

        # 5. Run compute_pnl
        #compute_pnl()

        return jsonify({"status": "success", "message": "All functions executed successfully."})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/process_alerts', methods=['GET'])
def process_alerts():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("UPDATE all_trades SET alerts = NULL")
    # Fetch all alerts
    cursor.execute("SELECT stockname, conditions, value FROM alerts")
    alerts = cursor.fetchall()

    for stockname, condition, value in alerts:
        # Fetch matching stock from all_trades
        cursor.execute("SELECT stock_price FROM all_trades WHERE stock_name = ?", (stockname,))
        row = cursor.fetchone()

        if row:
            stock_price = row[0]

            # Apply the condition
            if condition == 'Stock Price Above' and stock_price > value:
                alert_message = f"Stock Price Above {value}"
                cursor.execute(
                    "UPDATE all_trades SET alerts = ? WHERE stock_name = ?",
                    (alert_message, stockname)
                )
            elif condition == 'Stock Price Below' and stock_price < value:
                alert_message = f"Stock Price Below {value}"
                cursor.execute(
                    "UPDATE all_trades SET alerts = ? WHERE stock_name = ?",
                    (alert_message, stockname)
                )

    conn.commit()
    conn.close()
    return jsonify({'status': 'success', 'message': 'Alerts processed successfully'})

@app.route('/debug_option_pnl', methods=['GET'])
def debug_option_pnl():
    debug_logs = []

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Fetch current and previous optionpositions
        cursor.execute("SELECT * FROM optionpositions")
        curr_options = [dict(row) for row in cursor.fetchall()]

        cursor.execute("SELECT * FROM optionpositions_prev")
        prev_options = [dict(row) for row in cursor.fetchall()]

        for curr in curr_options:
            key = (curr['symbolname'], curr['optiontype'], curr['strikeprice'], curr['expirydate'])

            matched_prev = next((p for p in prev_options if (
                p['symbolname'], p['optiontype'], p['strikeprice'], p['expirydate']) == key), None)

            if not matched_prev:
                debug_logs.append(f"No match in prev_options for key: {key}")
                continue

            prev_qty = abs(matched_prev['netqty'])
            curr_qty = abs(curr['netqty'])

            if curr_qty >= prev_qty:
                debug_logs.append(f"Match found for {key} | No exit detected | Prev Qty: {prev_qty}, Curr Qty: {curr_qty}")
            else:
                debug_logs.append(f"Exit detected for {key} | Prev Qty: {prev_qty}, Curr Qty: {curr_qty}")

        conn.close()
        return "<br>".join(debug_logs)

    except Exception as e:
        return f"Error: {str(e)}"

@app.route('/get_last_expiry_date', methods=['GET'])
def get_last_expiry_date1():
    return get_last_expiry_date()

@app.route('/testing', methods=['POST','GET'])
def testing():
    try:
         # 4. Run sync_buying_schedule
        sync_buying_schedule_test()

        return jsonify({"status": "success", "message": "syncing done"})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

#@app.route('/sync_buying_schedule_test', methods=['GET', 'POST'])


@app.route('/sync_all', methods=['POST','GET'])
def sync_all():
    try:
        # Get broker module
        #broker_module = get_broker_module()

        # 1. Run stock_holdings
        #broker_module.stock_holdings()

        # 2. Run insert_positions
        #broker_module.insert_positions()

        # 3. Run merge_tables
        #merge_tables()

        # 4. Run sync_buying_schedule
        #sync_buying_schedule()
        return sync_buying_schedule_test()
        # 5. Run compute_pnl
        #compute_pnl()

        #return jsonify({"status": "success", "message": "All functions executed successfully."})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/testing123', methods=['POST', 'GET'])
def testing123():
    payload = {
        "test": "connection from pythonanywhere",
        "timestamp": "2025-06-26T12:00:00"
    }

    try:
        res = requests.post("http://82.208.20.218:5000/test-connection", json=payload)
        return jsonify({
            "status": "success",
            "response_text": res.text,
            "status_code": res.status_code
        }), res.status_code
    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500

@app.route('/download-db')
def download_db():
    db_path = 'trading.db'  # Replace with actual DB file name or full path
    if os.path.exists(db_path):
        return send_file(db_path, as_attachment=True)
    else:
        return {"error": "Database file not found"}, 404


@app.route('/get_table_data', methods=['GET'])
def get_table_data():
    table_name = request.args.get('table')
    
    if not table_name:
        return jsonify({"error": "Missing 'table' query parameter"}), 400

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row  # To return dict-like rows
        cursor = conn.cursor()

        # Basic validation to avoid SQL injection via table name
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        if not cursor.fetchone():
            return jsonify({"error": f"Table '{table_name}' does not exist."}), 404

        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        data = [dict(row) for row in rows]

        return jsonify(data)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()


@app.route('/health')
def health_check():
    return 'ok', 200

if __name__ == "__main__":
    app.run(debug=True)
