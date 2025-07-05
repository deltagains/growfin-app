from flask import Flask, request, jsonify
from flask_cors import CORS
from SmartApi import SmartConnect
from sqlalchemy import create_engine, text, MetaData, Table, insert, select, func, and_
from sqlalchemy.orm import Session
from decimal import Decimal
import requests
import json
import pandas as pd
import math
import time
import numpy as np
import pyotp
import datetime
import mibian
import yfinance as yf
import os
import importlib
from broker_integration import angelone

BOT_TOKEN = '7538695805:AAFfwDRXnSBgDbFTjC67dkJU1gbKAGzGw3k'

def get_lot_sizes(symbols):
    """Fetch lot sizes for all symbols from external API"""
    lot_sizes = {}
    api_url = "https://thetagains.pythonanywhere.com/get_lotsize"

    for symbol in symbols:
        try:
            # Send POST request with symbol name
            response = requests.post(api_url, json={"stockname": symbol})

            if response.status_code == 200:
                # Extract plain string response and strip whitespace
                lot_size_str = response.text.strip()

                # Ensure the response is a valid number
                if lot_size_str.isdigit():
                    lot_sizes[symbol] = Decimal(lot_size_str)
                else:
                    print(f"️ Invalid lot size for {symbol}: {lot_size_str}")
                    lot_sizes[symbol] = Decimal(1)  # Fallback to 1 if invalid
            else:
                print(f" Failed to fetch lot size for {symbol}")
                lot_sizes[symbol] = Decimal(1)  # Fallback on error

        except Exception as e:
            print(f" Error fetching lot size for {symbol}: {e}")
            lot_sizes[symbol] = Decimal(1)  # Fallback in case of exceptions

    return lot_sizes



def send_telegram_message(message,chat_id):
    """Function to send message via Telegram bot"""
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
    payload = {
        'chat_id': chat_id,
        'text': message
    }
    response = requests.post(url, json=payload)
    return response.json()

def DaysToExpiry(expiry):
    mons = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}

    year = int(expiry[5:9])
    mon = mons[expiry[2:5]]
    day = int(expiry[0:2])
    today = datetime.date.today()
    expiryday = datetime.date(year, mon, day)
    diff = expiryday - today
    days_to_expiry = diff.days

    return(days_to_expiry)

def calculate_greeks(ltp_underlying, strike, days_to_expiry, ltp_option, pe_ce):

    try:
        if pe_ce == "CE":
            # Call option delta and IV calculation
            c = mibian.BS([ltp_underlying, strike, 10, days_to_expiry], callPrice=ltp_option)
            c2 = mibian.BS([ltp_underlying, strike, 10, days_to_expiry], volatility=c.impliedVolatility)
            delta = round(c2.callDelta, 2)
            theta = round(c2.callTheta, 2)
            implied_volatility = c.impliedVolatility

        elif pe_ce == "PE":
            # Put option delta and IV calculation
            p = mibian.BS([ltp_underlying, strike, 10, days_to_expiry], putPrice=ltp_option)
            p2 = mibian.BS([ltp_underlying, strike, 10, days_to_expiry], volatility=p.impliedVolatility)
            delta = round(p2.putDelta, 2)
            theta = round(p2.putTheta, 2)
            implied_volatility = p.impliedVolatility

        else:
            # Fallback in case of invalid type
            delta = 0.0
            theta = 0.0
            implied_volatility = 0.0

    except Exception as e:
        print(f"Error calculating greeks: {str(e)}")
        delta = 0.0
        theta = 0.0
        implied_volatility = 0.0

    return delta, theta, implied_volatility


def login_process():
    #global post_data
    #post_data = request.get_json()

    username = 'ROTC1004'
    password = '0698'
    api_key_value = 'Nsiz7EkP'
    token = 'WVBD6LBXLHJQFFEV5LWK3X52AY'
    otp_token = pyotp.TOTP(token).now()

    global obj
    obj = SmartConnect(api_key=api_key_value)
    data = obj.generateSession(username,password,otp_token)


def logout_process():
    username = 'ROTC1004' #post_data['username']
    logout=obj.terminateSession(username)

def insert_into_db(username, pin, api_key, token):
    """Insert data into the user table."""
    try:
        # Establish connection
        engine = get_connection()

        # Create a connection object from the engine
        with engine.connect() as conn:
            query = text("INSERT INTO user (username, pin, api_key, token) VALUES (:username, :pin, :api_key, :token)")

            # Execute query with named parameters
            conn.execute(query, {
                "username": username,
                "pin": pin,
                "api_key": api_key,
                "token": token
            })

            print("Data inserted successfully!")  # Debug message

        return "Inserted"

    except Exception as e:
        print(f"Error inserting into DB: {str(e)}")  # Log the error
        return f"Error: {str(e)}"

def get_connection():
    user = 'deltagainsprod'
    password = 'Rocho234'
    host = 'deltagainsprod.mysql.pythonanywhere-services.com'
    port = 3306
    database = 'deltagainsprod$default'

    return create_engine(
        url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database
        )
    )

def get_connection2():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    database = os.getenv("DB_NAME")

    return create_engine(
        url=f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    )

#load_dotenv()

app = Flask(__name__)
#CORS(app)
app.secret_key = "secretkey"
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='None',   # Use 'None' for cross-origin cookies
    SESSION_COOKIE_SECURE=True,       # True is required when SAMESITE='None' (and you're using HTTPS)
)

CORS(
    app,
    resources={r"/*": {"origins": "http://82.208.20.218:3001"}},
    supports_credentials=True
)

@app.route('/')

@app.route('/credentials', methods=['GET', 'POST'])
def credentials():
    if request.method == 'POST':
        try:
            username = request.form.get('username', '')
            pin = request.form.get('pin', '')
            api_key = request.form.get('api_key', '')
            token = request.form.get('token', '')

            print("Received data:", username, pin, api_key, token)

            insert_into_db(username, pin, api_key, token)

            return redirect('/')

        except Exception as e:
            print(f"Error: {str(e)}")  # Print the error in the logs
            return jsonify({"error": str(e)}), 500

    return '''
        <form method="post">
            Username: <input type="text" name="username" required><br>
            PIN: <input type="password" name="pin" required><br>
            API Key: <input type="text" name="api_key" required><br>
            Token: <input type="text" name="token" required><br>
            <input type="submit" value="Submit">
        </form>
    '''

@app.route('/stock_holdings', methods=['GET'])
def holding_positions():
    """
    Fetches all stock holdings and inserts them into the stockpositions table.
    """
    try:
        # Connect to DB
        engine = get_connection()
        connection = engine.connect()

        # Fetch all stock holdings
        login_process()
        ds = obj.holding()
        logout_process()

        # Convert to DataFrame
        position_data = pd.DataFrame.from_dict(ds)

        # Prepare data for insertion
        stock_data = []

        for i in range(len(position_data)):
            stock = position_data['data'][i]

            averageprice = float(stock['averageprice'])
            underlying_ltp = float(stock['ltp'])
            quantity = int(stock['quantity'])
            unrealised = (underlying_ltp - averageprice) * quantity

            stock_data.append({
                'symbolname': stock['tradingsymbol'][:-3],       # Remove suffix
                'underlying_ltp': underlying_ltp,
                'netqty': quantity,
                'buyprice': averageprice,
                'unrealised': unrealised
            })

        if not stock_data:
            return jsonify({"message": "No stock data found."})

        # Insert into MySQL using SQLAlchemy
        metadata = MetaData()
        stockpositions = Table('stockpositions', metadata, autoload_with=engine)

        # Clear previous stock data before inserting new data
        connection.execute(stockpositions.delete())

        # Insert new stock data
        connection.execute(insert(stockpositions), stock_data)

        connection.close()

        return jsonify({
            "message": f"{len(stock_data)} rows inserted into stockpositions."
        })

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/insert_positions', methods=['GET'])
def insert_option_positions():
    login_process()
    try:
        # Get connection from get_connection()
        conn = get_connection()

        # Fetch position data
        ds = obj.position()
        position_data = pd.DataFrame.from_dict(ds)

        if position_data.empty:
            return jsonify({"message": "No position data found"}), 200

        # Clear old data from the database
        delete_query = "DELETE FROM optionpositions"
        exe = conn.execute(delete_query)
        exe.close()

        # Step 1: Get unique symbols
        symbols = list(set(pos['symbolname'] for pos in position_data['data']))

        # Step 2: Fetch LTP for each symbol and cache it
        ltp_cache = {}  # Store LTP for each symbol

        for symbolname in symbols:
            try:
                p1 = {'symbol_name': symbolname}
                t = requests.post(url="http://thetagains.pythonanywhere.com/check_underlying_token", json=p1)
                t.raise_for_status()
                underlying_token = t.json()

                ltp_data = obj.ltpData('NSE', symbolname, underlying_token)
                ltp_cache[symbolname] = str(ltp_data['data'].get('ltp', '0'))

            except (requests.RequestException, KeyError, ValueError) as e:
                print(f"Error fetching LTP for {symbolname}: {str(e)}")
                ltp_cache[symbolname] = '0'

        # Step 3: Insert new positions into the database
        insert_query = """
        INSERT INTO optionpositions (
            underlying_ltp, strikeprice, optiontype, totalsellavgprice,
            totalbuyavgprice, ltp, netqty, totallots, lotsize,
            unrealised, realised, symbolname, expirydate, delta, theta, implied_volatility
        ) VALUES (
            :underlying_ltp, :strikeprice, :optiontype, :totalsellavgprice,
            :totalbuyavgprice, :ltp, :netqty, :totallots, :lotsize,
            :unrealised, :realised, :symbolname, :expirydate, :delta, :theta, :implied_volatility
        )
        """

        positions = []
        for pos in position_data['data']:
            symbolname = pos.get('symbolname', '')
            ltp_underlying = float(ltp_cache.get(symbolname, '0'))
            strikeprice = pos.get('strikeprice', 0)
            optiontype = pos.get('optiontype', '')
            interest_rate = 10  # Assuming a fixed interest rate of 10%
            ltp_option = float(pos.get('ltp', 0))
            days_to_expiry = DaysToExpiry(pos.get('expirydate')) if len(pos.get('expirydate')) > 0 else 0
            days_to_expiry = max(float(days_to_expiry), 0.5)

            delta, theta, implied_volatility = calculate_greeks(ltp_underlying, strikeprice, days_to_expiry, ltp_option, optiontype)
            totallots = abs(int(pos.get('netqty', 0)) / int(pos.get('lotsize', 1)))

            # Prepare data for insert
            dt = {
                'underlying_ltp': ltp_underlying,
                'strikeprice': strikeprice,
                'optiontype': optiontype,
                'totalsellavgprice': pos.get('totalsellavgprice', 0),
                'totalbuyavgprice': pos.get('totalbuyavgprice', 0),
                'ltp': pos.get('ltp', 0),
                'netqty': pos.get('netqty', 0),
                'totallots': totallots,
                'lotsize': pos.get('lotsize', 1),
                'unrealised': pos.get('unrealised', 0),
                'realised': pos.get('realised', 0),
                'symbolname': pos.get('symbolname', ''),
                'expirydate': pos.get('expirydate', ''),
                'delta': delta,
                'theta': theta,
                'implied_volatility': implied_volatility
            }
            positions.append(dt)

        # Execute batch insert
        if totallots > 0:
            exe = conn.execute(text(insert_query), positions)
            exe.close()  # Close the execution object

    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({"error": str(e)}), 500

    finally:
        # Dispose of the connection properly
        if conn:
            conn.dispose()

    return jsonify({"message": "All positions inserted successfully"}), 201


@app.route('/send-message', methods=['POST'])
def send_message():
    """API route to trigger Telegram message"""
    data = request.json  # Expecting JSON input
    message = data.get('message', 'Hello from Flask API!')
    chat_id = data.get('chat_id')

    # Send message to Telegram
    response = send_telegram_message(message,chat_id)

    if response.get('ok'):
        return jsonify({'status': 'success', 'message_id': response['result']['message_id']})
    else:
        return jsonify({'status': 'error', 'error': response}), 400

@app.route('/merge_tables', methods=['POST','GET'])
def merge_tables():
    """Simplified function to merge stockpositions and optionpositions into all_trades"""
    engine = get_connection()

    try:
        with Session(engine) as session:

            # Step 1: Fetch Stock Positions
            stock_query = text("""
                SELECT
                    symbolname AS stock_name,
                    netqty AS stock_quantity,
                    unrealised AS stock_pnl,
                    underlying_ltp AS stock_price
                FROM stockpositions
            """)
            stock_results = session.execute(stock_query).fetchall()

            # Step 2: Fetch Option Positions
            option_query = text("""
                SELECT
                    symbolname,
                    SUM(CASE WHEN optiontype = 'CE' THEN delta * totallots ELSE 0 END) AS short_delta,
                    SUM(CASE WHEN optiontype = 'PE' THEN delta * totallots ELSE 0 END) AS put_delta,
                    SUM(CASE WHEN optiontype = 'PE' THEN totallots ELSE 0 END) AS put_quantity,
                    SUM(CASE WHEN optiontype = 'CE' THEN totallots ELSE 0 END) AS call_quantity,
                    SUM(COALESCE(unrealised, 0) + COALESCE(realised, 0)) AS current_pnl
                FROM optionpositions
                GROUP BY symbolname
            """)
            option_results = session.execute(option_query).fetchall()

            # Step 3: Get Lot Sizes from External API
            symbols = {row[0] for row in stock_results + option_results}
            lot_sizes = get_lot_sizes(symbols)

            # Step 4: Merge Data in Python
            merged_data = {}

            # Add stock data first
            for row in stock_results:
                stock_name, stock_quantity, stock_pnl, stock_price = row

                # Get lot size from API or fallback to 1
                lot_size = lot_sizes.get(stock_name, Decimal(1))

                # Calculate stock delta: stock_quantity / lot_size
                stock_delta = (Decimal(stock_quantity) / lot_size) if lot_size > 0 else Decimal(0)

                merged_data[stock_name] = {
                    "stock_name": stock_name,
                    "stock_quantity": stock_quantity or 0,
                    "stock_pnl": stock_pnl or Decimal(0),
                    "stock_price": stock_price or Decimal(0),
                    "short_delta": Decimal(0),
                    "long_delta": stock_delta,   # Start with stock delta
                    "put_quantity": 0,
                    "call_quantity": 0,
                    "current_pnl": stock_pnl or Decimal(0),
                    "total_potential_profit": None,
                    "alerts": None
                }

            # Add option data, merging into existing stock records
            for row in option_results:
                symbolname, short_delta, put_delta, put_quantity, call_quantity, current_pnl = row

                # Use Decimal(0) for safe arithmetic
                short_delta = short_delta or Decimal(0)
                put_delta = put_delta or Decimal(0)
                put_quantity = put_quantity or 0
                call_quantity = call_quantity or 0
                current_pnl = current_pnl or Decimal(0)

                # Add or merge with stock data
                if symbolname in merged_data:
                    # Merge with existing stock entry
                    merged_data[symbolname]["short_delta"] = short_delta
                    merged_data[symbolname]["long_delta"] += put_delta  # Add put delta to stock delta
                    merged_data[symbolname]["put_quantity"] = put_quantity
                    merged_data[symbolname]["call_quantity"] = call_quantity
                    merged_data[symbolname]["current_pnl"] += current_pnl
                else:
                    # Option-only entry
                    merged_data[symbolname] = {
                        "stock_name": symbolname,
                        "stock_quantity": 0,
                        "stock_pnl": Decimal(0),
                        "stock_price": Decimal(0),
                        "short_delta": short_delta,
                        "long_delta": put_delta,  # Only put delta, no stock delta
                        "put_quantity": put_quantity,
                        "call_quantity": call_quantity,
                        "current_pnl": current_pnl,
                        "total_potential_profit": None,
                        "alerts": None
                    }

            # Step 5: Clear `all_trades` table before inserting new data
            session.execute(text("DELETE FROM all_trades"))

            # Step 6: Insert Merged Data into `all_trades`
            insert_query = text("""
                INSERT INTO all_trades (
                    stock_name, stock_price, stock_quantity,
                    put_quantity, call_quantity,
                    long_delta, short_delta,
                    current_pnl, total_potential_profit, alerts
                )
                VALUES (
                    :stock_name, :stock_price, :stock_quantity,
                    :put_quantity, :call_quantity,
                    :long_delta, :short_delta,
                    :current_pnl, :total_potential_profit, :alerts
                )
                ON DUPLICATE KEY UPDATE
                    stock_price = VALUES(stock_price),
                    stock_quantity = VALUES(stock_quantity),
                    put_quantity = VALUES(put_quantity),
                    call_quantity = VALUES(call_quantity),
                    long_delta = VALUES(long_delta),
                    short_delta = VALUES(short_delta),
                    current_pnl = VALUES(current_pnl)
            """)

            # Insert all merged rows
            for data in merged_data.values():
                session.execute(insert_query, data)

            session.commit()
            return("Data merged successfully into all_trades!")

    except Exception as e:
        return(f"Error during merge: {e}")


@app.route('/get_stock_positions', methods=['POST', 'GET'])
def get_stock_positions():
    """Fetches stock positions and returns in required JSON format"""

    # Handle POST request
    if request.method == 'POST':
        content_type = request.headers.get('Content-Type')

        if content_type != 'application/json':
            return jsonify({"error": "Invalid content type"}), 400

        data = request.get_json()
        stockname = data.get('stockname')

    # Handle GET request
    elif request.method == 'GET':
        stockname = request.args.get('stockname')

    if not stockname:
        return jsonify({"error": "Missing stockname parameter"}), 400

    try:
        # Connect to MySQL
        engine = get_connection()
        with Session(engine) as session:

            # Query stockpositions table
            query = text("""
                SELECT
                    netqty AS quantity,
                    underlying_ltp AS lastTradedPrice,
                    unrealised AS unrealizedPnL,
                    buyprice AS buyPrice
                FROM stockpositions
                WHERE symbolname = :stockname
            """)

            result = session.execute(query, {'stockname': stockname}).fetchall()

            # Check if no results found
            if not result:
                return jsonify([])

            # Format the response
            response = []
            for row in result:
                quantity = row.quantity or 0
                buy_price = row.buyPrice or 0
                unrealized_pnl = row.unrealizedPnL or 0
                last_traded_price = row.lastTradedPrice or 0

                # Calculate lots (quantity / lot size)
                lot_size = 1  # Default lot size
                try:
                    # Fetch lot size from external API
                    api_url = "https://thetagains.pythonanywhere.com/get_lotsize"
                    api_response = requests.post(api_url, json={"stockname": stockname})

                    if api_response.status_code == 200:
                        lot_size_str = api_response.text.strip()
                        if lot_size_str.isdigit():
                            lot_size = max(1, int(lot_size_str))  # Ensure lot size >= 1
                except Exception as e:
                    print(f" Failed to fetch lot size: {e}")

                # Calculate number of lots
                lots = round((quantity / lot_size),2)

                # Add row to response
                response.append({
                    "quantity": quantity,
                    "lots": lots,
                    "buyPrice": float(buy_price),
                    "unrealizedPnL": float(unrealized_pnl),
                    "lastTradedPrice": float(last_traded_price)
                })

            return jsonify(response)

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500

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

    # Handle GET request
    elif request.method == 'GET':
        stockname = request.args.get('stockname')

    # Validate stockname
    if not stockname:
        return jsonify({"error": "Missing stockname parameter"}), 400

    try:
        #  Connect to MySQL
        engine = get_connection()
        with Session(engine) as session:

            #  Query optionpositions table
            query = text("""
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
                    unrealised AS unrealizedPnL,
                    potentialpnl AS potentialPnL
                FROM optionpositions
                WHERE symbolname = :stockname
            """)

            result = session.execute(query, {'stockname': stockname}).fetchall()

            #  Return empty list if no result
            if not result:
                return jsonify([])

            #  Format the response
            response = []
            for row in result:
                response.append({
                    "expiry": row.expirydate if row.expirydate else "",
                    "type": row.type,
                    "lots": int(row.lots) if row.lots else 0,
                    "strikePrice": float(row.strikeprice) if row.strikeprice else 0.0,
                    "buyPrice": float(row.totalbuyavgprice) if row.totalbuyavgprice else "",
                    "sellPrice": float(row.totalsellavgprice) if row.totalsellavgprice else "",
                    "lastTradedPrice": float(row.ltp) if row.ltp else 0.0,
                    "delta": float(row.delta) if row.delta else 0.0,
                    "theta": float(row.theta) if row.theta else 0.0,
                    "unrealizedPnL": float(row.unrealizedPnL) if row.unrealizedPnL else 0.0,
                    "potentialPnL": float(row.potentialPnL) if row.potentialPnL else 0.0
                })

            return jsonify(response)

    except Exception as e:
        print(f" Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500

@app.route('/get_all_positions', methods=['GET'])
def get_all_positions():
    """Fetches all unique stock names from the all_trades table."""
    try:
        engine = get_connection()
        with Session(engine) as session:
            query = text("SELECT DISTINCT stock_name FROM all_trades order by stock_name")
            result = session.execute(query).fetchall()

            # Convert to list of stock names
            stock_names = [row.stock_name for row in result if row.stock_name]

            return jsonify(stock_names)
    except Exception as e:
        return jsonify({"error": f"Internal Server Error: {str(e)}"}), 500

@app.route('/get_all_trades', methods=['POST', 'GET'])
def get_all_trades():
    try:
        data = request.get_json() if request.method == 'POST' else None
        stock_name = data.get('stock_name') if data else request.args.get('stock_name')

        engine = get_connection()
        with engine.connect() as connection:
            if stock_name:
                query = text("SELECT * FROM all_trades WHERE stock_name = :stock_name")
                result = connection.execute(query, {'stock_name': stock_name})
            else:
                query = text("SELECT * FROM all_trades")
                result = connection.execute(query)

            columns = result.keys()
            trades = [dict(zip(columns, row)) for row in result]

        return jsonify(trades)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/get-stock-option-position', methods=['GET'])
def get_stock_option_position():
    input_symbolname = request.args.get('symbolname')
    if not input_symbolname:
        return jsonify({'error': 'Missing required parameter: symbolname'}), 400

    engine = get_connection()
    query = text("""
        SELECT
            stockpositions.symbolname,
            stockpositions.underlying_ltp,
            stockpositions.netqty,
            buyprice,
            strikeprice,
            totalsellavgprice,
            totallots
        FROM stockpositions, optionpositions
        WHERE stockpositions.symbolname = optionpositions.symbolname
          AND optiontype = 'CE'
          AND stockpositions.symbolname = :symbolname
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {'symbolname': input_symbolname})
        rows = [dict(row) for row in result]

    return jsonify(rows)

@app.route('/add_alert', methods=['POST'])
def add_alert():
    data = request.get_json()
    stockname = data.get('stockname')
    conditions = data.get('conditions')
    value = data.get('value')

    if not all([stockname, conditions, value]):
        return jsonify({'error': 'Missing one or more required fields'}), 400

    try:
        engine = get_connection()
        with engine.begin() as conn:  # auto-commits at the end of block
            # Check for duplicate
            check_query = text("""
                SELECT 1 FROM alerts
                WHERE stockname = :stockname AND conditions = :conditions AND value = :value
                LIMIT 1
            """)
            result = conn.execute(check_query, {
                'stockname': stockname,
                'conditions': conditions,
                'value': float(value)
            }).first()

            if result:
                return jsonify({'message': 'Alert already exists'}), 200

            # Insert new alert
            insert_query = text("""
                INSERT INTO alerts (stockname, conditions, value)
                VALUES (:stockname, :conditions, :value)
            """)
            conn.execute(insert_query, {
                'stockname': stockname,
                'conditions': conditions,
                'value': float(value)
            })

        return jsonify({'message': 'Alert added successfully'}), 200

    except Exception as e:
        print("Database Error:", e)
        return jsonify({'error': 'Database error'}), 500

@app.route('/get_alerts', methods=['GET'])
def get_alerts():
    stockname = request.args.get('stockname')
    is_option = request.args.get('is_option', 'false').lower() == 'true'

    if not stockname:
        return jsonify({'error': 'Stockname required'}), 400

    try:
        engine = get_connection()
        with engine.connect() as conn:
            if is_option:
                query = text("""
                    SELECT id, stockname, conditions, value, created_at
                    FROM alerts
                    WHERE stockname = :stockname AND conditions NOT IN ('Stock Price Above', 'Stock Price Below')
                    ORDER BY created_at DESC
                """)
            else:
                query = text("""
                    SELECT id, stockname, conditions, value, created_at
                    FROM alerts
                    WHERE stockname = :stockname AND conditions IN ('Stock Price Above', 'Stock Price Below')
                    ORDER BY created_at DESC
                """)
            result = conn.execute(query, {'stockname': stockname})
            alerts = [dict(row._mapping) for row in result]
            return jsonify({'alerts': alerts}), 200
    except Exception as e:
        print("Error fetching alerts:", e)
        return jsonify({'error': 'Database error'}), 500



@app.route('/delete_alert', methods=['POST'])
def delete_alert():
    data = request.get_json()
    alert_id = data.get('id')

    if not alert_id:
        return jsonify({'error': 'Missing alert ID'}), 400

    try:
        engine = get_connection()
        with engine.begin() as conn:
            delete_query = text("DELETE FROM alerts WHERE id = :id")
            conn.execute(delete_query, {'id': alert_id})
        return jsonify({'message': 'Alert deleted'}), 200
    except Exception as e:
        print("Error deleting alert:", e)
        return jsonify({'error': 'Database error'}), 500


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        data = request.get_json()
    elif request.method == 'GET':
        data = request.args

    username = data.get("username")
    broker = data.get("broker")

    try:
        module = importlib.import_module(f"broker_integration.{broker.lower()}")
        return jsonify(module.login(data))
    except ModuleNotFoundError:
        return jsonify({"error": f"Broker {broker} not supported"}), 400

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)
