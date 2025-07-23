import datetime
import mibian
from datetime import datetime, timedelta, date
from decimal import Decimal

def DaysToExpiry(expiry):
    mons = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}

    year = int(expiry[5:9])
    mon = mons[expiry[2:5]]
    day = int(expiry[0:2])
    today = date.today()
    expiryday = date(year, mon, day)
    diff = expiryday - today
    return diff.days


def DaysToExpiry1(expiry):
    today = date.today()
    
    if len(expiry) == 7:  # e.g., 31JUL25
        expiry = expiry[:5] + '20' + expiry[5:]  # Convert to 31JUL2025

    try:
        expiry_date = datetime.strptime(expiry, "%d%b%Y").date()
        return (expiry_date - today).days
    except ValueError as e:
        return {"error": f"Invalid expiry format: {expiry}"}


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

def sanitize(val):
    if isinstance(val, Decimal):
        return float(val)
    return val

def fetch_table_data(cursor, table_name):
    cursor.execute(f"SELECT * FROM {table_name}")
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_last_expiry_date(reference_date=None):
    """
    Returns the last Thursday of the most recently completed expiry cycle
    (i.e., last Thursday of previous or current month, whichever is before today).
    """
    if reference_date is None:
        reference_date = datetime.now()

    year, month = reference_date.year, reference_date.month

    # Step 1: Get last Thursday of the current month
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    last_day_current_month = next_month - timedelta(days=1)

    # Find last Thursday of current month
    last_thursday_curr = last_day_current_month
    while last_thursday_curr.weekday() != 3:  # 3 = Thursday
        last_thursday_curr -= timedelta(days=1)

    # If expiry has already passed, return it
    if last_thursday_curr.date() < reference_date.date():
        return last_thursday_curr.strftime("%Y-%m-%d")

    # Else, go to previous month
    if month == 1:
        prev_month = datetime(year - 1, 12, 1)
    else:
        prev_month = datetime(year, month - 1, 1)
    last_day_prev_month = datetime(year, month, 1) - timedelta(days=1)

    last_thursday_prev = last_day_prev_month
    while last_thursday_prev.weekday() != 3:
        last_thursday_prev -= timedelta(days=1)

    return last_thursday_prev.strftime("%Y-%m-%d")
