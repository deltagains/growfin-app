import sqlite3

# Connect to (or create) the SQLite database file
conn = sqlite3.connect("trading.db")
c = conn.cursor()

# Create the user table
c.execute('''
    CREATE TABLE IF NOT EXISTS user (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        password TEXT NOT NULL,
        broker TEXT NOT NULL,
        api_key TEXT,
        totp_token TEXT
    )
''')

# Insert a sample user (modify as needed)
c.execute('''
    INSERT INTO user (username, password, broker, api_key, totp_token)
    VALUES (?, ?, ?, ?, ?)
''', (
    'ROTC1004',         # username
    '0698',             # password
    'angelone',         # broker module name (matches angelone.py)
    'Nsiz7EkP',         # api_key
    'WVBD6LBXLHJQFFEV5LWK3X52AY'  # TOTP token
))

conn.commit()
conn.close()

print("users.db created and sample user added.")
