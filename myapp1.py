from flask import Flask, request, jsonify, session
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text

app = Flask(__name__)
app.secret_key = '1234'  # Needed for session handling

# MySQL DB configuration
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:Rocho234@127.0.0.1/growfin'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        data = request.get_json()
    else:  # For GET
        data = request.args

    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return jsonify({'message': 'Username and password are required'}), 400

    # Raw SQL query
    query = text("SELECT * FROM users WHERE username = :username AND password = :password")
    result = db.session.execute(query, {'username': username, 'password': password}).fetchone()

    if result:
        user_data = dict(result._mapping)  # Convert to dictionary
        session['user'] = {
            'id': user_data['id'],
            'firstname': user_data['firstname'],
            'lastname': user_data['lastname'],
            'pythonanywhere_username': user_data['pythonanywhere_username']
        }
        return jsonify({'message': 'Login successful', 'user': session['user']})
    else:
        return jsonify({'message': 'Invalid username or password'}), 401


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000, debug=True)
