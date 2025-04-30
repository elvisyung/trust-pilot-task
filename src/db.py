import sqlite3

# Create a connection to the SQLite database
def get_connection(db_path='reviews.db'):
    conn = sqlite3.connect(db_path)
    return conn

# Create the reviews table if it doesn't already exist
def create_reviews_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reviewer_name TEXT,
            review_title TEXT,
            review_rating INTEGER,
            review_content TEXT,
            email_address TEXT,
            country TEXT,
            review_date TEXT
        )
    ''')
    conn.commit()