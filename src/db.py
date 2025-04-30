import sqlite3


def get_connection(db_path='reviews.db'):
    """Create a connection to the SQLite database."""
    conn = sqlite3.connect(db_path)
    return conn

def create_reviews_table(conn):
    """Create the reviews table with an auto-incrementing id column."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reviewer_name TEXT,
            review_title TEXT,
            review_rating INTEGER,
            review_content TEXT,
            email_address TEXT,
            country TEXT,
            review_date DATE
        )
    """)
    conn.commit()