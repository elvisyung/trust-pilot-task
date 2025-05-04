from src.db import get_connection
import sqlite3
from typing import Dict

def add_review(review: Dict[str, str], db_path: str = 'data/reviews.db') -> None:
    """Add a new review to the database."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    try:
        # Check for missing fields
        required_fields = ["reviewer_name", "review_title", "review_rating", "review_content", 
                           "email_address", "country", "review_date"]
        missing_fields = [field for field in required_fields if field not in review or not review[field]]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")

        # Check if a review with the same email already exists
        cursor.execute('SELECT COUNT(*) FROM reviews WHERE email_address = ?', (review['email_address'],))
        if cursor.fetchone()[0] > 0:
            raise ValueError(f"A review with the email {review['email_address']} already exists.")

        # Insert the new review
        cursor.execute('''
            INSERT INTO reviews (reviewer_name, review_title, review_rating, review_content, 
                                 email_address, country, review_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            review['reviewer_name'], review['review_title'], review['review_rating'],
            review['review_content'], review['email_address'], review['country'],
            review['review_date']
        ))
        conn.commit()
        print(f"Review added successfully for email: {review['email_address']}")
    except sqlite3.DatabaseError as e:
        print(f"Database error: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise
    finally:
        conn.close()

def update_review_content_by_email(email: str, new_content: str, db_path: str = 'data/reviews.db') -> None:
    """Update the content of a review by the email address."""
    conn = None
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE reviews SET review_content = ? WHERE email_address = ?
        ''', (new_content, email))
        
        if cursor.rowcount == 0:
            print(f"No review found with email: {email}")
        else:
            print(f"Review with email {email} updated successfully.")
        
        conn.commit()
    except sqlite3.DatabaseError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()