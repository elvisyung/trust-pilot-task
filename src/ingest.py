import pandas as pd
import re
import sqlite3
from src.db import get_connection, create_reviews_table

def is_valid_email(email):
    """Check if the email address is valid."""
    email_regex = r"[^@]+@[^@]+\.[^@]+"
    return email if re.match(email_regex, str(email)) else None

def clean_review_rating(rating):
    """Convert rating to an integer if valid, otherwise return None."""
    if str(rating).isdigit() and 1 <= int(rating) <= 5:
        return int(rating)
    return None

def remove_emojis(text):
    """Remove emojis from the given text."""
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # Emoticons
        "\U0001F300-\U0001F5FF"  # Symbols & Pictographs
        "\U0001F680-\U0001F6FF"  # Transport & Map Symbols
        "\U0001F1E0-\U0001F1FF"  # Flags (iOS)
        "\U00002702-\U000027B0"  # Dingbats
        "\U000024C2-\U0001F251"  # Enclosed Characters
        "]+",
        flags=re.UNICODE,
    )
    return emoji_pattern.sub(r'', str(text)) if text else text

def clean_data(df):
    """Clean the input DataFrame."""
    # Normalize column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Clean email addresses
    df['email_address'] = df['email_address'].apply(is_valid_email)

    # Clean review ratings
    df['review_rating'] = df['review_rating'].apply(clean_review_rating)

    # Convert review dates to datetime
    df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce').dt.date

    # Remove emojis from review content
    df['review_content'] = df['review_content'].apply(remove_emojis)

    # Drop duplicate reviews based on email_address, keeping the first review
    df = df.drop_duplicates(subset='email_address', keep='first')

    # Drop rows with missing values
    return df.dropna()

def ingest_csv_to_db(csv_path, db_path='data/reviews.db'):
    """Ingest a CSV file into the SQLite database."""
    conn = None
    try:
        # Read and clean the data
        df = pd.read_csv(csv_path)
        df = clean_data(df)

        # Save the data to the database
        conn = get_connection(db_path)
        create_reviews_table(conn)
        df.to_sql('reviews', conn, if_exists='replace', index=False)
        print(f"Data successfully ingested into {db_path}")
    except FileNotFoundError:
        print(f"Error: The file {csv_path} was not found.")
    except pd.errors.EmptyDataError:
        print(f"Error: The file {csv_path} is empty or invalid.")
    except sqlite3.DatabaseError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")