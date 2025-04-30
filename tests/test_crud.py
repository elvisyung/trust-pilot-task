import sqlite3
import pytest
from src.crud import add_review, update_review_content_by_email
from src.db import create_reviews_table

def test_add_review(tmp_path):
    db_path = tmp_path / "test_reviews.db"
    conn = sqlite3.connect(db_path)
    create_reviews_table(conn)
    conn.close()

    # Add a review
    review = {
        "reviewer_name": "Alice Example",
        "review_title": "Amazing Product",
        "review_rating": 5,
        "review_content": "Loved it!",
        "email_address": "alice@example.com",
        "country": "USA",
        "review_date": "2025-04-01"
    }
    add_review(review, db_path)

    # Verify the review was added
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM reviews")
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 1  # Ensure one row was added
    assert rows[0][1] == "Alice Example"  # Verify the reviewer's name

def test_add_review_missing_fields(tmp_path):
    db_path = tmp_path / "test_reviews.db"
    conn = sqlite3.connect(db_path)
    create_reviews_table(conn)
    conn.close()

    # Add a review with missing fields
    review = {
        "reviewer_name": "Bob Example",
        "review_title": "Good Product",
        "review_rating": 4,
    }

    # Check for ValueError
    with pytest.raises(ValueError, match="Missing required fields: review_content, email_address, country, review_date"):
        add_review(review, db_path)

def test_update_non_existent_email(tmp_path):
    db_path = tmp_path / "test_reviews.db"
    conn = sqlite3.connect(db_path)
    create_reviews_table(conn)
    conn.close()

    # Attempt to update a non-existent email
    result = update_review_content_by_email("nonexistent@example.com", "New content", db_path)
    assert result is None 

def test_add_duplicate_email(tmp_path):
    db_path = tmp_path / "test_reviews.db"
    conn = sqlite3.connect(db_path)
    create_reviews_table(conn)
    conn.close()

    # Add the first review
    review = {
        "reviewer_name": "Alice Example",
        "review_title": "Amazing Product",
        "review_rating": 5,
        "review_content": "Loved it!",
        "email_address": "alice@example.com",
        "country": "USA",
        "review_date": "2025-04-01"
    }
    add_review(review, db_path)

    # Attempt to add a second review with the same email
    with pytest.raises(ValueError, match="A review with the email alice@example.com already exists."):
        add_review(review, db_path)

    # Verify that only one review exists in the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM reviews WHERE email_address = 'alice@example.com'")
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 1  # Ensure only one review exists for the email
    assert rows[0][1] == "Alice Example"  # Verify the reviewer's name
    assert rows[0][4] == "Loved it!"  # Verify the review content

def test_invalid_email_update(tmp_path):
    db_path = tmp_path / "test_reviews.db"
    conn = sqlite3.connect(db_path)
    create_reviews_table(conn)
    conn.close()

    # Attempt to update with an invalid email
    result = update_review_content_by_email("invalid-email", "New content", db_path)
    assert result is None  

def test_update_review_content_by_email(tmp_path):
    db_path = tmp_path / "test_reviews.db"
    conn = sqlite3.connect(db_path)
    create_reviews_table(conn)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO reviews (reviewer_name, review_title, review_rating, review_content, email_address, country, review_date)
        VALUES ('Alice Example', 'Amazing Product', 5, 'Loved it!', 'alice@example.com', 'USA', '2025-04-01')
    """)
    conn.commit()
    conn.close()

    # Update the review content by email
    update_review_content_by_email("alice@example.com", "Updated review content.", db_path)

    # Verify the review content was updated
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT review_content FROM reviews WHERE email_address = 'alice@example.com'")
    updated_content = cursor.fetchone()[0]
    conn.close()

    assert updated_content == "Updated review content."