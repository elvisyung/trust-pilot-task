import sqlite3
from src.ingest import ingest_csv_to_db

def test_ingest_csv_to_db(tmp_path):
    # Create a temporary CSV file
    csv_path = tmp_path / "test_reviews.csv"
    csv_data = """
    reviewer_name,review_title,review_rating,review_content,email_address,country,review_date
    John Doe,Great Product,5,Excellent!,john.doe@example.com,USA,2025-04-01
    Jane Smith,Not Bad,4,Pretty good!,jane.smith@example.com,UK,2025-04-02
    Jane Smith,Not Bad,3,Nice!,john.doe@example,UK,2025-04-02
    """
    csv_path.write_text(csv_data)

    # Ingest the CSV into a temporary SQLite database
    db_path = tmp_path / "test_reviews.db"
    ingest_csv_to_db(csv_path, db_path)

    # Verify the data in the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM reviews")
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 2  # Ensure two rows were ingested
    assert rows[0][3] == "Excellent!"  
    assert rows[1][3] == "Pretty good!" 