from src.ingest import ingest_csv_to_db
from src.crud import add_review, update_review_content_by_email

if __name__ == "__main__":
    # Ingesting the CSV file into the SQLite database
    ingest_csv_to_db("data/dataops_tp_reviews.csv")

    # Add example review
    add_review({
        "reviewer_name": "Alice Example",
        "review_title": "Amazing Product",
        "review_rating": 5,
        "review_content": "Loved it!",
        "email_address": "alice@example.com",
        "country": "USA",
        "review_date": "2024-04-01"
    })

    # Update review content
    update_review_content_by_email("johndoe@example.com", "Updated review content.")