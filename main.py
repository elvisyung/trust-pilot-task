import sys
from src.ingest import ingest_csv_to_db
from src.crud import add_review, update_review_content_by_email

def run_pipeline(csv_path: str):
    try:
        # Step 1: Ingest CSV into DB
        ingest_csv_to_db(csv_path)
    except Exception as e:
        print(f"[ERROR] CSV ingestion failed: {e}")
        return False

    try:
        # Step 2: Add example review
        add_review({
            "reviewer_name": "Alice Example",
            "review_title": "Amazing Product",
            "review_rating": 5,
            "review_content": "Loved it!",
            "email_address": "alice@example.com",
            "country": "USA",
            "review_date": "2024-04-01"
        })
    except Exception as e:
        print(f"[ERROR] Failed to add review: {e}")
        return False

    try:
        # Step 3: Update review content
        update_review_content_by_email("johndoe@example.com", "Updated review content.")
    except Exception as e:
        print(f"[ERROR] Failed to update review: {e}")
        return False

    print("[INFO] Pipeline completed successfully.")
    return True

if __name__ == "__main__":
    success = run_pipeline("data/dataops_tp_reviews.csv")
    if not success:
        sys.exit(1)
