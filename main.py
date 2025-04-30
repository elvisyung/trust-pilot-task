from src.ingest import ingest_csv_to_db

if __name__ == "__main__":
    # Ingesting the CSV file into the SQLite database
    ingest_csv_to_db("data/dataops_tp_reviews.csv")
