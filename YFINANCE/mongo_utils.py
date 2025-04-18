from datetime import date, datetime
from config import MONGO_URI, DB_NAME, COLLECTION_NAME
from pymongo import MongoClient
import traceback

# Inisialisasi koneksi MongoDB saat modul dijalankan
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    print(f"[INFO] MongoDB connection established successfully to {DB_NAME}")
except Exception as e:
    print(f"[ERROR] Failed to connect to MongoDB: {e}")
    traceback.print_exc()

def convert_dates(data):
    """Recursively convert datetime.date to datetime.datetime in a dict or list."""
    if isinstance(data, list):
        return [convert_dates(item) for item in data]
    elif isinstance(data, dict):
        return {k: convert_dates(v) for k, v in data.items()}
    elif isinstance(data, date) and not isinstance(data, datetime):
        return datetime.combine(data, datetime.min.time())
    else:
        return data

def save_to_mongo(ticker, interval, spark_df):
    """Menyimpan hasil agregasi dari Spark DataFrame ke MongoDB."""
    try:
        if spark_df is None or spark_df.rdd.isEmpty():
            print(f"[WARNING] Spark DataFrame kosong untuk {ticker} ({interval})")
            return

        data = spark_df.toPandas().to_dict(orient="records")
        data = convert_dates(data)

        if not data:
            print(f"[WARNING] Tidak ada data yang bisa disimpan untuk {ticker} ({interval})")
            return

        ticker = ticker.upper()
        interval = interval.lower()

        print(f"[INFO] Menyimpan {len(data)} baris data untuk {ticker} ({interval})...")

        document = {
            "ticker": ticker,
            "interval": interval,
            "data": data,
            "saved_at": datetime.utcnow()
        }

        # Overwrite jika sudah ada
        collection.delete_many({"ticker": ticker, "interval": interval})
        result = collection.insert_one(document)

        print(f"[SUCCESS] {ticker} ({interval}) berhasil disimpan. ID Dokumen: {result.inserted_id}")
        return True

    except Exception as e:
        print(f"[ERROR] Gagal menyimpan data ke MongoDB untuk {ticker} ({interval}): {e}")
        traceback.print_exc()
        return False

def get_data_from_mongo(ticker, interval):
    """Mengambil data dari MongoDB berdasarkan ticker dan interval."""
    try:
        ticker = ticker.upper()
        interval = interval.lower()
        print(f"[INFO] Mencari data untuk {ticker} dengan interval {interval}")
        doc = collection.find_one({"ticker": ticker, "interval": interval})
        if doc:
            print(f"[INFO] Data ditemukan, jumlah data: {len(doc['data'])}")
            return doc["data"]
        else:
            print(f"[WARNING] Data tidak ditemukan untuk {ticker} ({interval})")
            return []
    except Exception as e:
        print(f"[ERROR] Gagal mengambil data dari MongoDB: {e}")
        traceback.print_exc()
        return []

def get_available_tickers():
    tickers = collection.distinct("ticker")
    return tickers
