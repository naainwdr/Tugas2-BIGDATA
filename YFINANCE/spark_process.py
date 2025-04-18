import yfinance as yf
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import trunc, avg, col
from mongo_utils import save_to_mongo
import traceback

def run_aggregation_for_multiple_tickers(ticker_list):
    print("[INFO] Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("StockAggregator") \
        .master("local[*]") \
        .getOrCreate()

    for ticker_symbol in ticker_list:
        try:
            print(f"\n[INFO] Mengambil data dari yfinance untuk: {ticker_symbol}")
            ticker = yf.Ticker(ticker_symbol)
            df = ticker.history(period="5y").reset_index()

            if df.empty:
                print(f"[WARNING] Data kosong untuk {ticker_symbol}, lewati.")
                continue

            print(f"[INFO] Data berhasil diambil: {len(df)} baris")

            # Konversi ke Spark DataFrame
            sdf = spark.createDataFrame(df)

            # Daily Aggregation
            daily_df = sdf.withColumn("Date", col("Date").cast("date")) \
                .groupBy("Date") \
                .agg(
                    avg("Open").alias("Avg_Open"),
                    avg("Close").alias("Avg_Close"),
                    avg("High").alias("Avg_High"),
                    avg("Low").alias("Avg_Low"),
                    avg("Volume").alias("Avg_Volume")
                )

            # Monthly Aggregation
            monthly_df = sdf.withColumn("Month", trunc("Date", "MM")) \
                .groupBy("Month") \
                .agg(
                    avg("Open").alias("Avg_Open"),
                    avg("Close").alias("Avg_Close"),
                    avg("High").alias("Avg_High"),
                    avg("Low").alias("Avg_Low"),
                    avg("Volume").alias("Avg_Volume")
                )

            # Yearly Aggregation
            yearly_df = sdf.withColumn("Year", trunc("Date", "YYYY")) \
                .groupBy("Year") \
                .agg(
                    avg("Open").alias("Avg_Open"),
                    avg("Close").alias("Avg_Close"),
                    avg("High").alias("Avg_High"),
                    avg("Low").alias("Avg_Low"),
                    avg("Volume").alias("Avg_Volume")
                )

            # Simpan ke MongoDB
            print("[INFO] Menyimpan agregasi ke MongoDB...")
            save_to_mongo(ticker_symbol, "daily", daily_df)
            save_to_mongo(ticker_symbol, "monthly", monthly_df)
            save_to_mongo(ticker_symbol, "yearly", yearly_df)

            print(f"[SUCCESS] Semua data {ticker_symbol} berhasil diproses.\n")

        except Exception as e:
            print(f"[ERROR] Gagal memproses {ticker_symbol}: {e}")
            traceback.print_exc()

    print("[INFO] Menutup Spark session...")
    spark.stop()
    print("[INFO] Proses agregasi multi-ticker selesai!")

if __name__ == "__main__":
    ticker_list = ["BBCA.JK", "BBRI.JK", "TLKM.JK", "UNVR.JK", "ASII.JK", 
                   "BMRI.JK", "ANTM.JK", "PGAS.JK", "SMGR.JK", "INDF.JK"]
    run_aggregation_for_multiple_tickers(ticker_list)
