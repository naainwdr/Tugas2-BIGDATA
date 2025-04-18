from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType
import re
import json

# ======== FUNGSI UDF UNTUK 5W1H ========
def summarize_5w1h(berita_json):
    try:
        berita = json.loads(berita_json)
        konten = str(berita.get('konten', '') or '')
        judul = str(berita.get('judul', '') or '')
        tanggal = str(berita.get('tanggal_artikel', '') or '')

        # WHO: Identifikasi perusahaan dan narasumber dengan lebih akurat
        who_match = re.match(r"([A-Z]{2,5}):\s*(.*?)(?:\s+CATAT|,\s|PADA|RAIH|MENCATAT|Tbk|MERAIH|$)", judul, re.IGNORECASE)
        if who_match:
            kode_saham = who_match.group(1).upper()
            nama_perusahaan = who_match.group(2).strip().title()
            who = f"{nama_perusahaan} ({kode_saham})"
        else:
            # Coba cari nama perusahaan di dalam konten
            company_pattern = re.search(r"PT\s+([A-Za-z\s]+?)(?:Tbk\.?|,|\(|\.)", konten)
            who = company_pattern.group(0).strip() if company_pattern else "Perusahaan tidak terdeteksi"
        
        # Cari narasumber dengan pola yang lebih lengkap
        narasumber_patterns = [
            r"(?:menurut|kata|ujar|ungkap|jelas|tutur|sebut|papar|nyata|terang)\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+){1,3})",
            r"([A-Z][a-z]+(?:\s[A-Z][a-z]+){1,3})(?:,| selaku| sebagai)?\s+(?:Direktur|Presiden|CEO|CFO|Manajer|Ketua)"
        ]
        
        narasumber = None
        for pattern in narasumber_patterns:
            narasumber_match = re.search(pattern, konten)
            if narasumber_match:
                narasumber = narasumber_match.group(1).strip()
                who += f", Narasumber: {narasumber}"
                break
        
        # WHAT: Fokus pada informasi keuangan dengan pola yang lebih spesifik
        what_patterns = [
            r"(?:mencatat|membukukan|meraih|mengalami).*?laba.*?(?:Rp[\d.,]+\s*[a-zA-Z]*|[0-9,.]+\s*[a-zA-Z]*).*?(?:naik|turun|tumbuh|meningkat)?.*?\.",
            r"(?:pendapatan|penjualan).*?(?:Rp[\d.,]+\s*[a-zA-Z]*|[0-9,.]+\s*[a-zA-Z]*).*?(?:periode|kuartal|semester|tahun).*?\.",
            r"(?:kinerja|performa).*?(?:positif|negatif|baik|buruk|meningkat|menurun).*?\."
        ]
        
        what = judul
        for pattern in what_patterns:
            what_match = re.search(pattern, konten, re.IGNORECASE)
            if what_match:
                what = what_match.group().strip()
                break
        
        # WHEN: Temukan periode pelaporan finansial yang lebih spesifik
        when_patterns = [
            r"(?:pada|untuk|sepanjang|di)\s+(?:kuartal|semester|periode|tahun).*?(?:20\d{2}|Q[1-4])",
            r"(?:Januari|Februari|Maret|April|Mei|Juni|Juli|Agustus|September|Oktober|November|Desember).*?20\d{2}"
        ]
        
        when = tanggal
        for pattern in when_patterns:
            when_match = re.search(pattern, konten, re.IGNORECASE)
            if when_match:
                when += f", Periode: {when_match.group().strip()}"
                break
        
        # WHERE: Cari lokasi dengan pola yang lebih lengkap
        where_patterns = [
            r"\bdi\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)",
            r"(?:berlokasi|bertempat|bermarkas)\s+di\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)",
            r"(?:kantor|gedung|pusat)\s+(?:di|pada)\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)"
        ]
        
        where = "Lokasi tidak ditemukan"  # Default untuk berita saham Indonesia
        for pattern in where_patterns:
            where_match = re.search(pattern, konten)
            if where_match:
                located = where_match.group(1) if len(where_match.groups()) > 0 else where_match.group().strip()
                where = located
                break
        
        # WHY: Identifikasi alasan atau faktor dengan lebih detail
        why_patterns = [
            r"(?:karena|disebabkan|didorong|berkat|akibat|lantaran)\s+.*?(?:\.|\,)",
            r"(?:faktor|alasan).*?(?:adalah|ialah|yakni).*?(?:\.|\,)",
            r"(?:hal ini|peningkatan ini|penurunan ini).*?(?:karena|disebabkan|dipengaruhi).*?(?:\.|\,)"
        ]
        
        why = "Faktor tidak disebutkan secara spesifik"
        for pattern in why_patterns:
            why_match = re.search(pattern, konten, re.IGNORECASE)
            if why_match:
                why = why_match.group().strip()
                break
        
        # HOW: Metode atau strategi yang lebih spesifik
        how_patterns = [
            r"(?:melalui|dengan|berkat|lewat)\s+(?:strategi|pendekatan|metode|cara|upaya).*?(?:\.|\,)",
            r"(?:upaya|langkah|tindakan|inisiatif).*?(?:yang dilakukan|yang diterapkan).*?(?:\.|\,)",
            r"(?:efisiensi|optimalisasi|peningkatan|penguatan).*?(?:operasional|kinerja|strategi).*?(?:\.|\,)"
        ]
        
        how = "Strategi tidak dijelaskan secara detail"
        for pattern in how_patterns:
            how_match = re.search(pattern, konten, re.IGNORECASE)
            if how_match:
                how = how_match.group().strip()
                break
        
        return {
            "Apa": {"value": what},
            "Siapa": {"value": who},
            "Kapan": {"value": when},
            "Di mana": {"value": where},
            "Mengapa": {"value": why},
            "Bagaimana": {"value": how}
        }

    except Exception as e:
        return {
            "Apa": {"value": ""},
            "Siapa": {"value": ""},
            "Kapan": {"value": ""},
            "Di mana": {"value": ""},
            "Mengapa": {"value": ""},
            "Bagaimana": {"value": ""}
        }

# ======== SCHEMA HASIL UDF ========
summary_schema = StructType([
    StructField("Apa", StructType([
        StructField("value", StringType(), True),
    ])),
    StructField("Siapa", StructType([
        StructField("value", StringType(), True),
    ])),
    StructField("Kapan", StructType([
        StructField("value", StringType(), True),
    ])),
    StructField("Di mana", StructType([
        StructField("value", StringType(), True),
    ])),
    StructField("Mengapa", StructType([
        StructField("value", StringType(), True),
    ])),
    StructField("Bagaimana", StructType([
        StructField("value", StringType(), True),
    ]))
])

# ======== DAFTARKAN UDF KE SPARK ========
summary_udf = udf(summarize_5w1h, summary_schema)

# ======== SPARK SESSION ========
spark = SparkSession.builder \
    .appName("MongoDBToSpark5W1H") \
    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/stock_news_db.stock_news") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/stock_news_db.transformed_stock_news") \
    .getOrCreate()

# ======== LOAD DARI MONGODB ========
df_mongo = spark.read.format("mongodb").load()

# ======== UBAH KE JSON STRING (judul + konten) ========
df = df_mongo.withColumn("berita_json", to_json(struct("judul", "konten", "tanggal_artikel")))

# ======== APPLY UDF ========
df = df.withColumn("summary", summary_udf(col("berita_json")))

# ======== TAMPILKAN HASILNYA ========
df.show(truncate=False)

# ======== SIMPAN KE KOLEKSI MONGODB BARU ========
df.write.format("mongodb") \
    .mode("overwrite") \
    .option("database", "stock_news_db") \
    .option("collection", "transformed_stock_news") \
    .save()

# ======== TUTUP SPARK ========
spark.stop()