# Tugas2-BIGDATA

LAPORAN INGESTION DATA YFINANCE
---
Membangun pipeline pemrosesan data saham berbasis Apache Spark, MongoDB, dan FastAPI. Data historis saham diambil dari Yahoo Finance, kemudian diproses untuk menghasilkan agregasi berdasarkan interval waktu harian, bulanan, dan tahunan. Data hasil agregasi disimpan ke MongoDB dan disediakan dalam bentuk REST API serta visualisasi web interaktif.

### Teknologi yang Digunakan

- Apache Spark 3.3.2 (mode local[*])
- Python 3.9 - 3.10
- pandas 1.5.3
- numpy 1.21.6
- MongoDB 7.0.5
- FastAPI + Uvicorn
- Chart.js (untuk visualisasi frontend)
- yfinance 0.2.36

### Struktur Komponen

1. **Spark Processing (spark_process.py)**  
   Mengambil data saham 5 tahun terakhir menggunakan yfinance, mengubah ke Spark DataFrame, dan mengagregasi data ke dalam tiga interval waktu: harian, bulanan, dan tahunan.

2. **Database Handling (mongo_utils.py)**  
   Menangani koneksi dan penyimpanan hasil agregasi ke MongoDB dalam koleksi stock_aggregates.

3. **API dan Visualisasi (main.py)**  
   Menyediakan REST API menggunakan FastAPI dengan beberapa endpoint utama:
   - /stock/{ticker}/{interval}
   - /stock/all/{interval}
   - /stock/refresh/{ticker}
   - /visualize/all

   Visualisasi data ditampilkan melalui Chart.js dalam tampilan web.

### Dataset dan Hasil

- Menggunakan 10 ticker saham dari pasar Indonesia (contoh: BBCA.JK, BBRI.JK, TLKM.JK, dan lainnya)
- Tiga jenis agregasi untuk setiap ticker: daily, monthly, yearly
- Total dokumen yang dihasilkan: 30 (10 ticker × 3 interval)
- Setiap dokumen berisi data rata-rata untuk Open, Close, High, Low, dan Volume
- Proses penuh (pengambilan, transformasi, dan penyimpanan) memerlukan waktu sekitar 6 menit untuk seluruh data

### Catatan Teknis
Dalam proses implementasi, sempat terjadi kendala kompatibilitas antara versi Apache Spark dan Python. Versi Python semula tidak sesuai sehingga dilakukan downgrade ke versi 3.9 agar Spark 3.3.2 dapat berjalan stabil bersama library seperti pandas dan numpy. Penyesuaian ini memastikan eksekusi pipeline berjalan tanpa kendala.

### Hasil Visualisasi
![WhatsApp Image 2025-04-18 at 16 02 45_ca6b11ce](https://github.com/user-attachments/assets/17eb7ac0-08c9-4576-9820-33a9f0377496)

![WhatsApp Image 2025-04-18 at 16 05 06_f4ab04ae](https://github.com/user-attachments/assets/7115f55a-adbf-495e-8554-746deb32bad6)

---

LAPORAN INGESTION DATA IDX
---
## Transformasi Data XBRL
Transformasi data laporan keuangan XBRL dilakukan menggunakan PySpark untuk membaca data dari MongoDB, membersihkannya, dan menambahkan fitur-fitur analitis guna mendukung eksplorasi dan visualisasi data.

### Proses Transformasi

1. **Konfigurasi MongoDB**  
   Mengatur URI, nama database, dan koleksi MongoDB sebagai sumber data laporan keuangan XBRL yang akan diproses.

2. **Inisialisasi Spark Session**  
   Spark Session diinisialisasi dengan konfigurasi MongoDB Spark Connector dan memori driver sebesar 4GB untuk mendukung pemrosesan data dalam jumlah besar.

3. **Pipeline MongoDB**  
   Menggunakan pipeline agregasi berbasis `$match` dan `$project` untuk mengambil hanya kolom-kolom keuangan utama, seperti pendapatan, laba, arus kas, dan rasio.

4. **Konversi Tipe Data**  
   Seluruh kolom numerik dikonversi ke tipe `double` untuk memastikan akurasi perhitungan numerik.

5. **Penanganan Nilai Null**  
   Nilai `null` pada kolom numerik diganti menjadi nol untuk menghindari error dalam perhitungan lebih lanjut.

6. **Penambahan Kolom Analitis Baru**  
   Ditambahkan beberapa kolom hasil kalkulasi untuk mendukung analisis keuangan, antara lain:
   - `TotalNetCashFlow`
   - `ProfitMargin`
   - `OperatingCashFlowRatio`
   - `FinancingDependenceRatio`
   - `InvestingToOperatingRatio`
   - `OperatingExpenseRatio`
   - `NetCommission`
   - `CashToAssetRatio`
   - `IsProfitable`

### Visualisasi Analisis
Vsualisasi data menggunakan matplotlib dan seaborn untuk membantu interpretasi hasil transformasi:

- **Heatmap Korelasi**: Menampilkan kekuatan hubungan antar variabel numerik berdasarkan koefisien Pearson.
- **Histogram Profit Margin**: Distribusi margin keuntungan dengan skala logaritmik untuk menangani outlier.
- **Scatter Plot Profit Margin vs Operating Cash Flow Ratio**: Menunjukkan korelasi antara profitabilitas dan kekuatan arus kas operasional.
- **Histogram Cash to Asset Ratio**: Menampilkan distribusi likuiditas setelah pembersihan nilai ekstrim.
- **Pairplot Variabel Keuangan**: Memetakan hubungan antar variabel keuangan dengan pengelompokan berdasarkan profitabilitas.
  ![image](https://github.com/user-attachments/assets/f7bf0e18-c3f5-4b83-b04e-846a8dd85271)


---

LAPORAN INGESTION DATA BERITA IQPLUS
---
Dalam proyek ini, kami membuat program untuk menyederhanakan isi berita saham ke dalam format 5W+1H (What, Who, When, Where, Why, How) dengan bantuan Python, MongoDB, dan Apache Spark. Tujuannya agar informasi penting dari berita lebih mudah dipahami dan diolah. Langkah-langkah Utama:

1. Mengambil Data dari MongoDB
- Kami mengambil berita dari dua koleksi yaitu stock_news di database stock_news_db dan market_news di database market_news_db
  
2. Ekstraksi Informasi 5W+1H
- Kami menggunakan fungsi summarize_5w1h() untuk mengekstrak:
- Who: Nama perusahaan, kode saham, atau narasumber seperti direktur, CEO, dll.
- What: Informasi kinerja perusahaan (laba, pendapatan, penjualan, dll).
- When: Waktu kejadian (bulan, tahun, kuartal).
- Where: Lokasi atau kantor perusahaan.
- Why: Alasan di balik suatu peristiwa (penyebab kenaikan/penurunan).
- How: Strategi atau cara perusahaan mencapai sesuatu.
- Ekstraksi dilakukan dengan pola kata kunci sederhana menggunakan library re (Regular Expression).

3. Pemrosesan dengan Apache Spark
- Berita dimuat ke dalam Spark DataFrame, lalu fungsi summarize_5w1h() dijalankan sebagai User Defined Function (UDF) agar bisa diproses secara paralel dan cepat dalam skala besar.
  
4. Menyimpan Hasil ke MongoDB
- Hasil ekstraksi disimpan ke dua koleksi baru yaitu transformed_stock_news dan transformed_market_news
- Setiap data hasil ekstraksi mencakup elemen 5W+1H, judul asli, dan ID berita
- Program akan menampilkan berapa banyak berita yang berhasil diproses saat dijalankan.
