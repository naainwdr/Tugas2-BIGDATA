# Tugas2-BIGDATA

LAPORAN INGESTION DATA YFINANCE
---
LAPORAN INGESTION DATA IDX
---
LAPORAN INGESTION DATA BERITA IQPLUS
---
Dalam proyek ini, kami membuat program untuk menyederhanakan isi berita saham ke dalam format 5W+1H (What, Who, When, Where, Why, How) dengan bantuan Python, MongoDB, dan Apache Spark. Tujuannya agar informasi penting dari berita lebih mudah dipahami dan diolah. Langkah-langkah Utama:

---
1. Mengambil Data dari MongoDB
- Kami mengambil berita dari dua koleksi yaitu stock_news di database stock_news_db dan market_news di database market_news_db
---
2. Ekstraksi Informasi 5W+1H
- Kami menggunakan fungsi summarize_5w1h() untuk mengekstrak:
- Who: Nama perusahaan, kode saham, atau narasumber seperti direktur, CEO, dll.
- What: Informasi kinerja perusahaan (laba, pendapatan, penjualan, dll).
- When: Waktu kejadian (bulan, tahun, kuartal).
- Where: Lokasi atau kantor perusahaan.
- Why: Alasan di balik suatu peristiwa (penyebab kenaikan/penurunan).
- How: Strategi atau cara perusahaan mencapai sesuatu.
- Ekstraksi dilakukan dengan pola kata kunci sederhana menggunakan library re (Regular Expression).
---
3. Pemrosesan dengan Apache Spark
- Berita dimuat ke dalam Spark DataFrame, lalu fungsi summarize_5w1h() dijalankan sebagai User Defined Function (UDF) agar bisa diproses secara paralel dan cepat dalam skala besar.
---
4. Menyimpan Hasil ke MongoDB
- Hasil ekstraksi disimpan ke dua koleksi baru yaitu transformed_stock_news dan transformed_market_news
- Setiap data hasil ekstraksi mencakup elemen 5W+1H, judul asli, dan ID berita
- Program akan menampilkan berapa banyak berita yang berhasil diproses saat dijalankan.
