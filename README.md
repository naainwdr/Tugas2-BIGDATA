# Tugas2-BIGDATA

LAPORAN INGESTION DATA BERITA IQPLUS
---
Dalam proyek ini, kami menggunakan pendekatan NLP (Natural Language Processing) untuk mengekstraksi informasi penting dari berita saham yang disimpan di MongoDB. Kami memanfaatkan Python dengan library pymongo untuk mengakses data dan menggunakan regex untuk ekstraksi informasi 5W+1H (What, Who, When, Where, Why, How).

---
1. Konfigurasi MongoDB:
- Menghubungkan ke MongoDB untuk mengambil data dari koleksi stock_news dan market_news.

2. Ekstraksi Informasi 5W+1H:

WHO (Siapa): Menemukan kode saham, nama perusahaan, dan narasumber menggunakan kata kunci seperti "Tbk", "menurut", "Direktur".

WHAT (Apa): Mencari informasi kinerja, laba, penjualan menggunakan kata kunci seperti "laba", "penjualan", "kinerja".

WHEN (Kapan): Mendeteksi waktu dengan kata kunci seperti "pada", bulan, tahun, atau kuartal.

WHERE (Di mana): Menemukan lokasi menggunakan kata seperti "berlokasi di", "kantor di".

WHY (Mengapa): Menyimpulkan alasan menggunakan kata seperti "karena", "dipengaruhi".

HOW (Bagaimana): Mengidentifikasi strategi atau cara dengan kata seperti "melalui", "strategi", "upaya".

Penyimpanan Hasil:

Berita yang berhasil diringkas disimpan dalam koleksi baru di MongoDB dan file JSON sebagai backup.

Program otomatis mencetak jumlah berita yang berhasil diringkas.

Kesimpulan: Kami memilih NLP untuk ekstraksi informasi karena teks berita bebas dan tidak terstruktur, yang lebih cocok untuk diproses dengan NLP, dibandingkan dengan Spark yang lebih cocok untuk data terstruktur.
