import json
import re
from pymongo import MongoClient
from bson import ObjectId

# Kelas untuk menangani serialisasi ObjectId ke JSON
class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super(JSONEncoder, self).default(obj)

# Fungsi ekstraksi 5W + 1H yang lebih spesifik
def summarize_5w1h(berita):
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
        "Apa": what,
        "Siapa": who,
        "Kapan": when,
        "Di mana": where,
        "Mengapa": why,
        "Bagaimana": how
    }

# Konfigurasi MongoDB
def process_news_from_mongodb():
    # Konfigurasi koneksi MongoDB
    mongo_uri = "mongodb://localhost:27017/"  # Ganti dengan URI MongoDB Anda
    db_name = "stock_news_db"                 # Ganti sesuai nama database Anda
    input_collection = "stock_news"           # Koleksi sumber berita
    output_collection = "ringkasan_stock_news" # Koleksi target hasil ringkasan
    
    try:
        # Buat koneksi ke MongoDB
        client = MongoClient(mongo_uri)
        db = client[db_name]
        
        # Periksa koneksi
        print(f"Terhubung ke MongoDB: {client.server_info()['version']}")
        
        # Buka koleksi
        stock_news_collection = db[input_collection]
        result_collection = db[output_collection]
        
        # Ambil semua berita
        berita_list = list(stock_news_collection.find({}))
        
        print(f"Ditemukan {len(berita_list)} berita di koleksi '{input_collection}'")
        
        # Proses semua berita
        ringkasan_semua = []
        for idx, berita in enumerate(berita_list):
            if 'konten' in berita:
                try:
                    ringkasan = summarize_5w1h(berita)
                    # Tambahkan judul asli sebagai referensi
                    ringkasan["Judul Asli"] = berita.get("judul", "")
                    
                    # Tambahkan ID berita asli sebagai referensi (ubah ObjectId menjadi string)
                    if '_id' in berita:
                        ringkasan["berita_id"] = str(berita["_id"])
                    
                    # Tambahkan ke list untuk disimpan
                    ringkasan_semua.append(ringkasan)
                    print(f"Berhasil meringkas berita ke-{idx+1}")
                except Exception as e:
                    print(f"Gagal memproses berita ke-{idx+1}: {e}")
            else:
                print(f"Lewatkan berita ke-{idx+1} tanpa 'konten': {berita.get('judul', '(judul tidak ada)')}")
        
        # Hapus koleksi lama jika ada
        if output_collection in db.list_collection_names():
            db[output_collection].drop()
            print(f"Koleksi '{output_collection}' lama dihapus")
        
        # Masukkan hasil ke koleksi baru
        if ringkasan_semua:
            result_collection.insert_many(ringkasan_semua)
            print(f"Berhasil menyimpan {len(ringkasan_semua)} ringkasan ke koleksi '{output_collection}'")
        
        # Simpan juga ke file JSON untuk backup (dengan custom encoder)
        try:
            output_path = "ringkasan_stock_news_backup.json"
            with open(output_path, "w", encoding="utf-8") as out_file:
                json.dump(ringkasan_semua, out_file, indent=2, ensure_ascii=False, cls=JSONEncoder)
                print(f"Backup tersimpan di: {output_path}")
        except Exception as e:
            print(f"Gagal menyimpan file backup: {e}")
            
        return len(ringkasan_semua), len(berita_list)
    
    except Exception as e:
        print(f"Terjadi kesalahan: {e}")
        return 0, 0
    finally:
        if 'client' in locals():
            client.close()
            print("Koneksi MongoDB ditutup")

# Jalankan proses
if __name__ == "__main__":
    success_count, total_count = process_news_from_mongodb()
    print(f"\nRingkasan 5W1H selesai: {success_count} dari {total_count} berita berhasil diproses.")