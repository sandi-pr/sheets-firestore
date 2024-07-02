import os
import requests
import gspread
import firebase_admin
from firebase_admin import credentials, firestore, storage
import hashlib
from datetime import datetime
from dateutil import parser

# Definisikan warna menggunakan kode ANSI escape
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# URL Google Sheets publik
url = 'https://docs.google.com/spreadsheets/d/1RQ2PZMRKjBVHpG0ettmuiDjjxzpF7OfFDfXlJDT0ElE/edit?usp=sharing'

# Buka Google Sheets
client = gspread.service_account(filename='wargabut-11-52713d34ead5.json')
sheet = client.open_by_url(url).sheet1

# Ambil semua data dari Google Sheets
all_data = sheet.get_all_values()
header = all_data[0]
data = all_data[2:]
# Konversi data menjadi list of dictionaries
data_dicts = [dict(zip(header, row)) for row in data]
print(f"{Colors.OKCYAN}Data fetched from Google Sheets{Colors.ENDC}")

# Inisialisasi Firebase Admin SDK
cred = credentials.Certificate('wargabut-11-firebase-adminsdk-i9m7l-d339a10a00.json')
firebase_admin.initialize_app(cred, {
    'storageBucket': 'gs://wargabut-11.appspot.com'  # Ganti dengan nama bucket Firebase Storage
})
print(f"{Colors.OKCYAN}Firebase Admin SDK initialized{Colors.ENDC}")
print("\n")

# Referensi ke Cloud Firestore
db = firestore.client()
bucket = storage.bucket()

# Mapping antara nama kolom di Google Sheets dan nama field yang diinginkan di Firestore
field_mapping = {
    'Area': 'area',
    'Jam': 'time',
    'Last Update': 'last_update',
    'Link Acara': 'event_link',
    'Lokasi (baca keterangan lebih lanjut di Facebook Page)': 'location',
    'Nama Acara (Link acara klik)': 'event_name',
    'Tanggal': 'date'
}

# Mapping nama bulan dari bahasa Indonesia ke bahasa Inggris
bulan_mapping = {
    'Jan': 'Jan',
    'Feb': 'Feb',
    'Mar': 'Mar',
    'Apr': 'Apr',
    'Mei': 'May',
    'Jun': 'Jun',
    'Jul': 'Jul',
    'Agu': 'Aug',
    'Sep': 'Sep',
    'Okt': 'Oct',
    'Nov': 'Nov',
    'Des': 'Dec'
}

# Fungsi untuk mengonversi tanggal dari format bahasa Indonesia ke bahasa Inggris
def convert_date_to_english(indonesian_date_str):
    for indo_bulan, eng_bulan in bulan_mapping.items():
        indonesian_date_str = indonesian_date_str.replace(indo_bulan, eng_bulan)
    return indonesian_date_str

# # Fungsi untuk mendownload gambar dari URL
# def download_image(url):
#     response = requests.get(url)
#     if response.status_code == 200:
#         print(f"{Colors.OKCYAN}Downloaded image from {url}{Colors.ENDC}")
#         print("\n")
#         return response.content
#     else:
#         print(f"{Colors.FAIL}Failed to download image from {url}{Colors.ENDC}")
#         print("\n")
#         return None

# Fungsi untuk mengunggah gambar ke Firebase Storage
def upload_to_firebase_storage(image_data, file_name):
    blob = bucket.blob(file_name)
    blob.upload_from_string(image_data, content_type='image/jpeg')
    print(f"{Colors.OKCYAN}Uploaded image to Firebase Storage{Colors.ENDC}")
    print("\n")
    return blob.public_url

# Menyimpan data ke Firestore
collection_name = 'jfestchart'  # Ganti dengan nama koleksi yang diinginkan

# Ambil semua dokumen dari koleksi untuk memeriksa last_update
existing_events = {doc.id: doc.to_dict() for doc in db.collection(collection_name).stream()}
initial_count = len(existing_events)
print(f"{Colors.OKCYAN}Jumlah data di Firestore sebelum update: {initial_count}{Colors.ENDC}")
print("\n")

added_count = 0
updated_count = 0
skipped_count = 0

# Koleksi area
area_collection_name = 'event_areas'
area_set = set()

for row in data_dicts:
    # Buat dokumen baru dengan field yang disesuaikan
    doc_data = {}
    for sheet_field, firestore_field in field_mapping.items():
        doc_data[firestore_field] = row.get(sheet_field, '')

    # Cek apakah tanggal event sudah terlewat
    event_date_str = doc_data.get('date', '')
    english_date_str = convert_date_to_english(event_date_str)

    if doc_data['event_name'] == '':
        print(f"{Colors.WARNING}Event '{doc_data['event_name']}' pada '{event_date_str}' diabaikan karena Nama Acara kosong.{Colors.ENDC}")
        continue
    
    try:
        event_date = parser.parse(english_date_str, dayfirst=True)
        # Lanjutkan ke event berikutnya jika tanggal event sudah terlewat, tetapi tidak menghapus yang terjadi hari ini
        if event_date < datetime.now().replace(hour=0, minute=0, second=0, microsecond=0):
            print(f"{Colors.WARNING}Event '{doc_data['event_name']}' pada '{event_date_str}' diabaikan karena sudah terlewat.{Colors.ENDC}")
            continue
    except ValueError:
        print(f"{Colors.FAIL}Tanggal event '{event_date_str}' tidak dapat diparsing.{Colors.ENDC}")

    # Tambahkan area ke set area
    if doc_data['area']:
        area_set.add(doc_data['area'])

    # Menambahkan field description kosong
    doc_data['desc'] = ''

    # Membuat ID unik berdasarkan Nama Acara dan Tanggal
    unique_str = f"{doc_data['event_name']}_{doc_data['date']}"
    unique_id = hashlib.md5(unique_str.encode()).hexdigest()

    # Cek jika event sudah ada dan bandingkan last_update
    if unique_id in existing_events:
        existing_last_update_str = existing_events[unique_id].get('last_update', '')
        new_last_update_str = doc_data.get('last_update', '')

        try:
            existing_last_update = parser.parse(existing_last_update_str)
            new_last_update = parser.parse(new_last_update_str)
            
            # Skip update jika last_update dari dokumen di Firestore lebih baru atau sama dengan yang ada di Google Sheets
            if existing_last_update >= new_last_update:
                print(f"{Colors.OKCYAN}Event '{doc_data['event_name']}' pada '{doc_data['date']}' tidak diperbarui karena last_update lebih baru atau sama.{Colors.ENDC}")
                skipped_count += 1
                continue
            else:
                print(f"{Colors.OKGREEN}Event '{doc_data['event_name']}' pada '{doc_data['date']}' diperbarui.{Colors.ENDC}")
                updated_count += 1
        except ValueError:
            print(f"{Colors.WARNING}Tanggal last_update tidak dapat diparsing: '{existing_last_update_str}' atau '{new_last_update_str}'. Akan tetap diperbarui.{Colors.ENDC}")
            updated_count += 1
    else:
        print("\n")
        print(f"{Colors.OKGREEN}Event baru '{doc_data['event_name']}' pada '{doc_data['date']}' ditambahkan.{Colors.ENDC}")
        print("\n")
        added_count += 1

    # # Mendownload gambar dari URL postingan Facebook
    # print(f"{Colors.OKBLUE}Mengambil link event: {doc_data['event_link']}{Colors.ENDC}")
    # event_link = doc_data.get('event_link', '')
    # print(f"{Colors.OKBLUE}Mencoba mendownload gambar dari link event: {event_link}{Colors.ENDC}")
    # image_url = None
    # for post in get_posts(post_urls=[event_link]):
    #     print(f"{Colors.OKBLUE}Memeriksa postingan: {post}{Colors.ENDC}")
    #     if 'images' in post:
    #         image_url = post['images'][0]
    #         print(f"{Colors.OKBLUE}Gambar ditemukan: {image_url}{Colors.ENDC}")
    #         break
    #     else:
    #         print(f"{Colors.WARNING}Tidak ditemukan gambar pada postingan: {post}{Colors.ENDC}")
    #         print("\n")

    # if image_url:
    #     print("\n")
    #     print(f"{Colors.OKBLUE}Mengunggah gambar ke Firebase Storage: {image_url}{Colors.ENDC}")
    #     image_data = download_image(image_url)
    #     if image_data:
    #         # Mengunggah gambar ke Firebase Storage
    #         image_file_name = f"{unique_id}.jpg"
    #         image_public_url = upload_to_firebase_storage(image_data, image_file_name)
    #         # Menambahkan URL gambar ke field event
    #         doc_data['image_url'] = image_public_url
    #         print("\n")
    #         print(f"{Colors.OKBLUE}Gambar untuk event '{doc_data['event_name']}' diunggah ke Firebase Storage.{Colors.ENDC}")
    #         print("\n")

    # Menyimpan dokumen ke Firestore dengan ID unik
    doc_ref = db.collection(collection_name).document(unique_id)
    doc_ref.set(doc_data)

print("\n")
print(f"{Colors.OKBLUE}Data berhasil disimpan ke Cloud Firestore tanpa duplikasi.{Colors.ENDC}")
print(f"{Colors.OKGREEN}Jumlah data ditambahkan: {added_count}{Colors.ENDC}")
print(f"{Colors.OKGREEN}Jumlah data diperbarui: {updated_count}{Colors.ENDC}")
print(f"{Colors.WARNING}Jumlah data diabaikan: {skipped_count}{Colors.ENDC}")
print(f"{Colors.OKCYAN}Jumlah total data di Firestore setelah update: {initial_count + added_count}{Colors.ENDC}")
print("\n")

# Menyimpan area ke koleksi baru
area_data = [{'area': area} for area in area_set]
area_added_count = 0

# Ambil semua area yang sudah ada di Firestore
existing_areas = {doc.id for doc in db.collection(area_collection_name).stream()}

for area in area_data:
    area_id = area['area']
    if area_id in existing_areas:
        continue
    area_doc_ref = db.collection(area_collection_name).document(area_id)
    area_doc_ref.set(area)
    print(f"{Colors.OKGREEN}Area baru '{area_id}' ditambahkan ke koleksi {area_collection_name}.{Colors.ENDC}")
    area_added_count += 1

print("\n")
print(f"{Colors.OKGREEN}Jumlah area ditambahkan: {area_added_count}{Colors.ENDC}")
print("\n")

# Menghapus event yang sudah terlewat
def delete_past_events():
    deleted_count = 0
    # Mengambil semua dokumen dari koleksi
    events = db.collection(collection_name).stream()
    for event in events:
        event_data = event.to_dict()
        event_date_str = event_data.get('date', '')

        # Mengonversi tanggal dari format bahasa Indonesia ke bahasa Inggris
        english_date_str = convert_date_to_english(event_date_str)

        # Parsing tanggal event menggunakan dateutil.parser
        try:
            event_date = parser.parse(english_date_str, dayfirst=True)
            # Mengecek apakah tanggal event sudah terlewat, tetapi tidak menghapus yang terjadi hari ini
            if event_date < datetime.now().replace(hour=0, minute=0, second=0, microsecond=0):
                # Menghapus event yang sudah terlewat
                db.collection(collection_name).document(event.id).delete()
                deleted_count += 1
                print(f"{Colors.FAIL}Event '{event_data['event_name']}' pada '{event_date_str}' dihapus.{Colors.ENDC}")
        except ValueError:
            print(f"{Colors.FAIL}Tanggal event '{event_date_str}' tidak dapat diparsing.{Colors.ENDC}")
    return deleted_count
    
deleted_events_count = delete_past_events()
print("\n")
print(f"{Colors.FAIL}Jumlah event yang dihapus: {deleted_events_count}{Colors.ENDC}")
print("\n")

# Menghapus area yang tidak memiliki event lagi
def delete_empty_areas():
    deleted_area_count = 0
    # Mengambil semua dokumen dari koleksi area
    areas = db.collection(area_collection_name).stream()
    for area in areas:
        area_id = area.id
        # Mengecek apakah ada event yang terkait dengan area ini
        events_in_area = db.collection(collection_name).where('area', '==', area_id).stream()
        if not any(events_in_area):
            # Menghapus area yang tidak memiliki event terkait
            db.collection(area_collection_name).document(area_id).delete()
            deleted_area_count += 1
            print(f"{Colors.FAIL}Area '{area_id}' dihapus karena tidak ada event terkait.{Colors.ENDC}")
    return deleted_area_count

deleted_areas_count = delete_empty_areas()
print("\n")
print(f"{Colors.FAIL}Jumlah area yang dihapus: {deleted_areas_count}{Colors.ENDC}")
print("\n")

final_count = len(list(db.collection(collection_name).stream()))
print("\n")
print(f"{Colors.OKCYAN}Jumlah total event di Firestore setelah penghapusan: {final_count}{Colors.ENDC}")
print("\n")