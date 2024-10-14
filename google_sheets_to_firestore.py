import os
import requests
import gspread
from firebase_functions import https_fn, scheduler_fn
import firebase_admin
from firebase_admin import firestore, storage, credentials
import google.cloud.firestore
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
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1RQ2PZMRKjBVHpG0ettmuiDjjxzpF7OfFDfXlJDT0ElE/edit?usp=sharing'

# Buka Google Sheets
client = gspread.service_account(filename='wargabut-11-52713d34ead5.json')
sheet = client.open_by_url(SHEET_URL).sheet1

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
    """Konversi bulan dari bahasa Indonesia ke bahasa Inggris pada tanggal."""
    for indo_bulan, eng_bulan in bulan_mapping.items():
        indonesian_date_str = indonesian_date_str.replace(indo_bulan, eng_bulan)
    return indonesian_date_str

def fetch_existing_events(db, collection_name):
    """Ambil semua dokumen dari koleksi Firestore."""
    return {doc.id: doc.to_dict() for doc in db.collection(collection_name).stream()}


def process_event_data(row, field_mapping):
    """Proses data dari row dan mapping field ke dalam dokumen Firestore."""
    doc_data = {}
    for sheet_field, firestore_field in field_mapping.items():
        doc_data[firestore_field] = row.get(sheet_field, '')
    
    # Tambahkan field description kosong
    doc_data['desc'] = ''
    return doc_data


def generate_unique_id(event_name, event_date):
    """Generate unique ID berdasarkan nama event dan tanggal."""
    unique_str = f"{event_name}_{event_date}"
    return hashlib.md5(unique_str.encode()).hexdigest()


def should_skip_event(event_date_str, event_name, existing_events, doc_data):
    """Periksa apakah event perlu di-skip."""
    try:
        event_date = parser.parse(event_date_str, dayfirst=True)
        if event_date < datetime.now().replace(hour=0, minute=0, second=0, microsecond=0):
            print(f"{Colors.WARNING}Event '{event_name}' pada '{event_date_str}' diabaikan karena sudah terlewat.{Colors.ENDC}")
            return True
    except ValueError:
        print(f"{Colors.FAIL}Tanggal event '{event_name}' kosong atau tidak valid.{Colors.ENDC}")
    return False


def update_event_in_firestore(db, collection_name, unique_id, doc_data):
    """Simpan dokumen event ke Firestore."""
    doc_ref = db.collection(collection_name).document(unique_id)
    doc_ref.set(doc_data)


def update_area_collection(db, area_set, area_collection_name):
    """Simpan area ke koleksi Firestore."""
    area_data = [{'area': area} for area in area_set]
    area_added_count = 0

    existing_areas = {doc.id for doc in db.collection(area_collection_name).stream()}

    for area in area_data:
        area_id = area['area']
        if area_id not in existing_areas:
            area_doc_ref = db.collection(area_collection_name).document(area_id)
            area_doc_ref.set(area)
            print(f"{Colors.OKGREEN}Area baru '{area_id}' ditambahkan ke koleksi {area_collection_name}.{Colors.ENDC}")
            area_added_count += 1

    return area_added_count


def delete_past_events(db, collection_name):
    """Hapus event yang sudah terlewat dari Firestore."""
    deleted_count = 0
    events = db.collection(collection_name).stream()

    for event in events:
        event_data = event.to_dict()
        event_date_str = event_data.get('date', '')

        english_date_str = convert_date_to_english(event_date_str)

        try:
            event_date = parser.parse(english_date_str, dayfirst=True)
            if event_date < datetime.now().replace(hour=0, minute=0, second=0, microsecond=0):
                db.collection(collection_name).document(event.id).delete()
                deleted_count += 1
                print(f"{Colors.FAIL}Event '{event_data['event_name']}' pada '{event_date_str}' dihapus.{Colors.ENDC}")
        except ValueError:
            print(f"{Colors.FAIL}Tanggal event '{event_data['event_name']}' kosong.{Colors.ENDC}")

    return deleted_count


def delete_events_without_date(sheet, firestore_collection_name, db, data_dicts):
    """Hapus event tanpa tanggal dan tidak relevan di sheet."""
    events_ref = db.collection(firestore_collection_name)
    events = events_ref.stream()

    deleted_events_count = 0

    for event in events:
        event_data = event.to_dict()
        event_name = event_data.get("event_name", "")
        event_date_str = event_data.get("event_date", "")

        if not event_date_str:
            event_exists_in_sheet = any(row['Nama Acara (Link acara klik)'] == event_name for row in data_dicts)

            if not event_exists_in_sheet:
                event.reference.delete()
                deleted_events_count += 1
                print(f"{Colors.FAIL}Menghapus event '{event_name}' tanpa tanggal dan sudah tidak ada di sheet.{Colors.ENDC}")

    print(f"{Colors.OKBLUE}Jumlah event tanpa tanggal yang dihapus: {deleted_events_count}{Colors.ENDC}")
    return deleted_events_count


def delete_empty_areas(db, collection_name, area_collection_name):
    """Hapus area yang tidak memiliki event terkait dari Firestore."""
    deleted_area_count = 0
    areas = db.collection(area_collection_name).stream()

    for area in areas:
        area_id = area.id
        events_in_area = db.collection(collection_name).where('area', '==', area_id).stream()

        if not any(events_in_area):
            db.collection(area_collection_name).document(area_id).delete()
            deleted_area_count += 1
            print(f"{Colors.FAIL}Area '{area_id}' dihapus karena tidak ada event terkait.{Colors.ENDC}")

    return deleted_area_count


def main_process(db, data_dicts, field_mapping):
    collection_name = 'jfestchart'
    area_collection_name = 'event_areas'

    # Fetch existing events
    existing_events = fetch_existing_events(db, collection_name)

    initial_count = len(existing_events)
    print(f"{Colors.OKCYAN}Jumlah data di Firestore sebelum update: {initial_count}{Colors.ENDC}\n")

    added_count, updated_count, skipped_count = 0, 0, 0
    area_set = set()

    # Process event rows
    for row in data_dicts:
        doc_data = process_event_data(row, field_mapping)
        event_name = doc_data.get('event_name', '')
        event_date_str = doc_data.get('date', '')
        english_date_str = convert_date_to_english(event_date_str)

        if not event_name or should_skip_event(english_date_str, event_name, existing_events, doc_data):
            continue

        if doc_data['area']:
            area_set.add(doc_data['area'])

        unique_id = generate_unique_id(event_name, doc_data['date'])

        if unique_id in existing_events:
            existing_last_update_str = existing_events[unique_id].get('last_update', '')
            new_last_update_str = doc_data.get('last_update', '')

            try:
                existing_last_update = parser.parse(existing_last_update_str)
                new_last_update = parser.parse(new_last_update_str)
                if existing_last_update >= new_last_update:
                    skipped_count += 1
                    continue
                else:
                    updated_count += 1
            except ValueError:
                updated_count += 1
        else:
            added_count += 1

        update_event_in_firestore(db, collection_name, unique_id, doc_data)

    print(f"{Colors.OKBLUE}Data berhasil disimpan ke Cloud Firestore tanpa duplikasi.{Colors.ENDC}")
    print(f"{Colors.OKGREEN}Jumlah data ditambahkan: {added_count}{Colors.ENDC}")
    print(f"{Colors.OKGREEN}Jumlah data diperbarui: {updated_count}{Colors.ENDC}")
    print(f"{Colors.WARNING}Jumlah data diabaikan: {skipped_count}{Colors.ENDC}")
    print(f"{Colors.OKCYAN}Jumlah total data di Firestore setelah update: {initial_count + added_count}{Colors.ENDC}\n")

    area_added_count = update_area_collection(db, area_set, area_collection_name)
    print(f"{Colors.OKGREEN}Jumlah area ditambahkan: {area_added_count}{Colors.ENDC}\n")

    # Delete past events
    deleted_events_count = delete_past_events(db, collection_name)
    print(f"{Colors.FAIL}Jumlah event yang dihapus: {deleted_events_count}{Colors.ENDC}\n")

    # Delete events without date
    delete_events_without_date(sheet, collection_name, db, data_dicts)

    # Delete empty areas
    deleted_areas_count = delete_empty_areas(db, collection_name, area_collection_name)
    print(f"{Colors.FAIL}Jumlah area yang dihapus: {deleted_areas_count}{Colors.ENDC}\n")

    final_count = len(list(db.collection(collection_name).stream()))
    print(f"{Colors.OKCYAN}Jumlah total event di Firestore setelah penghapusan: {final_count}{Colors.ENDC}\n")


# Panggil fungsi utama untuk memulai proses
# main_process(db, data_dicts, field_mapping)

# Run once a day at midnight, to update events.
# Manually run the task here https://console.cloud.google.com/cloudscheduler
@scheduler_fn.on_schedule(schedule="every day 00:00")
def update_events(event: scheduler_fn.ScheduledEvent) -> None:
    try:
        main_process(db, data_dicts, field_mapping)
        print("Events updated successfully")
        return https_fn.Response("Events updated successfully", status=200)
    except Exception as e:
        print(f"Error: {str(e)}")
        return https_fn.Response(f"Error: {str(e)}", status=500)