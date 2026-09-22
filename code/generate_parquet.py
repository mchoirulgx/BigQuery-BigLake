import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import time
import os
import uuid
from faker import Faker

# ==========================================
# KONFIGURASI
# ==========================================
TARGET_MB = 1024 # Set target 1024 MB (1 GB)
TARGET_BYTES = TARGET_MB * 1024 * 1024
FILE_NAME = f'users_dummy_{TARGET_MB}MB.parquet'
CHUNK_SIZE = 100_000 # Proses per 100.000 baris agar RAM tidak jebol

print("Mempersiapkan dictionary (pool) Nama & Email agar proses lebih cepat...")
fake = Faker('id_ID')

# Membuat 10.000 data unik di awal untuk di-sample
POOL_SIZE = 10_000
name_pool = np.array([fake.name() for _ in range(POOL_SIZE)])
# Membuat email berdasarkan nama dengan menghapus spasi dan titik
email_pool = np.array([
    f"{n.lower().replace(' ', '').replace('.', '')}{np.random.randint(10, 99)}@gmail.com" 
    for n in name_pool
])

# Definisikan Schema sesuai dengan tabel `users`
# Menggunakan 'ns' (nanosecond) karena ini adalah default output dari Pandas datetime
# PERBAIKAN: Ganti 'ns' menjadi 'us'
schema = pa.schema([
    ('id', pa.string()),
    ('name', pa.string()),
    ('email', pa.string()),
    ('created_at', pa.timestamp('us')),
    ('updated_at', pa.timestamp('us')),
    ('deleted_at', pa.timestamp('us'))
])

writer = pq.ParquetWriter(FILE_NAME, schema, compression='NONE')

current_size = 0
total_rows = 0

# Range waktu: 1 Januari 2024 - 1 Januari 2026 (dalam detik)
start_ts = int(pd.Timestamp('2024-01-01').timestamp())
end_ts = int(pd.Timestamp('2026-01-01').timestamp())

print(f"\nMulai generate dan menulis data ke {FILE_NAME} ...")
start_time = time.time()

try:
    while current_size < TARGET_BYTES:
        # 1. Generate Timestamps (created_at)
        created_arr = np.random.randint(start_ts, end_ts, size=CHUNK_SIZE)
        
        # 2. updated_at (Acak antara 0 hari sampai 60 hari setelah created_at)
        updated_arr = created_arr + np.random.randint(0, 86400 * 60, size=CHUNK_SIZE)
        
        # 3. deleted_at (Simulasi 10% user dihapus, sisanya NULL)
        is_deleted = np.random.rand(CHUNK_SIZE) < 0.10 
        # Jika dihapus, waktunya 1 sampai 30 hari setelah updated_at
        deleted_arr = updated_arr + np.random.randint(86400, 86400 * 30, size=CHUNK_SIZE)
        
        # 4. Susun ke dalam DataFrame
        df = pd.DataFrame({
            'id': [str(uuid.uuid4()) for _ in range(CHUNK_SIZE)], # Tetap 100% unik
            'name': np.random.choice(name_pool, size=CHUNK_SIZE),
            'email': np.random.choice(email_pool, size=CHUNK_SIZE),
            'created_at': pd.to_datetime(created_arr, unit='s'),
            'updated_at': pd.to_datetime(updated_arr, unit='s'),
            # np.where digunakan untuk menaruh NaT (Not a Time) / NULL jika is_deleted False
            'deleted_at': pd.to_datetime(np.where(is_deleted, deleted_arr, np.nan), unit='s') 
        })
        
        # Convert ke PyArrow dan tulis ke file Parquet
        table = pa.Table.from_pandas(df, schema=schema)
        writer.write_table(table)
        
        total_rows += CHUNK_SIZE
        
        # Cek ukuran file di disk saat ini
        current_size = os.path.getsize(FILE_NAME)
        
        print(f"Tertulis {total_rows:,} baris | Ukuran saat ini: {current_size / (1024*1024):.2f} MB", end='\r')

finally:
    # Selalu tutup writer agar metadata Parquet tidak corrupt
    writer.close()

final_size = os.path.getsize(FILE_NAME)
print(f"\n\n--- Simulasi Selesai! ---")
print(f"Total waktu eksekusi : {time.time() - start_time:.2f} detik")
print(f"Total baris          : {total_rows:,}")
print(f"Ukuran akhir file    : {final_size / (1024*1024):.2f} MB ({final_size:,} bytes)")