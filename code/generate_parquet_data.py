import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

N = 1000000 # 1 Juta baris

print(f"Mulai men-generate {N} baris data...")

# 1. Men-generate data secara efisien menggunakan NumPy
transaction_ids = np.arange(1, N + 1, dtype=np.int32)
customer_ids = np.random.randint(1000, 5000, size=N).astype(np.int32)

end_date = pd.Timestamp('2026-08-11')
start_date = end_date - pd.DateOffset(years=1)
seconds_diff = int((end_date - start_date).total_seconds())
random_seconds = np.random.randint(0, seconds_diff, size=N)
transaction_dates = start_date + pd.to_timedelta(random_seconds, unit='s')

amounts = np.round(np.random.uniform(10.0, 10000.0, size=N), 2)

is_processed = np.random.choice([True, False], size=N, p=[0.8, 0.2])

sales_codes = np.random.choice(
    ['SP01', 'SP02', 'SP03', 'SP04', None], 
    size=N, 
    p=[0.3, 0.3, 0.2, 0.1, 0.1]
)

# 2. Gabungkan ke dalam Pandas DataFrame
df = pd.DataFrame({
    'TransactionID': transaction_ids,
    'CustomerID': customer_ids,
    'TransactionDate': transaction_dates,
    'Amount': amounts,
    'IsProcessed': is_processed,
    'SalesPersonCode': sales_codes
})

# 3. Definisikan Schema Awal (Gunakan float64 untuk Amount sementara)
initial_schema = pa.schema([
    ('TransactionID', pa.int32()),
    ('CustomerID', pa.int32()),
    ('TransactionDate', pa.timestamp('ms')),
    ('Amount', pa.float64()),  # <-- Diubah menjadi float64 agar sesuai dengan Pandas
    ('IsProcessed', pa.bool_()),
    ('SalesPersonCode', pa.string())
])

# 4. Buat PyArrow Table
table = pa.Table.from_pandas(df, schema=initial_schema)

# 5. Lakukan CASTING ke Decimal secara aman menggunakan engine PyArrow
final_schema = table.schema.set(
    table.schema.get_field_index('Amount'), 
    pa.field('Amount', pa.decimal128(18, 2))
)
table = table.cast(final_schema)

# 6. Tulis ke file Parquet
output_filename = 'sql_server_1m_sample.parquet'
pq.write_table(table, output_filename, compression='snappy')

print(f"Selesai! File '{output_filename}' berhasil dibuat.")