import os
import pandas as pd
import mmap
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pc

csv_path = 'sensor_data/220110_sensor_data/csv_data/'
csv_file_name = 'event_data_2022.csv'

parquet_path = 'sensor_data/220110_sensor_data/parquet_data/'

py_table = pc.read_csv(csv_path + csv_file_name, parse_options=pc.ParseOptions(delimiter=','))
df = py_table.to_pandas()

df.to_parquet(parquet_path + 'event_data_2022.parquet', compression='gzip')
print(df)


