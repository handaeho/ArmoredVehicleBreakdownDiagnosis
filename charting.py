import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

parquet_path = 'sensor_data/220110_sensor_data/parquet_data/'

parquet_file_name_2020 = 'periodic_data_2020.parquet'
parquet_file_name_2021 = 'periodic_data_2021.parquet'
parquet_file_name_2022 = 'periodic_data_2022.parquet'

# read
df_2020 = pd.read_parquet(parquet_path + parquet_file_name_2020)
# df_2021 = pd.read_parquet(parquet_path + parquet_file_name_2021)
# df_2022 = pd.read_parquet(parquet_path + parquet_file_name_2022)
print(df_2020)

df_2020[["engineSpeed", "engineTorque"]] = df_2020[["engineState", "engineSpeed", "engineTorque"]].astype(float)
df_2020["engineSpeed"] = df_2020["engineSpeed"].fillna(df_2020["engineSpeed"].median())
df_2020["engineTorque"] = df_2020["engineTorque"].fillna(df_2020["engineTorque"].median())
df_2020.head(5)



