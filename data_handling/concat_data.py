import pandas as pd
from tqdm import tqdm   # 작업진행률 표시
import os


# read periodic file list in periodic folder
periodic_csv_path_dir = '/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220110_sensor_data/csv_data/periodic_220314/'
periodic_csv_file_list = os.listdir(periodic_csv_path_dir)
print(periodic_csv_file_list)

# read event file list in event folder
event_csv_path_dir = '/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220110_sensor_data/csv_data/event_220314/'
event_csv_file_list = os.listdir(event_csv_path_dir)
print(event_csv_file_list)

# save path of merged data
merged_csv_path_dir = '/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220110_sensor_data/csv_data/'

periodic_df_all = pd.DataFrame()
event_df_all = pd.DataFrame()

for i in tqdm(range(len(periodic_csv_file_list))):
    periodic_path = periodic_csv_path_dir + periodic_csv_file_list[i]

    periodic_df = pd.read_csv(periodic_path, sep=',', skipinitialspace=True, encoding='CP949', low_memory=False)

    periodic_df_all = pd.concat([periodic_df_all, periodic_df]).reset_index(drop=True).drop_duplicates()

print(periodic_df_all)

periodic_df_all.to_csv(merged_csv_path_dir + 'all_periodic_merged_data_with_code.csv', sep=',', encoding='CP949', index=False)

for j in tqdm(range(len(event_csv_file_list))):
    event_path = event_csv_path_dir + event_csv_file_list[j]

    event_df = pd.read_csv(event_path, sep=',', skipinitialspace=True, encoding='CP949', low_memory=False)

    event_df_all = pd.concat([event_df_all, event_df]).reset_index(drop=True).drop_duplicates()

print(event_df_all)

event_df_all.to_csv(merged_csv_path_dir + 'all_event_merged_data_with_code.csv', sep=',', encoding='CP949', index=False)




