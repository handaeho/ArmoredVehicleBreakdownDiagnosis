"""
> 220110 데이터(중복 제거) => periodic, event = 466(2020), 668(2021)
> 220502 데이터(중복 제거) => periodic: 275, event: 274
"""
import pandas as pd

from data_handling import convert_data_csv


def extract_periodic_file_name(year, dat_path, csv_path, parquet_path):
    t = convert_data_csv(year=year, dat_path=dat_path, csv_path=csv_path, parquet_path=parquet_path)

    periodic_file_path_list = t.find_path_periodic_file()
    print(len(periodic_file_path_list), periodic_file_path_list)

    file_name = []
    date_list = []
    data_list = []

    for p in periodic_file_path_list:
        f_name = p.split('/')[-1][:-4]
        c_date = f_name[:8]

        file_name.append(f_name)
        date_list.append(c_date)

    for id, name, path, create, edit in zip(file_name, file_name, periodic_file_path_list, date_list, date_list):
        data_list.append([id, name, path, create, edit])

    for i in data_list:
        i.append('O')
        i.append('S')
        i.append('C')
        i.append('N')
        i.append('육군')
        i.append('육군')

    df = pd.DataFrame(data_list, columns=['FILEID', 'FILENM', 'FILEPATH', 'CRTDT', 'MDFCDT', 'FILESNSR', 'FILETYPE', 'FILEDIV', 'FILEPT', 'CRTOR', 'MDFR'])
    df = df[['FILEID', 'FILENM', 'FILESNSR', 'FILETYPE', 'FILEDIV', 'FILEPT', 'CRTDT', 'CRTOR', 'MDFCDT', 'MDFR', 'FILEPATH']]

    df = df.astype('string')

    cond = df[df['FILEPATH'].str.contains('기술')].index
    df.drop(cond, inplace=True)

    print(df)

    df.to_csv('/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/220502_sensor_data_p.csv', encoding='CP949', index=False)

    return df


def extract_event_file_name(year, dat_path, csv_path, parquet_path):
    t = convert_data_csv(year=year, dat_path=dat_path, csv_path=csv_path, parquet_path=parquet_path)

    event_file_path_list = t.find_path_event_file()
    print(len(event_file_path_list), event_file_path_list)

    file_name = []
    date_list = []
    data_list = []

    for p in event_file_path_list:
        f_name = p.split('/')[-1][:-4]
        c_date = f_name[:8]

        file_name.append(f_name)
        date_list.append(c_date)

    for id, name, path, create, edit in zip(file_name, file_name, event_file_path_list, date_list, date_list):
        data_list.append([id, name, path, create, edit])

    for i in data_list:
        i.append('O')
        i.append('S')
        i.append('U')
        i.append('N')
        i.append('육군')
        i.append('육군')

    df = pd.DataFrame(data_list, columns=['FILEID', 'FILENM', 'FILEPATH', 'CRTDT', 'MDFCDT', 'FILESNSR', 'FILETYPE', 'FILEDIV', 'FILEPT', 'CRTOR', 'MDFR'])
    df = df[['FILEID', 'FILENM', 'FILESNSR', 'FILETYPE', 'FILEDIV', 'FILEPT', 'CRTDT', 'CRTOR', 'MDFCDT', 'MDFR', 'FILEPATH']]

    df = df.astype('string')

    cond = df[df['FILEPATH'].str.contains('기술')].index
    df.drop(cond, inplace=True)

    print(df)

    df.to_csv('/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/220502_sensor_data_e.csv', encoding='CP949', index=False)

    return df


if __name__ == '__main__':
    y = 2020

    dat_path = f'/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220110_sensor_data/dat_data/{y}'
    csv_path = '/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220110_sensor_data/csv_data'
    # dat_path = '/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220502_sensor_data/dat_data'
    # csv_path = '/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220502_sensor_data/csv_data'
    parquet_path = '/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220502_sensor_data/parquet_data'

    df_p_2020 = extract_periodic_file_name(year=2020, dat_path=dat_path, csv_path=csv_path, parquet_path=parquet_path)
    df_p_2021 = extract_periodic_file_name(year=2021, dat_path=dat_path, csv_path=csv_path, parquet_path=parquet_path)

    df_e_2020 = extract_event_file_name(year=2020, dat_path=dat_path, csv_path=csv_path, parquet_path=parquet_path)
    df_e_2021 = extract_event_file_name(year=2021, dat_path=dat_path, csv_path=csv_path, parquet_path=parquet_path)

    # Dataframe Concat
    df_1 = pd.read_csv('/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/220110_sensor_data_2020_p.csv', encoding='CP949')
    df_2 = pd.read_csv('/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/220110_sensor_data_2020_e.csv', encoding='CP949')
    df_3 = pd.read_csv('/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/220110_sensor_data_2021_p.csv', encoding='CP949')
    df_4 = pd.read_csv('/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/220110_sensor_data_2021_e.csv', encoding='CP949')
    df_5 = pd.read_csv('/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/220502_sensor_data_p.csv', encoding='CP949')
    df_6 = pd.read_csv('/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/220502_sensor_data_e.csv', encoding='CP949')

    df_1_2 = pd.concat([df_1, df_2]).reset_index(drop=True)
    df_1_2_3 = pd.concat([df_1_2, df_3]).reset_index(drop=True)
    df_1_2_3_4 = pd.concat([df_1_2_3, df_4]).reset_index(drop=True)
    df_1_2_3_4_5 = pd.concat([df_1_2_3_4, df_5]).reset_index(drop=True)
    df_1_2_3_4_5_6 = pd.concat([df_1_2_3_4_5, df_6]).reset_index(drop=True)

    df_fin = pd.DataFrame(df_1_2_3_4_5_6)

    df_fin = df_fin.astype('string')
    print(df_fin.dtypes)
    print(df_fin)

    df_fin.to_csv('/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/220110_220506_all_file_name_FILEINFO_table_data.csv', encoding='CP949', index=False)
