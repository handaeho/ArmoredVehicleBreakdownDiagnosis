import pyarrow.csv as pc

csv_path = '/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220110_sensor_data/'

periodic_csv_file_name = 'all_periodic_merged_data_with_code.csv'
event_csv_file_name = 'all_event_merged_data_with_code.csv'

parquet_path = '/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220110_sensor_data/'

periodic_py_table = pc.read_csv(csv_path + periodic_csv_file_name, parse_options=pc.ParseOptions(delimiter=','))
event_py_table = pc.read_csv(csv_path + event_csv_file_name, parse_options=pc.ParseOptions(delimiter=','))

periodic_df = periodic_py_table.to_pandas()
event_df = event_py_table.to_pandas()

periodic_df.to_parquet(parquet_path + 'all_periodic_merged_data_with_code.parquet', compression='gzip')
event_df.to_parquet(parquet_path + 'all_event_merged_data_with_code.parquet', compression='gzip')

print(periodic_df)
print(event_df)

periodic_df = periodic_df.drop(columns='year')
# event_df = event_df.drop(columns='year')

periodic_df.columns = ['FILENM', 'SDAID', 'OPERDATE', 'OPERTIME', 'DTTIME', 'TIME', 'ENGSTA', 'ENGSPD', 'ENGTOK', 'REQENDTOK',
                       'ENGLOAD', 'SDHSPD', 'ACCPEDAL', 'COOLTEMP', 'ENGGASTEMP', 'ENGWARNING', 'ENGOILPRS', 'ENGOILSTA', 'ENGHEAT', 'ENGGOV',
                       'INDUSTFIL', 'OUTTEMP', 'COOLOIL', 'COOLLANT', 'FUELTEMP', 'TRANSOUTSPD', 'TRANSINSPD', 'ENGOVERCTLMD', 'OVERREQTOK', 'AUTOTRANS',
                       'DETAILTRANS', 'REQTRANS', 'CURRTTRANS', 'EMERTRANSMD', 'TRANSOILTEMP', 'TRANSOILHEAT', 'FANMD', 'FANAIRTEMP', 'FANCOOLANTTEMP', 'FANVVALDUTY',
                       'PARKSTA', 'BREAK', 'RETDBREAK', 'RETDCHO', 'RETDTOK', 'REQRETDTOK', 'RETDOILTEMP', 'AIRMASTER', 'BREAKOIL', 'FRTBREAKPRS',
                       'BACKBREAKPRS', 'AIRTANKR', 'AIRTANKL', 'FRTAIRTAND', 'FUEL', 'VOLTAGE', 'BATTSTA', 'ABSOPER', 'ABSYAJI', 'ABSWARNING',
                       '2AVGSPEED', '2LSPEED', '2RSPEED', '3LSPEED', '3RSPEED', '1LOCK', '2LOCK', 'DMOTION', '3LOCK', '4LOCK',
                       'SHIFTMODE', 'LOWSWITCH', 'NORSWITCH', 'HIGHSWITCH', 'PNEUMATIC', '8BY8', '6BY6', '1WHEEL', '2WHEEL', '3WHEEL',
                       '4WHEEL', 'PROMOTEWATER', 'LOCKRELEASE', 'LOWOILPRS', 'LOWOILTEMP', 'LOWOILQTY', 'LOWOILFILTER', 'WINCHCLUTCH', 'BACKDOOROPEN', 'OVERPRS',
                       'OVERPRSEQP', 'CTISAIRPRS', 'PBIT']


# event_df.columns = ['FILENM', 'SDAID', 'OPERDATE', 'OPERTIME', 'DATE', 'TIME', 'EVENTCD', 'MSG', 'STATE']

print(periodic_df.columns)
# print(event_df.columns)

periodic_df.to_csv(csv_path + 'all_periodic_merged_data_with_code_20220110.csv', sep=',', encoding='CP949', index=False)
# event_df.to_csv(csv_path + 'all_event_merged_data_with_code_20220110.csv', sep=',', encoding='CP949', index=False)

