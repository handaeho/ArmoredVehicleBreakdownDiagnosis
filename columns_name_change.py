import pandas as pd


csv_path = '/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220110_sensor_data/'

print("************************* START *************************")

# df_periodic = pd.read_csv(csv_path + 'all_periodic_merged_data_with_code.csv', skipinitialspace=True, encoding='CP949', low_memory=False)
df_event = pd.read_csv(csv_path + 'all_periodic_merged_data_with_code.csv', skipinitialspace=True, encoding='CP949', low_memory=False)

print("************************* Read data finish *************************")

# df_periodic = df_periodic.drop(columns='year')
df_event = df_event.drop(columns='year')

# print(df_periodic.columns)
print(df_event.columns)

# df_periodic.columns = ['FILENM', 'SDAID', 'OPERDATE', 'OPERTIME', 'DTTIME', 'TIME', 'ENGSTA', 'ENGSPD', 'ENGTOK', 'REQENDTOK',
#                        'ENGLOAD', 'SDHSPD', 'ACCPEDAL', 'COOLTEMP', 'ENGGASTEMP', 'ENGWARNING', 'ENGOILPRS', 'ENGOILSTA', 'ENGHEAT', 'ENGGOV',
#                        'INDUSTFIL', 'OUTTEMP', 'COOLOIL', 'COOLLANT', 'FUELTEMP', 'TRANSOUTSPD', 'TRANSINSPD', 'ENGOVERCTLMD', 'OVERREQTOK', 'AUTOTRANS',
#                        'DETAILTRANS', 'REQTRANS', 'CURRTTRANS', 'EMERTRANSMD', 'TRANSOILTEMP', 'TRANSOILHEAT', 'FANMD', 'FANAIRTEMP', 'FANCOOLANTTEMP', 'FANVVALDUTY',
#                        'PARKSTA', 'BREAK', 'RETDBREAK', 'RETDCHO', 'RETDTOK', 'REQRETDTOK', 'RETDOILTEMP', 'AIRMASTER', 'BREAKOIL', 'FRTBREAKPRS',
#                        'BACKBREAKPRS', 'AIRTANKR', 'AIRTANKL', 'FRTAIRTAND', 'FUEL', 'VOLTAGE', 'BATTSTA', 'ABSOPER', 'ABSYAJI', 'ABSWARNING',
#                        '2AVGSPEED', '2LSPEED', '2RSPEED', '3LSPEED', '3RSPEED', '1LOCK', '2LOCK', 'DMOTION', '3LOCK', '4LOCK',
#                        'SHIFTMODE', 'LOWSWITCH', 'NORSWITCH', 'HIGHSWITCH', 'PNEUMATIC', '8BY8', '6BY6', '1WHEEL', '2WHEEL', '3WHEEL',
#                        '4WHEEL', 'PROMOTEWATER', 'LOCKRELEASE', 'LOWOILPRS', 'LOWOILTEMP', 'LOWOILQTY', 'LOWOILFILTER', 'WINCHCLUTCH', 'BACKDOOROPEN', 'OVERPRS',
#                        'OVERPRSEQP', 'CTISAIRPRS', 'PBIT']


df_event.columns = ['FILENM', 'SDAID', 'OPERDATE', 'OPERTIME', 'DATE', 'TIME', 'EVENTCD', 'MSG', 'STATE']

# print(df_periodic.columns)
print(df_event.columns)

# df_periodic.to_csv(csv_path + 'all_periodic_merged_data_with_code_20220110.csv', sep=',', encoding='CP949', index=False)
df_event.to_csv(csv_path + 'all_event_merged_data_with_code_20220110.csv', sep=',', encoding='CP949', index=False)



