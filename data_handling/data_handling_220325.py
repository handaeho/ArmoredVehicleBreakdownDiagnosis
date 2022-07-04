"""
[원본 dat 파일 전처리]

    1. 경로 내 data 파일을 모두 읽어온다.
        1) 'listdir'로 경로에 있는 폴더를 가져온다.
        2) (1)에서 가져온 폴더 리스트를 반복하며 그 하위 폴더를 또 가져온다.
        3) 위 과정을 반복하여 일 -> 월 -> 연도 순으로 dat 파일을 읽고 하나로 합친다. (일별 합치고 월별 하비고 연도별로 합침)

    2. dat 파일을 하나로 합치고 컬럼명 바꾸고 csv 로 convert.

    3. DB에 insert. table 이름은 '차량 ID'
"""
import datetime
import os
import numpy as np
import pandas as pd

from tqdm import tqdm  # 작업진행률 표시

year = 2020

# dat_path = f'/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220110_sensor_data/dat_data/{year}'
# csv_path = '/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220110_sensor_data/csv_data'
dat_path = f'/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220502_sensor_data/dat_data'
csv_path = '/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220502_sensor_data/csv_data'


class convert_data_csv:
    def __init__(self, year, dat_path, csv_path):
        self.year = year
        self.now = datetime.datetime.now()
        self.todayDate = self.now.strftime("%Y%m%d")

        # read folder list in dat_data folder
        self.dat_path = dat_path

        # csv file save path
        self.csv_path = csv_path

    def periodic_file(self):
        all_file_path = []

        periodic_file_list = []
        periodic_file_path_list = []
        error_list = []

        # os.walk(path): 경로 안의 폴더를 재귀적 탐색해 경로에 속한 모든 폴더 및 파일의 위치를 읽는다.
        for (root, directories, files) in os.walk(dat_path):
            for d in directories:
                dir_path = os.path.join(root, d)
                # print('Thin is Folder: ', dir_path)

            for file in files:
                file_path = os.path.join(root, file)
                # print('Thin is Folder: ', file_path)

                # 해당 연도 periodic, event 모든 파일 경로
                all_file_path.append(file_path)

                # 각 파일 이름 리스트
                periodic_file_list.append([file for file in files if 'Event' not in file])

                # 각 파일 절대 경로 리스트
                periodic_file_path_list.append([path for path in all_file_path if 'Event' not in path])

        # 2차원 리스트 -> 1차원 리스트
        periodic_file_path_list_all = [element for array in periodic_file_path_list for element in array]

        # 중복 제거 => 2020: periodic, event = 466 / 2021: periodic, event = 668
        periodic_file_path_list_all = list(set(periodic_file_path_list_all))

        # 생성시간 별 파일 read & 컬럼명 변경 & csv로 저장
        for periodic_file_path in periodic_file_path_list_all[:2]:
            file_name = periodic_file_path.split('/')[-1]

            each_file_name = file_name[:-4]  # ex) 20200103112837.dat => 20200103112837
            year = file_name[:4]  # ex) 20200103112837.dat => 2020

            try:
                periodic_data = pd.read_csv(periodic_file_path, sep='\t', skipinitialspace=True, encoding='CP949', skiprows=1, low_memory=False).drop(index=0)

                periodic_data.insert(loc=0, column='code', value=each_file_name)
                periodic_data.insert(loc=1, column='year', value=year)

                periodic_data.columns = ['fileCode', 'year', 'carId', 'operationDate', 'operationTime', 'operationTotalTime', 'time', 'engineState', 'engineSpeed',
                                         'engineTorque', 'demandEngineTorque', 'engineBurden', 'carSpeed', 'acceleratorPedal', 'coolantTemperature', 'engineExhaustGasTemperature',
                                         'engineWarning', 'engineOilPressure', 'engineOilPressureState', 'engineWarmup', 'engineGovernor', 'intakeDustFilter', 'outsideTemperature',
                                         'coolingOilAmount', 'coolantAmount', 'fuelTemperature', 'transmissionOutputSpeed', 'transmissionInputSpeed', 'engineOverrideControlMode',
                                         'overrideDemandTorque', 'automaticTransmission', 'detailedTransmission', 'demandTransmission', 'currentTransmission', 'emergencyTransmissionMode',
                                         'transmissionOilTemperature', 'transmissionOilOverheat', 'coolingFanMode', 'coolingFanIntakeAirTemperature', 'coolingFanCoolantTemperature',
                                         'coolingFanHydraulicValveDutyRatio', 'parkingStatus', 'brake', 'retarderBrake', 'retarderSelection', 'retarderTorque',
                                         'demandRetarderTorque', 'retarderOilTemperature', 'airMaster', 'brakeOil', 'frontBrakePneumatic', 'backBrakePneumatic',
                                         'rightInfantryRoomAirTank', 'leftInfantryRoomAirTank', 'frontAirTank', 'fuelLevel', 'voltage', 'chargeState',
                                         'ABSOperation', 'ABSRoughTerrainMode', 'ABSWarningLamp', 'twoAxisSpeed', 'twoAxisLeftRelativeSpeed',
                                         'twoAxisRightRelativeSpeed', 'threeAxisLeftRelativeSpeed', 'threeAxisRightRelativeSpeed', 'oneAxisDifferentialLock', 'twoAxisDifferentialLock',
                                         'axleDifferential',
                                         'threeAxisDifferentialLock', 'fourAxisDifferentialLock', 'mediumTransmissionMode', 'additionalTransmission_lowSpeedSwitch',
                                         'additionalTransmission_neutralSwitch',
                                         'additionalTransmission_highSpeedSwitch', 'powerPneumatic', 'powerTransmissionMode_8x8', 'powerTransmissionMode_6x6', 'oneAxisWheelPower',
                                         'twoAxisWheelPower', 'threeAxisWheelPower', 'fourAxisWheelPower', 'waterSurfacePropulsion', 'differentialUnlocking', 'oilStorageHydraulicPressure',
                                         'oilStorageHydraulicOilTemperature', 'oilStorageOilAmount', 'oilStorageLowPressureFilter', 'rescueWinchClutch', 'rearDoorOpen', 'positivePressure',
                                         'positivePressureDevice', 'CTIS_controlAirPressure', 'P_BIT_command']

                periodic_data = periodic_data.replace(['-'], np.nan)

                # print(periodic_data)

                # TODO: CSV 데이터 파일 정리, 생성일자로 폴더 만들어서 저장 (생성일자 폴더가 없으면 생성, 있으면 저장)
                def createDirectory(path):
                    try:
                        if not os.path.exists(path):
                            os.makedirs(path)

                            return True

                    except OSError:
                        print("Error: Failed to create the directory.")

                        return False

                # 현재 날짜 폴더 생성
                createDirectory(csv_path + '/' + self.todayDate + '/periodic')

                # 현재 날짜 폴더에 csv 파일 저장
                periodic_data.to_csv(csv_path + '/' + self.todayDate + '/periodic/' + str(each_file_name) + '.csv', sep=',', encoding='CP949', index=False)

            except Exception as e:
                print(e)
                error_list.append(file_name)

                continue

    def event_file(self):
        all_file_path = []

        event_file_list = []
        event_file_path_list = []
        error_list = []

        # os.walk(path): 경로 안의 폴더를 재귀적 탐색해 경로에 속한 모든 폴더 및 파일의 위치를 읽는다.
        for (root, directories, files) in os.walk(dat_path):
            for d in directories:
                dir_path = os.path.join(root, d)
                # print('Thin is Folder: ', dir_path)

            for file in files:
                file_path = os.path.join(root, file)
                # print('Thin is Folder: ', file_path)

                # 해당 연도 periodic, event 모든 파일 경로
                all_file_path.append(file_path)

                # 각 파일 이름 리스트
                event_file_list.append([file for file in files if 'Event' in file])

                # 각 파일 절대 경로 리스트
                event_file_path_list.append([path for path in all_file_path if 'Event' in path])

        # 2차원 리스트 -> 1차원 리스트
        event_file_path_list_all = [element for array in event_file_path_list for element in array]

        # 중복 제거 => 2020: periodic, event = 466 / 2021: periodic, event = 668
        event_file_path_list_all = list(set(event_file_path_list_all))

        # 생성시간 별 파일 read & 컬럼명 변경 & csv로 저장
        for event_file_path in event_file_path_list_all[:2]:
            file_name = event_file_path.split('/')[-1]

            each_file_name = file_name[:-10]  # ex) 20200107132502_Event.dat => 20200107132502
            year = file_name[:4]  # ex) 20200103112837.dat => 2020

            try:
                event_data = pd.read_csv(event_file_path, sep='\t', skipinitialspace=True, encoding='CP949', skiprows=1, low_memory=False).drop(columns='Unnamed: 8')

                event_data.insert(loc=0, column='code', value=each_file_name)
                event_data.insert(loc=1, column='year', value=year)

                event_data.columns = ['fileCode', 'year', 'carId', 'operationDate', 'operationTime', 'operationTotalTime', 'time', 'code', 'message', 'state']

                event_data = event_data.replace(['-'], np.nan)

                # print(event_data)

                # TODO: CSV 데이터 파일 정리, 생성일자로 폴더 만들어서 저장 (생성일자 폴더가 없으면 생성, 있으면 저장)
                def createDirectory(path):
                    try:
                        if not os.path.exists(path):
                            os.makedirs(path)

                            return True

                    except OSError:
                        print("Error: Failed to create the directory.")

                        return False

                # 현재 날짜 폴더 생성
                createDirectory(csv_path + '/' + self.todayDate + '/event')

                # 현재 날짜 폴더에 csv 파일 저장
                event_data.to_csv(csv_path + '/' + self.todayDate + '/event/' + str(each_file_name) + '_Event' + '.csv', sep=',', encoding='CP949', index=False)

            except Exception as e:
                print(e)
                error_list.append(file_name)

                continue

    def merge_periodic_file(self):
        periodic_csv_path_dir = self.csv_path + '/' + self.todayDate + '/periodic/'
        periodic_csv_file_list = os.listdir(periodic_csv_path_dir)

        periodic_df_all = pd.DataFrame()

        for i in tqdm(range(len(periodic_csv_file_list))):
            periodic_path = periodic_csv_path_dir + periodic_csv_file_list[i]

            periodic_df = pd.read_csv(periodic_path, sep=',', skipinitialspace=True, encoding='CP949', low_memory=False)

            periodic_df_all = pd.concat([periodic_df_all, periodic_df]).reset_index(drop=True).drop_duplicates()

        print(periodic_df_all)

        periodic_df_all.to_csv(self.csv_path + '/' + 'all_periodic_merged_data_with_code' + f'_{self.todayDate}' + '.csv', sep=',', encoding='CP949', index=False)

    def merge_event_file(self):
        event_csv_path_dir = self.csv_path + '/' + self.todayDate + '/event/'
        event_csv_file_list = os.listdir(event_csv_path_dir)

        event_df_all = pd.DataFrame()

        for i in tqdm(range(len(event_csv_file_list))):
            event_path = event_csv_path_dir + event_csv_file_list[i]

            event_df = pd.read_csv(event_path, sep=',', skipinitialspace=True, encoding='CP949', low_memory=False)

            event_df_all = pd.concat([event_df_all, event_df]).reset_index(drop=True).drop_duplicates()

        print(event_df_all)

        event_df_all.to_csv(self.csv_path + '/' + 'all_event_merged_data_with_code' + f'_{self.todayDate}' + '.csv', sep=',', encoding='CP949', index=False)


if __name__ == '__main__':
    t = convert_data_csv(year=year, dat_path=dat_path, csv_path=csv_path)

    t.periodic_file()
    t.event_file()
    t.merge_periodic_file()
    t.merge_event_file()



