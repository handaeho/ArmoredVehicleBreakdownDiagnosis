import pandas as pd
import numpy as np
import os

# 연도
year = 2022

# read periodic file list in periodic folder
periodic_dat_path_dir = 'D:\\PythonProjects\\ArmoredVehicleBreakdownDiagnosis\\sensor_data\\220110_sensor_data\\dat_data\\{year}\\periodic\\'.format(year=year)
periodic_dat_file_list = os.listdir(periodic_dat_path_dir)
print(periodic_dat_file_list)

periodic_csv_path_dir = 'D:\\PythonProjects\\ArmoredVehicleBreakdownDiagnosis\\sensor_data\\220110_sensor_data\\csv_data\\{year}\\periodic\\'.format(year=year)

# read event file list in event folder
event_dat_path_dir = 'D:\\PythonProjects\\ArmoredVehicleBreakdownDiagnosis\\sensor_data\\220110_sensor_data\\dat_data\\{year}\\event\\'.format(year=year)
event_dat_file_list = os.listdir(event_dat_path_dir)
print(event_dat_file_list)

event_csv_path_dir = 'D:\\PythonProjects\\ArmoredVehicleBreakdownDiagnosis\\sensor_data\\220110_sensor_data\\csv_data\\{year}\\event\\'.format(year=year)

error_list = []

for periodic_file_name in periodic_dat_file_list:
    path = periodic_dat_path_dir + periodic_file_name

    try:
        periodic_data = pd.read_csv(path, sep='\t', skipinitialspace=True, encoding='CP949', skiprows=1, low_memory=False).drop(index=0)

        periodic_data.insert(loc=0, column='year', value=year)

        periodic_data.columns = ['year', 'carId', 'operationDate', 'operationTime', 'operationTotalTime', 'time', 'engineState', 'engineSpeed',
                                 'engineTorque', 'demandEngineTorque', 'engineBurden', 'carSpeed', 'acceleratorPedal', 'coolantTemperature', 'engineExhaustGasTemperature',
                                 'engineWarning', 'engineOilPressure', 'engineOilPressureState', 'engineWarmup', 'engineGovernor', 'intakeDustFilter', 'outsideTemperature',
                                 'coolingOilAmount', 'coolantAmount', 'fuelTemperature', 'transmissionOutputSpeed', 'transmissionInputSpeed', 'engineOverrideControlMode',
                                 'overrideDemandTorque', 'automaticTransmission', 'detailedTransmission', 'demandTransmission', 'currentTransmission', 'emergencyTransmissionMode',
                                 'transmissionOilTemperature', 'transmissionOilOverheat', 'coolingFanMode', 'coolingFanIntakeAirTemperature', 'coolingFanCoolantTemperature',
                                 'coolingFanHydraulicValveDutyRatio', 'parkingStatus', 'brake', 'retarderBrake', 'retarderSelection', 'retarderTorque',
                                 'demandRetarderTorque', 'retarderOilTemperature', 'airMaster', 'brakeOil', 'frontBrakePneumatic', 'backBrakePneumatic',
                                 'rightInfantryRoomAirTank', 'leftInfantryRoomAirTank', 'frontAirTank', 'fuelLevel', 'voltage', 'chargeState',
                                 'ABSOperation', 'ABSRoughTerrainMode', 'ABSWarningLamp', 'twoAxisSpeed', 'twoAxisLeftRelativeSpeed',
                                 'twoAxisRightRelativeSpeed', 'threeAxisLeftRelativeSpeed', 'threeAxisRightRelativeSpeed', 'oneAxisDifferentialLock', 'twoAxisDifferentialLock', 'axleDifferential',
                                 'threeAxisDifferentialLock', 'fourAxisDifferentialLock', 'mediumTransmissionMode', 'additionalTransmission_lowSpeedSwitch', 'additionalTransmission_neutralSwitch',
                                 'additionalTransmission_highSpeedSwitch', 'powerPneumatic', 'powerTransmissionMode_8x8', 'powerTransmissionMode_6x6', 'oneAxisWheelPower',
                                 'twoAxisWheelPower', 'threeAxisWheelPower', 'fourAxisWheelPower', 'waterSurfacePropulsion', 'differentialUnlocking', 'oilStorageHydraulicPressure',
                                 'oilStorageHydraulicOilTemperature', 'oilStorageOilAmount', 'oilStorageLowPressureFilter', 'rescueWinchClutch', 'rearDoorOpen', 'positivePressure',
                                 'positivePressureDevice', 'CTIS_controlAirPressure', 'P_BIT_command']

        periodic_data = periodic_data.replace(['-'], np.nan)

        # print(periodic_data)

        periodic_data.to_csv(periodic_csv_path_dir + periodic_file_name[:-4] + '.csv', sep=',', encoding='CP949', index=False)

    except Exception as e:
        print(e)
        error_list.append(periodic_file_name)

        continue

for event_file_name in event_dat_file_list:
    path = event_dat_path_dir + event_file_name

    try:
        event_data = pd.read_csv(path, sep='\t', skipinitialspace=True, encoding='CP949', skiprows=1, low_memory=False).drop(columns='Unnamed: 8')

        event_data.insert(loc=0, column='year', value=year)

        event_data.columns = ['year', 'carId', 'operationDate', 'operationTime', 'operationTotalTime', 'time', 'code', 'message', 'state']

        event_data = event_data.replace(['-'], np.nan)

        # print(event_data)

        event_data.to_csv(event_csv_path_dir + event_file_name[:-10] + '_Event' + '.csv', sep=',', encoding='CP949', index=False)

    except Exception as e:
        print(e)
        error_list.append(event_file_name)

        continue

print(error_list)


