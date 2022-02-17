import numpy as np
from matplotlib import pyplot as plt
import pandas as pd
import os


def day_edit_dataframe(day_dataframe):
    # 분류 기준
    sort_criteria = list(map(int, set(day_dataframe[f'{label_name}'].values)))

    # 분류 기준별 dataframe 분할
    df_list_2020 = []
    df_list_2021 = []

    for s_name in sort_criteria:
        globals()['key_{}'.format(s_name)] = pd.DataFrame(columns=['day', f'{label_name}', f'2020_{label_name}', f'2021_{label_name}'])

        for i in range(len(day_dataframe)):
            if day_dataframe[f'{label_name}'][i] == s_name:
                globals()['key_{}'.format(s_name)] = globals()['key_{}'.format(s_name)].append(
                    pd.DataFrame([[day_dataframe['day'][i], day_dataframe[f'{label_name}'][i], day_dataframe[f'2020_{label_name}'][i], day_dataframe[f'2021_{label_name}'][i]]],
                                 columns=['day', f'{label_name}', f'2020_{label_name}', f'2021_{label_name}'])
                )

        df_list_2020.append(globals()['key_{}'.format(s_name)][['day', f'{label_name}', f'2020_{label_name}']])
        df_list_2021.append(globals()['key_{}'.format(s_name)][['day', f'{label_name}', f'2021_{label_name}']])

    # 연도별 merge
    df_2020 = pd.DataFrame(columns=['day', f'{label_name}', f'2020_{label_name}'])
    df_2021 = pd.DataFrame(columns=['day', f'{label_name}', f'2021_{label_name}'])

    for x_2020 in range(len(df_list_2020)):
        df_2020 = pd.concat([df_2020, df_list_2020[x_2020]], axis=0).reset_index(drop=True).astype('int')

    for x_2021 in range(len(df_list_2020)):
        df_2021 = pd.concat([df_2021, df_list_2021[x_2021]], axis=0).reset_index(drop=True).astype('int')

    # print(df_2020)
    # print(df_2021)

    return df_2020, df_2021


def month_edit_dataframe(month_dataframe):
    # 분류 기준
    sort_criteria = list(map(int, set(month_dataframe[f'{label_name}'].values)))

    # 분류 기준별 dataframe 분할
    df_list_2020 = []
    df_list_2021 = []

    for s_name in sort_criteria:
        globals()['key_{}'.format(s_name)] = pd.DataFrame(columns=['month', f'{label_name}', f'2020_{label_name}', f'2021_{label_name}'])

        for i in range(len(month_dataframe)):
            if month_dataframe[f'{label_name}'][i] == s_name:
                globals()['key_{}'.format(s_name)] = globals()['key_{}'.format(s_name)].append(
                    pd.DataFrame([[month_dataframe['month'][i], month_dataframe[f'{label_name}'][i], month_dataframe[f'2020_{label_name}'][i], month_dataframe[f'2021_{label_name}'][i]]],
                                 columns=['month', f'{label_name}', f'2020_{label_name}', f'2021_{label_name}'])
                )

        df_list_2020.append(globals()['key_{}'.format(s_name)][['month', f'{label_name}', f'2020_{label_name}']])
        df_list_2021.append(globals()['key_{}'.format(s_name)][['month', f'{label_name}', f'2021_{label_name}']])

    # 연도별 merge
    df_2020 = pd.DataFrame(columns=['month', f'{label_name}', f'2020_{label_name}'])
    df_2021 = pd.DataFrame(columns=['month', f'{label_name}', f'2021_{label_name}'])

    for x_2020 in range(len(df_list_2020)):
        df_2020 = pd.concat([df_2020, df_list_2020[x_2020]], axis=0).reset_index(drop=True).astype('int')

    for x_2021 in range(len(df_list_2020)):
        df_2021 = pd.concat([df_2021, df_list_2021[x_2021]], axis=0).reset_index(drop=True).astype('int')

    month_zero_index_2020 = df_2020[df_2020['month'] == 0].index
    month_zero_index_2021 = df_2021[df_2021['month'] == 0].index

    df_2020 = df_2020.drop(month_zero_index_2020).reset_index(drop=True)
    df_2021 = df_2021.drop(month_zero_index_2021).reset_index(drop=True)

    # print(df_2020)
    # print(df_2021)

    return df_2020, df_2021


def year_edit_dataframe(year_dataframe):
    # 분류 기준
    sort_criteria = list(map(int, set(year_dataframe[f'{label_name}'].values)))

    # 분류 기준별 dataframe 분할
    df_list_2020 = []
    df_list_2021 = []

    for s_name in sort_criteria:
        globals()['key_{}'.format(s_name)] = pd.DataFrame(columns=['year', f'{label_name}', f'2020_{label_name}', f'2021_{label_name}'])

        for i in range(len(year_dataframe)):
            if year_dataframe[f'{label_name}'][i] == s_name:
                globals()['key_{}'.format(s_name)] = globals()['key_{}'.format(s_name)].append(
                    pd.DataFrame([[year_dataframe['year'][i], year_dataframe[f'{label_name}'][i], year_dataframe[f'2020_{label_name}'][i], year_dataframe[f'2021_{label_name}'][i]]],
                                 columns=['year', f'{label_name}', f'2020_{label_name}', f'2021_{label_name}'])
                )

        df_list_2020.append(globals()['key_{}'.format(s_name)][['year', f'{label_name}', f'2020_{label_name}']])
        df_list_2021.append(globals()['key_{}'.format(s_name)][['year', f'{label_name}', f'2021_{label_name}']])

    # 연도별 merge
    df_2020 = pd.DataFrame(columns=['year', f'{label_name}', f'2020_{label_name}'])
    df_2021 = pd.DataFrame(columns=['year', f'{label_name}', f'2021_{label_name}'])

    for x_2020 in range(len(df_list_2020)):
        df_2020 = pd.concat([df_2020, df_list_2020[x_2020]], axis=0).reset_index(drop=True).astype('int')

    for x_2021 in range(len(df_list_2020)):
        df_2021 = pd.concat([df_2021, df_list_2021[x_2021]], axis=0).reset_index(drop=True).astype('int')

    year_not_2020 = df_2020[df_2020['year'] == 2021].index
    year_not_2021 = df_2021[df_2021['year'] == 2020].index

    df_2020_edit = df_2020[[f'{label_name}', f'2020_{label_name}']].drop(year_not_2020).reset_index(drop=True)
    df_2021_edit = df_2021[[f'{label_name}', f'2021_{label_name}']].drop(year_not_2021).reset_index(drop=True)

    df_2020_2021 = pd.merge(df_2020_edit, df_2021_edit)

    # print(df_2020_2021)

    return df_2020_2021


def day_charting(save_path, label, dataframe_2020, dataframe_2021):
    plt.style.use('ggplot')

    # line graphs with dataframe
    groups_2020 = list(set(dataframe_2020[f'{label_name}']))
    groups_2021 = list(set(dataframe_2021[f'{label_name}']))

    plt.figure(figsize=(10, 10))

    for group_name in groups_2020:
        # sub-setting
        df_sub = dataframe_2020[dataframe_2020[f'{label_name}'] == group_name]

        # plotting
        plt.subplot(2, 1, 1)
        plt.title(f'2020 {label_name} Result for Day')
        plt.plot(df_sub['day'], df_sub[f'2020_{label_name}'])
        plt.legend(groups_2020, fontsize=10, loc='best')

    for group_name in groups_2021:
        # sub-setting
        df_sub = dataframe_2021[dataframe_2021[f'{label_name}'] == group_name]

        # plotting
        plt.subplot(2, 1, 2)
        plt.title(f'2021 {label_name} Result for Day')
        plt.plot(df_sub['day'], df_sub[f'2021_{label_name}'])
        plt.legend(groups_2021, fontsize=10, loc='best')

    try:
        if not os.path.exists(save_path + f'/{label}'):
            os.makedirs(save_path + f'/{label}')

    except OSError:
        print('Error to create directory. => ' + f'{label}')

    finally:
        plt.savefig(save_path + f'/{label}' + f'/day_{label}_chart_result.png', bbox_inches='tight')
        # plt.show()


def month_charting(save_path, label, dataframe_2020, dataframe_2021):
    plt.style.use('ggplot')

    # line graphs with dataframe
    groups_2020 = list(set(dataframe_2020[f'{label_name}']))
    groups_2021 = list(set(dataframe_2021[f'{label_name}']))

    plt.figure(figsize=(10, 10))

    for group_name in groups_2020:
        # sub-setting
        df_sub = dataframe_2020[dataframe_2020[f'{label_name}'] == group_name]

        # plotting
        plt.subplot(2, 1, 1)
        plt.title(f'2020 {label_name} Result for Month')
        plt.plot(df_sub['month'], df_sub[f'2020_{label_name}'])
        plt.legend(groups_2020, fontsize=10, loc='best')

    for group_name in groups_2021:
        # sub-setting
        df_sub = dataframe_2021[dataframe_2021[f'{label_name}'] == group_name]

        # plotting
        plt.subplot(2, 1, 2)
        plt.title(f'2021 {label_name} Result for Month')
        plt.plot(df_sub['month'], df_sub[f'2021_{label_name}'])
        plt.legend(groups_2021, fontsize=10, loc='best')

    try:
        if not os.path.exists(save_path + f'/{label}'):
            os.makedirs(save_path + f'/{label}')

    except OSError:
        print('Error to create directory. => ' + f'{label}')

    finally:
        plt.savefig(save_path + f'/{label}' + f'/month_{label}_chart_result.png', bbox_inches='tight')
        # plt.show()


def year_charting(save_path, label, dataframe_2020_2021):
    plt.style.use('ggplot')

    dataframe_2020_2021.plot(f'{label}', [f'2020_{label_name}', f'2021_{label_name}'], kind='bar', figsize=(10, 10))

    plt.title(f'2020 / 2021 {label_name} Result for Year')
    plt.legend(fontsize=10, loc='best')

    try:
        if not os.path.exists(save_path + f'/{label}'):
            os.makedirs(save_path + f'/{label}')

    except OSError:
        print('Error to create directory. => ' + f'{label}')

    finally:
        plt.savefig(save_path + f'/{label}' + f'/year_{label}_chart_result.png', bbox_inches='tight')
        # plt.show()


if __name__ == '__main__':
    groupBy_path = '/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220110_sensor_data/groupBy_data'

    chart_save_path = '/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/chart/result/categorical_chart'

    day_file_list = os.listdir(groupBy_path + '/day' + '/categorical')
    month_file_list = os.listdir(groupBy_path + '/month' + '/categorical')
    year_file_list = os.listdir(groupBy_path + '/year' + '/categorical')

    for index, day_file_name in enumerate(day_file_list):
        if not day_file_name == '.ipynb_checkpoints':
            print(f'Day {day_file_name} Start ===>  {index} / {len(day_file_list)}')

            label_name = day_file_name.split('_')[1]

            day = pd.read_csv(groupBy_path + '/day' + '/categorical' + f'/{day_file_name}', encoding='CP949', index_col=0).reset_index('day').fillna(0)

            df_2020, df_2021 = day_edit_dataframe(day_dataframe=day)

            day_charting(save_path=chart_save_path, label=label_name, dataframe_2020=df_2020, dataframe_2021=df_2021)

            print(f'Day {day_file_name} Done ===>  {index} / {len(day_file_list)}')

        else:
            pass

    for index, month_file_name in enumerate(month_file_list):
        if not month_file_name == '.ipynb_checkpoints':
            print(f'Month {month_file_name} Start ===>  {index} / {len(month_file_list)}')

            label_name = month_file_name.split('_')[1]

            month = pd.read_csv(groupBy_path + '/month' + '/categorical' + f'/{month_file_name}', encoding='CP949', index_col=0).reset_index('month').fillna(0)

            df_2020, df_2021 = month_edit_dataframe(month_dataframe=month)

            month_charting(save_path=chart_save_path, label=label_name, dataframe_2020=df_2020, dataframe_2021=df_2021)

            print(f'Month {month_file_name} Done ===>  {index} / {len(month_file_list)}')

        else:
            pass

    for index, year_file_name in enumerate(year_file_list):
        if not year_file_name == '.ipynb_checkpoints':
            print(f'Year {year_file_name} Start ===>  {index} / {len(year_file_list)}')

            label_name = year_file_name.split('_')[1]

            year = pd.read_csv(groupBy_path + '/year' + '/categorical' + f'/{year_file_name}', encoding='CP949', index_col=0).reset_index('year').fillna(0)

            df_2020_2021 = year_edit_dataframe(year_dataframe=year)

            year_charting(save_path=chart_save_path, label=label_name, dataframe_2020_2021=df_2020_2021)

            print(f'Year {year_file_name} Done ===>  {index} / {len(year_file_list)}')

        else:
            pass

















