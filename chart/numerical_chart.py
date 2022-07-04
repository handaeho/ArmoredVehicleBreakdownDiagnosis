from matplotlib import pyplot as plt
import pandas as pd
import os


def day_edit_dataframe(day_dataframe):
    # edited to dataframe shape and all dataframe's 'NaN' value to 0
    day_val_2020 = day_dataframe.iloc[:, 0].values
    day_val_2021 = day_dataframe.iloc[:, 1].values
    day_df = pd.DataFrame([x for x in zip(day_val_2020, day_val_2021)], columns=['2020', '2021']).fillna(value=0)
    day_df.index += 1

    return day_df


def month_edit_dataframe(month_dataframe):
    # edited to dataframe shape and all dataframe's 'NaN' value to 0
    month_val_2020 = month_dataframe.iloc[:, 0].values
    month_val_2021 = month_dataframe.iloc[:, 1].values
    month_df = pd.DataFrame([x for x in zip(month_val_2020, month_val_2021)], columns=['2020', '2021']).fillna(value=0).drop(index=0)

    return month_df


def year_edit_dataframe(year_dataframe):
    # edited to dataframe shape and all dataframe's 'NaN' value to 0
    year_val = [year_dataframe.iloc[0, 0], year_dataframe.iloc[1, 1]]
    year_df = pd.DataFrame([year_val], columns=['2020', '2021']).fillna(value=0)
    year_df.index += 1

    return year_df


def day_charting(save_path, label, df):
    plt.style.use('ggplot')

    df.reset_index().plot(x='index', y=['2020', '2021'], kind='line', figsize=(10, 10))

    plt.xlabel(f'{label}')
    plt.ylabel('values')
    plt.title(f'{label} Result for Day')
    plt.legend(['2020', '2021'])
    # plt.xticks(df['day'])
    plt.grid(True, alpha=0.5, linestyle='--')

    try:
        if not os.path.exists(save_path + f'/{label}'):
            os.makedirs(save_path + f'/{label}')

    except OSError:
        print('Error to create directory. => ' + f'{label}')

    finally:
        # plt.savefig(save_path + f'/{label}' + f'/day_{label}_chart_result.png', bbox_inches='tight')
        plt.show()


def month_charting(save_path, label, df):
    plt.style.use('ggplot')

    df.reset_index().plot(x='index', y=['2020', '2021'], kind='line', figsize=(10, 10))

    plt.xlabel(f'{label}')
    plt.ylabel('values')
    plt.title(f'{label} Result for Month')
    plt.legend(['2020', '2021'])
    # plt.xticks(df['month'])
    plt.grid(True, alpha=0.5, linestyle='--')

    try:
        if not os.path.exists(save_path + f'/{label}'):
            os.makedirs(save_path + f'/{label}')

    except OSError:
        print('Error to create directory. => ' + f'{label}')

    finally:
        # plt.savefig(save_path + f'/{label}' + f'/month_{label}_chart_result.png', bbox_inches='tight')
        plt.show()


def year_charting(save_path, label, df):
    plt.style.use('ggplot')

    df.reset_index().plot(x='index', y=['2020', '2021'], kind='bar', figsize=(10, 10))

    plt.xlabel(f'{label}')
    plt.ylabel('values')
    plt.title(f'{label} Result for Year')
    plt.legend(['2020', '2021'])
    # plt.xticks(df['year'])
    plt.grid(True, alpha=0.5, linestyle='--')

    try:
        if not os.path.exists(save_path + f'/{label}'):
            os.makedirs(save_path + f'/{label}')

    except OSError:
        print('Error to create directory. => ' + f'{label}')

    finally:
        # plt.savefig(save_path + f'/{label}' + f'/year_{label}_chart_result.png', bbox_inches='tight')
        plt.show()


if __name__ == '__main__':
    groupBy_path = '/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220110_sensor_data/groupBy_data'

    chart_save_path = '/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/chart/result/numerical_chart'

    day_file_list = os.listdir(groupBy_path + '/day' + '/numerical')
    month_file_list = os.listdir(groupBy_path + '/month' + '/numerical')
    year_file_list = os.listdir(groupBy_path + '/year' + '/numerical')

    for day_file_name in day_file_list[:2]:
        label_name = day_file_name.split('_')[1]

        day = pd.read_csv(groupBy_path + '/day' + '/numerical' + f'/{day_file_name}', encoding='CP949', index_col=0)

        day_df = day_edit_dataframe(day_dataframe=day)

        day_charting(save_path=chart_save_path, label=label_name, df=day_df)

    for month_file_name in month_file_list[:2]:
        label_name = month_file_name.split('_')[1]

        month = pd.read_csv(groupBy_path + '/month' + '/numerical' + f'/{month_file_name}', encoding='CP949', index_col=0)

        month_df = month_edit_dataframe(month_dataframe=month)

        month_charting(save_path=chart_save_path, label=label_name, df=month_df)

    for year_file_name in year_file_list[:2]:
        label_name = year_file_name.split('_')[1]

        year = pd.read_csv(groupBy_path + '/year' + '/numerical' + f'/{year_file_name}', encoding='CP949', index_col=0)

        year_df = year_edit_dataframe(year_dataframe=year)

        year_charting(save_path=chart_save_path, label=label_name, df=year_df)





