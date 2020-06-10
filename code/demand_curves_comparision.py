# Libraries needed are Plotly and Pandas
# Plotly go can be installed using this command: pip install plotly==4.8.1
# Pandas can be installed with: pip install pandas

import pandas as pd
from dateutil.parser import parse
import plotly.graph_objects as go


# Read in the data for Germany and Spain into Pandas dataframes
def read_data(file_name):
    """
    :param file_name: input the name of the csv data file name as string
    :return: pandas dataframe
    """

    file_path = '../all_data/'
    return pd.read_csv(file_path + file_name)


seasons = ('summer', 'transition period', 'winter')

for season in seasons:
    if season == 'summer':
        de_summer_workday = read_data('sum_workday_data.csv')
        de_summer_saturday = read_data('sum_sat_data.csv')
        de_summer_sunday = read_data('sum_sun_data.csv')
        es_summer_data = read_data('red_elec_summer.csv')

    elif season == 'transition period':
        de_tp_workday = read_data('tp_workday_data.csv')
        de_tp_saturday = read_data('tp_sat_data.csv')
        de_tp_sunday = read_data('tp_sun_data.csv')
        es_tp_spring_data = read_data('red_elec_spring.csv')
        es_tp_autumn_data = read_data('red_elec_autumn.csv')
    elif season == 'winter':
        de_winter_workday = read_data('win_workday_data.csv')
        de_winter_saturday = read_data('win_sat_data.csv')
        de_winter_sunday = read_data('win_sun_data.csv')
        es_winter_data = read_data('red_elec_winter.csv')

# Resample the German utilities' time resolution from 15-min intervals to hourly
de_dfs = [de_summer_workday, de_summer_saturday, de_summer_sunday,
          de_tp_workday, de_tp_saturday, de_tp_saturday, de_winter_workday, de_winter_saturday, de_winter_sunday]


# print(de_summer_workday.shape)
# print(de_summer_workday.head())
# print('=========================')

def to_hourly(df_name):
    """
    :param df_name: pandas dataframe
    :return: pandas dataframe with 24 rows
    """
    # Initialize lists to hold the hourly values for each utility
    bdew_hourly_values = []
    ed_netze_hourly_values = []

    list_rel_columns = ['bdew', 'ed_netze']

    # Loop to sum the four 15-min interval demand values into hourly demand
    for utility in list_rel_columns:
        for row in range(0, len(df_name), 4):
            i = 0
            sumof = 0
            while i < 4:
                sumof = sumof + df_name.iloc[row + i][utility]
                i = i + 1
            if utility == 'bdew':
                bdew_hourly_values.append(sumof)
            else:
                ed_netze_hourly_values.append(sumof)

    hours = list(range(1, 25))

    df_name = pd.DataFrame({'hour': hours, 'bdew': bdew_hourly_values, 'ed_netze': ed_netze_hourly_values})

    return df_name


# Convert the timestamps to Pandas timestamps format
def conv_times(datetimestr):
    """
    Function to convert the timestamp in the CSV files obtained from Red Electrica
    to a Pandas accepted timestamp.

    Argument: the timestamp in the DF as a string
    Returns a pandas accepted timestamp
    """
    date = parse(datetimestr)
    return date.strftime("%Y-%m-%d-%H")


# Read in the spanish seasonal data
# Drop unncessary columns (id and name)
# Apply the conv_times function on the datetime column
# Apply the conversion to hourly resolution in the datetime column
# Then according to the weekday, filter the data for the relevant data
# Now, average the data across each hour of the day to obtain the typical day data

# Function to filter the data
def which_day(df_name3, week_day):
    """
    :param df_name3: pandas dataframe with all days in a season
    :param week_day: either 'workday' or 'saturday' or 'sunday'
    :return: pandas dataframe with demand data for specific days in a season
    """
    working_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']

    if week_day == 'workday':
        df_name3 = df_name3[(df_name3['datetime'].dt.day_name()).isin(working_days) == True]
    elif week_day == 'saturday':
        df_name3 = df_name3[df_name3['datetime'].dt.day_name() == 'Saturday']
    elif week_day == 'sunday':
        df_name3 = df_name3[df_name3['datetime'].dt.day_name() == 'Sunday']

    return df_name3


# Function to get the hour time stamp

def hourofday(timestamp):
    return timestamp.hour


# Shift the Spanish demand time series data by moving it two hours back

def shift_ts(df_shift):
    """
    :param df_shift: pandas dataframe with the spanish hourly demand data
    :return: pandas dataframe with demand data shifted by two hours
    """
    new_23 = df_shift.at[0, 'red_electrica']
    new_24 = df_shift.at[1, 'red_electrica']

    df_shift['red_electrica'] = df_shift['red_electrica'].shift(-2)

    df_shift.at[22, 'red_electrica'] = new_23
    df_shift.at[23, 'red_electrica'] = new_24

    return df_shift

# Function to calculate the hourly values

def create_df_spanish(df_name4):
    df_name4['hour'] = df_name4['datetime'].apply(hourofday)

    grouped = df_name4.groupby('hour')
    hourly_values = {}

    for ganta in range(0, 24):
        df = (grouped.get_group(ganta)).copy()
        avg_val = df['value'].mean()
        hourly_values.update({ganta: avg_val})

    removed = hourly_values.pop(0)
    hourly_values.update({24: removed})

    df_name5 = pd.DataFrame.from_dict(hourly_values, orient='index', columns=['red_electrica'])

    df_name5.index.name = 'hour'
    df_name5.reset_index(inplace=True)
    df_name5 = shift_ts(df_name5)

    return df_name5


# Reformat the Spanish data to appropriate pandas datetime style and filter the days
def spanish_data_formatting(df_name2, day_of_week):
    df_name2 = df_name2.drop(columns=['id', 'name'])
    df_name2['datetime'] = df_name2['datetime'].apply(conv_times)
    df_name2['datetime'] = pd.to_datetime(df_name2['datetime'], format='%Y-%m-%d-%H')

    df_name2 = which_day(df_name2, day_of_week)

    df_name2 = create_df_spanish(df_name2)

    return df_name2


for season_plots in seasons:
    if season_plots == 'summer':
        de_summer_workday_hourly = to_hourly(de_summer_workday)
        de_summer_saturday_hourly = to_hourly(de_summer_saturday)
        de_summer_sunday_hourly = to_hourly(de_summer_sunday)
        es_summer_workday = spanish_data_formatting(es_summer_data, 'workday')
        es_summer_saturday = spanish_data_formatting(es_summer_data, 'saturday')
        es_summer_sunday = spanish_data_formatting(es_summer_data, 'sunday')
    elif season_plots == 'winter':
        de_winter_workday_hourly = to_hourly(de_winter_workday)
        de_winter_saturday_hourly = to_hourly(de_winter_saturday)
        de_winter_sunday_hourly = to_hourly(de_winter_sunday)
        es_winter_workday_hourly = spanish_data_formatting(es_winter_data, 'workday')
        es_winter_saturday_hourly = spanish_data_formatting(es_winter_data, 'saturday')
        es_winter_sunday_hourly = spanish_data_formatting(es_winter_data, 'sunday')
    elif season_plots == 'transition period':
        de_tp_workday_hourly = to_hourly(de_tp_workday)
        de_tp_saturday_hourly = to_hourly(de_tp_saturday)
        de_tp_sunday_hourly = to_hourly(de_tp_sunday)

        frames = [es_tp_spring_data, es_tp_autumn_data]
        es_tp_data = pd.concat(frames)

        es_tp_workday_hourly = spanish_data_formatting(es_tp_data, 'workday')
        es_tp_saturday_hourly = spanish_data_formatting(es_tp_data, 'saturday')
        es_tp_sunday_hourly = spanish_data_formatting(es_tp_data, 'sunday')


def combine_de_es(df1, df2):
    df3 = pd.merge(df1, df2, on='hour')

    utils = list(df3.columns)
    utils.remove('hour')

    for utility in utils:
        df3[utility] = df3[utility].div(df3[utility].max(axis=0))

    df3 = df3.round(3)
    return df3


# loop to combine the German and Spanish data
for season_dfs in seasons:
    if season_dfs == 'summer':
        summer_workday = combine_de_es(de_summer_workday_hourly, es_summer_workday)
        summer_saturday = combine_de_es(de_summer_saturday_hourly, es_summer_saturday)
        summer_sunday = combine_de_es(de_summer_sunday_hourly, es_summer_sunday)
    elif season_dfs == 'winter':
        winter_workday = combine_de_es(de_winter_workday_hourly, es_winter_workday_hourly)
        winter_saturday = combine_de_es(de_winter_saturday_hourly, es_winter_saturday_hourly)
        winter_sunday = combine_de_es(de_winter_sunday_hourly, es_winter_sunday_hourly)
    elif season_dfs == 'transition period':
        transition_period_workday = combine_de_es(de_tp_workday_hourly, es_tp_workday_hourly)
        transition_period_saturday = combine_de_es(de_tp_saturday_hourly, es_tp_saturday_hourly)
        transition_period_sunday = combine_de_es(de_tp_sunday_hourly, es_tp_sunday_hourly)


def plot_demand(df_season_name, title_of_graph, fig_name):
    fig = go.Figure(
        data=[
            go.Scatter(x=df_season_name['hour'],
                       y=df_season_name['bdew'],
                       name='BDEW (Germany)',
                       marker=dict(symbol="circle", size=10),
                       mode='lines+markers',
                       line=dict(color='#018a16', width=4)),
            go.Scatter(x=df_season_name['hour'],
                       y=df_season_name['ed_netze'],
                       name='ED Netze GmbH (Germany)',
                       marker=dict(symbol="diamond", size=10),
                       mode='lines+markers',
                       line=dict(color='#aa02f2', width=4)),
            go.Scatter(x=df_season_name['hour'],
                       y=df_season_name['red_electrica'],
                       name='Red Electrica (Spain)',
                       marker=dict(symbol="star", size=10),
                       mode='lines+markers',
                       line=dict(color='#f20202', width=4))
        ])

    fig.update_layout(title=dict(text=title_of_graph, x=0.5, xref='paper', font=dict(family="Arial",
                                                                                     size=24, color='#090380')),
                      legend=dict(bordercolor='#0c085c', borderwidth=1, y=0.75),
                      xaxis=dict(type='category', showticklabels=True, showgrid=True, ticks='inside', showline=True,
                                 linecolor='#000000', gridcolor='#968f8f', gridwidth=0.01,
                                 title=dict(text='Hour of the day', font=dict(family="Balto",
                                                                              size=18, color='#702102'))),
                      yaxis=dict(title=dict(text='Demand (normalized)', font=dict(family="Balto",
                                                                                  size=18,
                                                                                  color='#702102')),
                                 ticks='inside', showline=True, linecolor='#000000',
                                 showgrid=True, gridcolor='#968f8f',
                                 gridwidth=0.01, showticklabels=True),
                      template='xgridoff')

    # Export the plot as a HTML file to be viewed in the browser
    path = '../plots/' + fig_name
    return fig.write_html(path)


# Tuple containing the three different types of days considered for plotting the curves
days = ('workingday', 'saturday', 'sunday')

# Loop to generate the nine different plots
for season_now in seasons:
    for day in days:
        if season_now == 'summer' and day == 'workingday':
            title = "Electricity Demand on a Typical Summer Working Day"
            image_file_name = 'summer_work_day.html'
            plot_demand(summer_workday, title, image_file_name)
        elif season_now == 'transition period' and day == 'workingday':
            title = "Electricity Demand on a Typical Spring/Autumn Working Day"
            image_file_name = 'transition_period_work_day.html'
            plot_demand(transition_period_workday, title, image_file_name)
        elif season_now == 'winter' and day == 'workingday':
            title = "Electricity Demand on a Winter Working Day"
            image_file_name = 'winter_work_day.html'
            plot_demand(winter_workday, title, image_file_name)
        elif season_now == 'summer' and day == 'saturday':
            title = "Electricity Demand on a Typical Summer Saturday"
            image_file_name = 'summer_saturday.html'
            plot_demand(summer_saturday, title, image_file_name)
        elif season_now == 'transition period' and day == 'saturday':
            title = "Electricity Demand on a Typical Autumn/Spring Saturday"
            image_file_name = 'transition_period_saturday.html'
            plot_demand(transition_period_saturday, title, image_file_name)
        elif season_now == 'winter' and day == 'saturday':
            title = "Electricity Demand on a Typical Winter Saturday"
            image_file_name = 'winter_saturday.html'
            plot_demand(winter_saturday, title, image_file_name)
        elif season_now == 'summer' and day == 'sunday':
            title = "Electricity Demand on a Typical Summer Sunday"
            image_file_name = 'summer_sunday.html'
            plot_demand(summer_sunday, title, image_file_name)
        elif season_now == 'transition period' and day == 'sunday':
            title = "Electricity Demand on a Typical Spring/Autumn Sunday"
            image_file_name = 'transition_period_sunday.html'
            plot_demand(transition_period_sunday, title, image_file_name)
        elif season_now == 'winter' and day == 'sunday':
            title = "Electricity Demand on a Typical Winter Sunday"
            image_file_name = 'winter_sunday.html'
            plot_demand(winter_sunday, title, image_file_name)
