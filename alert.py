import telegram
from datetime import datetime, timedelta, date
import requests
import pandas as pd
import pandahouse as ph
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.pyplot import figure
import sys
import os
import io

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context



# defaults
default_args = {
    'owner': '####',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2023, 2, 1)
}


# connection ClickHouse
connection = {'host': 'https://####',
                      'database': '####',
                      'user': '####',
                      'password': '####'
                      }


# DAG start interval
schedule_interval = '*/15 * * * *'


# bot and chat information
chat_id = ####
my_token = '####'
bot = telegram.Bot(token=my_token)

#query for metrics

q = """
    SELECT *
    FROM (
        SELECT
            toStartOfFifteenMinutes(time) as ts,
            toDate(time) as date,
            formatDateTime(ts, '%R') as hs,
            count(DISTINCT user_id) AS users_feed,
            countIf(user_id, action='view') as views,
            countIf(user_id, action='like') as likes,
            countIf(user_id, action = 'like') / countIf(user_id, action = 'view') AS ctr
        FROM
            {db}.feed_actions
        WHERE 
            time >= today() - 1 and time < toStartOfFifteenMinutes(now())
        GROUP BY 
            ts, 
            date, 
            hs
        ) AS feed
        JOIN (
        SELECT
            toStartOfFifteenMinutes(time) as ts,
            toDate(time) as date,
            formatDateTime(ts, '%R') as hs,
            uniqExact(user_id) AS users_messenger,
            count(user_id) as messages_sent
        FROM 
            {db}.message_actions
        WHERE 
            time >= today() - 1 and time < toStartOfFifteenMinutes(now())
        GROUP BY 
            ts,
            date,
            hs
        ORDER BY date, hs
        ) AS message 
            USING ts, date, hs  
        """
# function of checking anomaly
def check_anomaly(df, metric, a=3, n=5):
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a * df['iqr']
    df['down'] = df['q25'] - a * df['iqr']
    
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['down'] = df['down'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['down'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df

        #dag alerts
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def krupen_dag_alert():
    
    @task()
    def get_data():
        result = ph.read_clickhouse(q, connection=connection)
        return result  
    
    @task()
    def run_alerts(result):
        metrics = ['users_feed', 'views', 'likes', 'ctr', 'users_messenger', 'messages_sent']
        for metric in metrics:
            print(metric)
            df = result[['ts', 'date', 'hs', metric]].copy()

            is_alert, df = check_anomaly(df, metric)

            if is_alert == 1:
                msg = '''Метрика {metric}:\n текущее значение {current_value:.2f}\n отклонение от предыдущего значения {last_value_diff:.2%}'''.format(metric=metric, current_value=df[metric].iloc[-1], last_value_diff=abs(1-(df[metric].iloc[-1]/df[metric].iloc[-2])))            

                bot.sendMessage(chat_id=chat_id, text=msg)


                sns.set(rc={'figure.figsize': (16, 20)}) # set chart size
                plt.tight_layout()
                ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric')
                ax = sns.lineplot(x=df['ts'], y=df['up'], label='up')
                ax = sns.lineplot(x=df['ts'], y=df['down'], label='down')    

                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)


                ax.set(xlabel='time')
                ax.set(ylabel=metric)

                ax.set_title(metric)
                ax.set(ylim=(0, None))

                # create a file object
                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = f'{metric}.png'
                plt.close()

                bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    result = get_data()
    run_alerts(result)
    
krupen_dag_alert = krupen_dag_alert()    
    
        