import pandahouse
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import iqr
import telegram
import io
from datetime import datetime, timedelta, date
###
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context #

default_args = {
    'owner':'nakokhov_rd',
    'depends_on_past':False,
    'retries':2,
    'retry_delay':timedelta(minutes=5),
    'start_date':datetime(2022, 8, 8)
}

schedule_interval = '*/15 * * * *'

bot_token = '5389713558:AAHbSHO0Q9VvP-UwGEWrkjv7DvFkIcxZtYc'
bot = telegram.Bot(token=bot_token)

chat_id = -788021703

connection = {
'host': 'https://clickhouse.lab.karpov.courses',
'password': 'dpo_python_2020',
'user': 'student',
'database': 'simulator_20220720'
}


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alert_system_nrd():
    
    @task()
    def extraction(connection = connection):

        query_metrics_today = '''WITH feed_metrics AS 
        (select toDate(time) as date,
        toStartOfFifteenMinutes(time) as time,
        formatDateTime(time, '%R') as interval,
        uniq(user_id) as feed_users,
        countIf(action='like') as likes,
        countIf(action='view') as views,
        countIf(action='like') / countIf(action='view') * 100 as ctr
        from simulator_20220720.feed_actions
        where toDate(time) between addDays(now(), -1) and now()
        and time < toStartOfFifteenMinutes(now())
        group by date, time, interval
        order by date, time, interval),

        messenger_metrics AS
        (select toDate(time) as date,
        toStartOfFifteenMinutes(time) as time,
        formatDateTime(time, '%R') as interval,
        uniq(user_id) as messenger_users,
        count(user_id) as messages_sent
        from simulator_20220720.message_actions
        where toDate(time) between addDays(now(), -1) and now()
        and time < toStartOfFifteenMinutes(now())
        group by date, time, interval
        order by date, time, interval)

        select *
        from feed_metrics join messenger_metrics using interval
        '''

        metrics_today = pandahouse.read_clickhouse(query_metrics_today, connection=connection)

        return metrics_today #, metrics_week

    
    @task 
    def check_and_send_one(metrics_today):
        def check_anomaly_one(df, metric, window, coef, alpha):
            df = df[['time','interval', metric]].copy()
            #
            def one_exp_smooth(series, alpha):
                results = [series[0]] #функция экспоненциального сглаживания
                for i in range(1, len(series)):
                    results.append(alpha * series[i] + (1-alpha)*results[i-1])
                return pd.Series(results)
            #
            df['smooth'] = one_exp_smooth(df[metric], alpha)
            df['smooth_75'] = df['smooth'].rolling(window).quantile(0.75)
            df['smooth_25'] = df['smooth'].rolling(window).quantile(0.25)
            df['iqr'] = df['smooth_75'] - df['smooth_25']
            df['deviation'] = (df[metric] - df['smooth']) / df['smooth'] * 100
            df['smooth_upper'] =  df.smooth_75 + coef * df.iqr
            df['smooth_lower'] =  df.smooth_25 - coef * df.iqr

            df['smooth_upper'] = df['smooth_upper'].rolling(10, center=True).mean()
            df['smooth_lower'] = df['smooth_lower'].rolling(10, center=True).mean()

            if df[metric].iloc[-1] > df['smooth_upper'].iloc[-1] or df[metric].iloc[-1] < df['smooth_lower'].iloc[-1]:
                alert = 1
            else:
                alert = 0
            return alert, df
        #
        one_metrics = ['messenger_users', 'messages_sent']
        for metric in one_metrics:
            alert, df = check_anomaly_one(metrics_today, metric, window=6, coef=2, alpha=0.85)
            if alert == 1:
                bot.sendMessage(chat_id = chat_id, text='{} = {}, Отклонение = {} %'.format(metric, df[metric][-1], df['deviation'][-1]))
                sns.set(rc={'figure.figsize': (18, 8)})
                plt.tight_layout()
                ax = sns.lineplot(data = df, y=metric, x='time', color='blue')
                ax = sns.lineplot(data = df, y='smooth', x='time', color='red')
                ax = sns.lineplot(data = df, y='smooth_upper', x='time', color='green')
                ax = sns.lineplot(data = df, y='smooth_lower', x='time', color='green')
                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 5 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                messenger_metrics = io.BytesIO()
                plt.savefig(messenger_metrics)
                messenger_metrics.seek(0)
                messenger_metrics.name = 'messenger_metrics'
                plt.close()
                #
                bot.sendPhoto(chat_id=chat_id, photo=messenger_metrics)

            else:
                pass
                
    @task 
    def check_and_send_two(metrics_today):
        def check_anomaly_double(df, metric, window, coef, alpha, beta):
            df = df[['time','interval', metric]].copy()
            #
            def double_exponential_smoothing(series, alpha, beta):
                result = [series[0]]
                for n in range(1, len(series)+1):
                    if n == 1:
                        level, trend = series[0], series[1] - series[0]
                    if n >= len(series): # прогнозируем
                        value = result[-1]
                    else:
                        value = series[n]
                    last_level, level = level, alpha*value + (1-alpha)*(level+trend)
                    trend = beta*(level-last_level) + (1-beta)*trend
                    result.append(level+trend)
                return pd.Series(result)
            #
            df['smooth'] = double_exponential_smoothing(df[metric], alpha, beta)
            df['smooth_75'] = df['smooth'].rolling(window).quantile(0.75)
            df['smooth_25'] = df['smooth'].rolling(window).quantile(0.25)
            df['iqr'] = df['smooth_75'] - df['smooth_25']
            df['deviation'] = (df[metric] - df['smooth']) / df['smooth'] * 100
            df['smooth_upper'] =  df.smooth_75 + coef * df.iqr
            df['smooth_lower'] =  df.smooth_25 - coef * df.iqr

            df['smooth_upper'] = df['smooth_upper'].rolling(10, center=True).mean()
            df['smooth_lower'] = df['smooth_lower'].rolling(10, center=True).mean()    

            if df[metric].iloc[-1] > df['smooth_upper'].iloc[-1] or df[metric].iloc[-1] < df['smooth_lower'].iloc[-1]:
                alert_2 = 1
            else:
                alert_2 = 0
            return alert_2, df
        #
        metrics_double = ['views','feed_users','likes', 'ctr']
        for metric in metrics_double:
            alert_2, df = check_anomaly_double(metrics_today, metric, window=6, coef=2, alpha=0.35, beta=1)
            if alert_2 == 1:
                bot.sendMessage(chat_id = chat_id, text='{} = {}, Отклонение = {} %'.format(metric, df[metric][-1], df['deviation'][-1]))
                sns.set(rc={'figure.figsize': (18, 8)})
                plt.tight_layout()

                ax = sns.lineplot(data = df, y=metric, x='interval', color='blue')
                ax = sns.lineplot(data = df, y='smooth', x='interval', color='red')
                ax = sns.lineplot(data = df, y='smooth_upper', x='interval', color='green')
                ax = sns.lineplot(data = df, y='smooth_lower', x='interval', color='green')
                #
                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 5 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                feed_metrics = io.BytesIO()
                plt.savefig(feed_metrics)
                feed_metrics.seek(0)
                feed_metrics.name = 'feed_metrics'
                plt.close()
                
                bot.sendPhoto(chat_id=chat_id, photo=feed_metrics)
            else:
                pass
        
    metrics_today = extraction()
    check_and_send_one(metrics_today)
    check_and_send_two(metrics_today)
    
alert_system_nrd = alert_system_nrd()
