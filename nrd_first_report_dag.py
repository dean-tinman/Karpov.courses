#pip install python-telegram-bot
#pip install telegram

import telegram
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import io
import pandahouse as ph
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


def report_1(chat=None):
    chat_id = chat or -770113521 

    bot_token = '5389713558:AAHbSHO0Q9VvP-UwGEWrkjv7DvFkIcxZtYc'
    bot = telegram.Bot(token=bot_token)

    #my_id = 174568015
    #chat_id = -770113521
    
    connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20220720'
}

    query = '''SELECT toDate(time) as Date,
                uniq(user_id) AS Feed_DAU,
                countIf(action='like') AS Likes,
                countIf(action='view') AS Views,
                Likes/Views AS CTR
                FROM {db}.feed_actions
                group by Date
                Having Date between addWeeks(yesterday(), -1) and yesterday()'''

    df = ph.read_clickhouse(query=query, connection=connection)


    message = df.iloc[-1].transpose().to_string()
    bot.sendMessage(chat_id=chat_id, text=message)

    prev_week_df = df.iloc[0:7]

    fig, axes = plt.subplots(3,1, figsize=(15,15))

    plt.subplot(3,1,1)
    plt.title('Feed_DAU')
    ax1 = sns.lineplot(data=prev_week_df, y='Feed_DAU', x='Date', marker='o', color='red')
    plt.grid(visible=True)
    ax1.set(xlabel=None)

    plt.subplot(3,1,2)
    plt.title('Likes and views')
    ax2 = sns.lineplot(data=prev_week_df, y='Likes', x='Date', marker='o', color='blue', legend=True, label='Likes')
    ax3 = sns.lineplot(data=prev_week_df, y='Views', x='Date', marker='o', color='green', legend=True, label='Views')
    plt.grid(visible=True)
    ax2.set(xlabel=None)

    plt.subplot(3,1,3)
    plt.title('CTR')
    ax4 = sns.lineplot(data=prev_week_df, y='CTR', x='Date', marker='o', color='purple')
    plt.grid(visible=True)
    ax4.set(xlabel=None)
    

    seven_days_charts = io.BytesIO()
    plt.savefig(seven_days_charts)
    seven_days_charts.seek(0)
    seven_days_charts.name = 'metrics for the previous week'
    plt.close()

    bot.sendPhoto(chat_id=chat_id, photo=seven_days_charts)




default_args = {
    'owner':'nakokhov_rd',
    'depends_on_past':False,
    'retries':2,
    'retry_delay':timedelta(minutes=5),
    'start_date':datetime(2022, 8, 8)
}

schedule_interval = '0 11 * * *'

#group_chat_id = -770113521

@dag(default_args=default_args, schedule_interval = schedule_interval, catchup=False)
def nrd_first_report_dag():
    
    @task    
    def make_report_1():
        report_1()
    
    make_report_1()

nrd_first_report_dag = nrd_first_report_dag()