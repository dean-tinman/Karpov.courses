import telegram
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import io
import pandahouse as ph
from datetime import datetime, timedelta, date

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


def report_2(chat=None):
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

    daily_users_query = '''with purely_feed_users as (select toDate(time) as date, uniq(user_id) as purely_feed_users
    from simulator_20220720.feed_actions
    where toDate(time) = yesterday()
    and user_id not in
    (select distinct user_id
    from simulator_20220720.message_actions
    where toDate(time) = yesterday())
    group by date),

    purely_messenger_users as (select toDate(time) as date, uniq(user_id) as purely_messenger_users
    from simulator_20220720.message_actions
    where toDate(time) = yesterday()
    and user_id not in 
    (select distinct user_id
    from simulator_20220720.feed_actions
    where toDate(time) = yesterday())
    group by date),

    both_func_users as (select toDate(time) as date, uniq(user_id) as both_func_users
    from simulator_20220720.message_actions
    where toDate(time) = yesterday()
    and user_id in 
    (select distinct user_id
    from simulator_20220720.feed_actions
    where toDate(time) = yesterday())
    group by date)


    select *, purely_feed_users + purely_messenger_users + both_func_users as all_unique_users from 
    (select *
    from purely_feed_users
    join purely_messenger_users using date) t1
    join both_func_users using date
    '''
    #

    general_query = '''select * from
    (select toDate(time) as date, 
            CASE
            WHEN age < 18 THEN '<18'
            WHEN age >=18 and age <=30 THEN '18-30'
            WHEN age >30 and age <= 45 THEN '30-45'
            ELSE '45+'
            END as age_groups,
            country,
            os,
            source,
            count(reciever_id) as messages,
            count(reciever_id) / uniq(user_id) as messages_per_user,
            uniq(user_id) as messenger_users
    from simulator_20220720.message_actions
    group by date, age_groups, country, os, source
    order by date, age_groups, country, os, source) AS mes_t

    join
    (select  toDate(time) as date,
            CASE
            WHEN age < 18 THEN '<18'
            WHEN age >=18 and age <=30 THEN '18-30'
            WHEN age >30 and age <= 45 THEN '30-45'
            ELSE '45+'
            END as age_groups,
            country,
            os,
            source,
            countIf(action='like') as likes,
            countIf(action='view') as views,
            countIf(action='like') / uniq(user_id) as likes_per_user,
            countIf(action='view') / uniq(user_id) as views_per_user,
            uniq(user_id) as feed_users
    from simulator_20220720.feed_actions
    group by date, age_groups, country, os, source
    order by date, age_groups, country, os, source) AS feed_t 
    using date, age_groups, country, os, source
    where date between addWeeks(yesterday(),-1) and yesterday()'''
    #

    by_source_os_query = '''select toDate(time) as date, os, source, uniq(user_id) as users
    from simulator_20220720.feed_actions
    where toDate(time) = yesterday()
    group by date, os, source
    order by date'''
    #

    daily_users = ph.read_clickhouse(query=daily_users_query, connection=connection)
    percentages_df = daily_users[['purely_feed_users', 'purely_messenger_users', 'both_func_users']] \
    .apply(lambda x: x / daily_users['all_unique_users'] * 100)
    percentages_df.insert(0, value=daily_users.date, column='date')

    general_df = ph.read_clickhouse(query=general_query, connection=connection)
    general_df.date = pd.to_datetime(general_df.date).dt.date
    yesterday = date.today()-timedelta(days=1)
    yesterday_general_df = general_df.loc[general_df['date'] == yesterday]

    likes_views_per_user = general_df.groupby('date', as_index=False)[['likes_per_user', 'views_per_user']].mean()


    mes_likes_views = general_df.groupby('date', as_index=False)[['messages', 'likes', 'views']].sum()

    by_source_os_df = ph.read_clickhouse(query=by_source_os_query, connection=connection)
    by_source_pivot = by_source_os_df.pivot_table(index='date', columns='source', values='users', aggfunc='sum').reset_index()
    by_os_pivot = by_source_os_df.pivot_table(index='date', columns='os', values='users', aggfunc='sum').reset_index()

    #simple metrics message for yesterday
    sm_message = f'''Date: {daily_users.iloc[0,0]} \n
    Unique users: {daily_users.iloc[0,-1]}
        Feed-only: {daily_users.iloc[0,1]} / {round(percentages_df.iloc[0,1],2)}%
        Messenger-only: {daily_users.iloc[0,2]} / {round(percentages_df.iloc[0,2],2)}%
        Both functions: {daily_users.iloc[0,3]} / {round(percentages_df.iloc[0,3],2)}%
    Messages: {mes_likes_views.iloc[-1, 1]}
    Likes: {mes_likes_views.iloc[-1, 2]}
    Views: {mes_likes_views.iloc[-1, 3]}
    CTR: {round(mes_likes_views.iloc[-1, 2] / mes_likes_views.iloc[-1, 3], 2)} \n
    Sources:
        Organic: {round(by_source_pivot.iloc[0,2] / (by_source_pivot.iloc[0,2] + by_source_pivot.iloc[0,1]) * 100, 2)}%
        Ads: {round(by_source_pivot.iloc[0,1] / (by_source_pivot.iloc[0,2] + by_source_pivot.iloc[0,1]) * 100, 2)}% \n
    OS:
        iOS: {round(by_os_pivot.iloc[0,2] / (by_os_pivot.iloc[0,2] + by_os_pivot.iloc[0,1]) * 100, 2)}%
        Android: {round(by_os_pivot.iloc[0,1] / (by_os_pivot.iloc[0,2] + by_os_pivot.iloc[0,1]) * 100, 2)}%'''

    bot.sendMessage(chat_id=chat_id, text=sm_message)
    

    plt.figure(figsize=(12,8))
    plt.grid()
    plt.title("Likes and views per user")
    sns.lineplot(data=likes_views_per_user, x='date',y='likes_per_user', label='Likes', color='red', legend=True)
    plt.ylabel('')
    sns.lineplot(data=likes_views_per_user, x='date', y='views_per_user', label='Views', color='blue', legend=True)
    plt.ylabel('');
    #
    percentages_area_chart = io.BytesIO()
    plt.savefig(percentages_area_chart)
    percentages_area_chart.seek(0)
    percentages_area_chart.name = 'percentages_area_chart'
    plt.close()
    #
    bot.sendPhoto(chat_id=chat_id, photo=percentages_area_chart)



#

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
def nrd_second_report_dag():
    
    @task    
    def make_report_2():
        report_2()
    
    make_report_2()

nrd_second_report_dag = nrd_second_report_dag()