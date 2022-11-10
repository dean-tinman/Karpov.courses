from datetime import datetime, timedelta, date
import pandas as pd
from io import StringIO
import requests
##
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import pandahouse


default_args = {
    'owner':'nakokhov_rd',
    'depends_on_past':False,
    'retries':2,
    'retry_delay':timedelta(minutes=5),
    'start_date':datetime(2022, 7, 30)
}

schedule_interval = '0 12 * * *'

def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

@dag(default_args = default_args, schedule_interval = schedule_interval, catchup=False)
def dag_nakokhov():
                             
    @task()
    def feed_extraction(): 
        feed_query = '''
                    SELECT user_id, os, toString(gender) as gender, toString(age) as age,
                            countIf(action='like') as likes,
                            countIf(action='view') as views
                    FROM simulator_20220720.feed_actions
                    GROUP BY user_id, os, gender, age
                    format TSVWithNames'''
        feed_df = ch_get_df(feed_query)
        return feed_df
                                                    
   
    @task()
    def messages_extraction():
        messages_query = '''
                    SELECT * from
                    (SELECT reciever_id as user_id,
                            count(reciever_id) as messages_received,
                            uniq(user_id) as users_received
                    FROM simulator_20220720.message_actions
                    GROUP BY user_id) t1

                    FULL JOIN

                    (SELECT user_id, os, toString(gender) as gender, toString(age) as age, 
                            count(user_id) as messages_sent, 
                            uniq(reciever_id) as users_sent
                    FROM simulator_20220720.message_actions
                    GROUP BY user_id, os, gender, age) t2 using user_id
                    format TSVWithNames'''
        messages_df = ch_get_df(messages_query)
                             
        return messages_df
    
    
    @task()
    def transform_join(feed_df, messages_df):
        full_df = feed_df.merge(messages_df, on = ['user_id', 'os', 'gender', 'age'])
        return full_df
    
    
    @task()
    def groupby_gender(full_df):
        by_gender = full_df.groupby('gender', as_index=False)[['views',
                                        'likes',
                                        'messages_received',
                                        'messages_sent',
                                      'users_received',
                                      'users_sent']].sum()
        by_gender.insert(0, 'date', date.today() - timedelta(days=1))
        by_gender.insert(1, 'dimension', 'gender')
        by_gender = by_gender.rename(columns={'gender':'dimension_value'})
        return by_gender
    
    
    @task()
    def groupby_age(full_df):
        by_age = full_df.groupby('age', as_index=False)[['views',
                                        'likes',
                                        'messages_received',
                                        'messages_sent',
                                      'users_received',
                                      'users_sent']].sum().sort_values(by='age')
        by_age.insert(0, 'date', date.today() - timedelta(days=1))
        by_age.insert(1, 'dimension', 'age')
        by_age = by_age.rename(columns={'age':'dimension_value'})
        return by_age
    
    
    @task()
    def groupby_os(full_df):
        by_os = full_df.groupby('os', as_index=False)[['views',
                                        'likes',
                                        'messages_received',
                                        'messages_sent',
                                      'users_received',
                                      'users_sent']].sum()
        by_os.insert(0, 'date', date.today() - timedelta(days=1))
        by_os.insert(1, 'dimension', 'os')
        by_os = by_os.rename(columns={'os':'dimension_value'})
        return by_os
    
    
    @task()
    def transform_final(by_os, by_gender, by_age):
        frames = [by_os, by_gender, by_age]
        final_df_nrd = pd.concat(frames).reset_index(drop=True)
        final_df_nrd['date'] = pd.to_datetime(final_df_nrd['date'])
        final_df_nrd.rename(columns={'date':'event_date'}, inplace=True)
        return final_df_nrd
    
    
    @task()
    def load(final_df_nrd):

        connection_test = {
                'host': 'https://clickhouse.lab.karpov.courses',
                'password': '656e2b0c9c',
                'user': 'student-rw',
                'database': 'test'
            }


        query_load = '''
                    CREATE TABLE IF NOT EXISTS test.nakokhov_rd
                    (event_date Date,
                     dimension String,
                     dimension_value String,
                     views Int64,
                     likes Int64,
                     messages_received Int64,
                     messages_sent Int64,
                     users_received Int64,
                     users_sent Int64)
                     ENGINE = Log() '''

        pandahouse.execute(query=query_load, connection=connection_test)
        pandahouse.to_clickhouse(df=final_df_nrd, table='nakokhov_rd', index=False, connection=connection_test)
    
    
    
    feed_df = feed_extraction() 
    messages_df = messages_extraction()
    full_df = transform_join(feed_df, messages_df)
    by_gender = groupby_gender(full_df)
    by_age = groupby_age(full_df)
    by_os = groupby_os(full_df)
    final_df_nrd = transform_final(by_os, by_gender, by_age)
    load(final_df_nrd)

nrd_dag = dag_nakokhov()