import sys, glob, os
from datetime import datetime
import multiprocessing as mp
import warnings
import pymysql

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import uuid
import utils_


def preprocess(df):
    # 멀티프로세스 이용.
    df['timestamp'] = pd.to_datetime(df.listened_at)
    df = df.set_index('timestamp')
    df = df.rename(columns={'user_name': 'name'})


    return df


def store_user(df):
    # 데이터를 저장하는 단계
    # 테이블에 없는 ID 테이블에 추가
    names = set(df['name'])
    q = f'''
    # 테이블에 있는 ID
    SELECT *
    FROM sample.lb_user
    WHERE NAME IN ({"'" + "','".join(names) + "'"})
    '''

    db_connection, _ = utils_.get_mariadb_conn()
    old_users = pd.read_sql(sql=q,
                            con=db_connection,
                            parse_dates=["first_active_date","last_active_date"])

    def set_new_user(df):
        # user_id 생성
        # print('hhh',df.iloc[0]['name'], df.iloc[0].name)
        df['user_id'] = [uuid.uuid5(uuid.NAMESPACE_OID, df.iloc[i]['name']).bytes for i in range(len(df.index))]
        temp1 = df.sort_values('listened_at').groupby('name').tail(1)[['listened_at', 'name', 'user_id']]
        temp2 = df.sort_values('listened_at').groupby('name').head(1)[['listened_at', 'name']]
        temp = temp1.reset_index('timestamp').merge(temp2.reset_index('timestamp'), on='name')
        temp = temp.rename(columns={'listened_at_x': 'last_active_date',
                                    'listened_at_y': 'first_active_date'})
        temp = temp[['user_id', 'name', 'first_active_date', 'last_active_date']]
        # print(temp[['name', 'first_active_date', 'last_active_date']])

        return temp

    if old_users.empty:  # 테이블에 저장된 user가 없을 때 (모두 새 유저일 때)
        df = set_new_user(df)
        df.to_sql(name='lb_user',
                  con=db_connection,
                  if_exists='append',
                  index=False)

        # print(df.sort_values('listened_at').groupby('user_name').tail(1)[['listened_at', 'user_name']])
        # print(df.sort_values('listened_at').groupby('user_name').head(1)[['listened_at', 'user_name']])
        # print(df[['user_id', 'user_name']])

    else:  # 테이블에 저장된 user가 있을 때

        # 테이블에 있는 ID last_active_date 바꿔줌
        temp1 = df.sort_values('listened_at').groupby('name').tail(1)[['listened_at', 'name']]
        old_users = old_users.merge(temp1.reset_index('timestamp'), on='name')
        old_users = old_users.drop(labels='last_active_date', axis=1).rename(columns={'timestamp': 'last_active_date'})
        update_ = old_users[['user_id', 'last_active_date']]

        import pymysql
        conf = utils_.get_db_config()
        conn = pymysql.connect(host=conf['host'],
                               user=conf['user'],
                               password=conf['passwd'],
                               database=conf['database'],
                               port=conf['port'])
        cur = conn.cursor()
        q = f'''
                update lb_user
                set last_active_date=%s
                where user_id=%s 
                '''
        cur.executemany(q, [list(i) for i in update_.values])
        conn.commit()
        conn.close()
        rst = cur.fetchall()
        # 'upsert' 안됨 ------
        # old_users.to_sql(name='lb_user',
        #                  con=db_connection,
        #                  if_exists='replace',
        #                  index=False)

        # print(old_users[['name','first_active_date','last_active_date']])
        # print(old_users.name.values)

        new_users = df[df.name.isin(old_users.name.values) == False][['name', 'listened_at']]
        # print('new:')
        # print(new_users)
        if not new_users.empty:
            new_users = set_new_user(new_users)
            new_users['user_id'] = [uuid.uuid5(uuid.NAMESPACE_OID,
                                               new_users.iloc[i]['name']).bytes for i in range(len(new_users.index))]
            # users = pd.concat([old_users, new_users], axis=0, ignore_index=True)[['user_id', 'name',
            #                                                                       'first_active_date',
            #                                                                       'last_active_date']]
            new_users = new_users[['user_id', 'name',
                          'first_active_date',
                          'last_active_date']]
            new_users.to_sql(name='lb_user',
                             con=db_connection,
                             if_exists='append',
                             index=False)


if __name__ == '__main__':
    # print(get_data_from_mariadb(table='orders'))
    # df = get_data_from_mariadb_v2()
    # print(df.head())

    df = utils_.get_data(test=True)
    df = preprocess(df)
    store_user(df)
