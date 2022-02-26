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

table = 'lb_track'


def preprocess(df):
    # 멀티프로세스 이용.
    df['timestamp'] = pd.to_datetime(df.listened_at)
    df = df.set_index('timestamp')
    df = df[['track_name', 'recording_msid', 'artist_msid']]
    df = df.rename(columns={'recording_msid': 'record_msid',
                            'track_name': 'name'})

    return df


def store(df):
    # 데이터를 저장하는 단계
    # 테이블에 없는 ID 테이블에 추가

    '''
    이미 데이터가 있으면 저장할 필요 없음
    없는 데이터만 생성
    '''

    q = f'''
    # 테이블에 있는 ID
    SELECT record_msid
    FROM sample.{table}
    '''
    db_connection, _ = utils_.get_mariadb_conn()
    saved_tracks = pd.read_sql(sql=q,
                               con=db_connection)

    if not saved_tracks.empty:  # 테이블에 저장된 artist가 없을 때 (모두 새 artist일 때)
        print('get only new track data ..')
        mask = ~(df.record_msid.isin(list(saved_tracks.record_msid.values)))  # ~:not
        new_artists = df.loc[mask, :]
        print(new_artists)

        new_artists.to_sql(name=table,
                           con=db_connection,
                           if_exists='append',
                           index=False)
    else:
        df.to_sql(name=table,
                  con=db_connection,
                  if_exists='append',
                  index=False)


if __name__ == '__main__':
    # print(get_data_from_mariadb(table='orders'))
    # df = get_data_from_mariadb_v2()
    # print(df.head())

    df = utils_.get_data(test=True)
    print(df.columns)
    print(df)
    # df = preprocess(df)
    # store_track(df)
