import numpy as np
import pandas as pd
import uuid
import utils_

table = 'lb_user'


def preprocess(df):
    # 멀티프로세스 이용.
    df['timestamp'] = pd.to_datetime(df.listened_at)
    df = df.set_index('timestamp')
    df = df.rename(columns={'user_name': 'name'})

    return df


def store(df):
    # 데이터를 저장하는 단계
    # 테이블에 없는 ID 테이블에 추가
    names = set(df['name'])
    q = f'''
    # 테이블에 있는 ID
    SELECT *
    FROM sample.{table}
    WHERE NAME IN ({"'" + "','".join(names) + "'"})
    '''

    db_connection, _ = utils_.get_mariadb_conn()
    old_users = pd.read_sql(sql=q,
                            con=db_connection,
                            parse_dates=["first_active_date", "last_active_date"])
    history_ = pd.DataFrame()

    def set_new_user(df):
        # user_id 생성
        # print('hhh',df.iloc[0]['name'], df.iloc[0].name)
        df['user_id'] = [uuid.uuid5(uuid.NAMESPACE_OID, df.iloc[i]['name']).bytes for i in range(len(df.index))]

        # 배치 데이터 중 유저의 마지막 데이터
        temp1 = df.sort_values('listened_at').groupby('name').tail(1)[['listened_at', 'name', 'user_id']]
        # 배치 데이터 중 유저의 첫번째 데이터
        temp2 = df.sort_values('listened_at').groupby('name').head(1)[['listened_at', 'name']]
        temp = temp1.reset_index('timestamp').merge(temp2.reset_index('timestamp'), on='name')
        temp = temp.rename(columns={'listened_at_x': 'last_active_date',
                                    'listened_at_y': 'first_active_date'})
        temp = temp[['user_id', 'name', 'first_active_date', 'last_active_date']]
        # print(temp[['name', 'first_active_date', 'last_active_date']])

        return temp

    if old_users.empty:  # 테이블에 저장된 user가 없을 때 (모두 새 유저일 때)
        # user data
        df_ = set_new_user(df)
        df_.to_sql(name=table,
                  con=db_connection,
                  if_exists='append',
                  index=False)

        # print(df.sort_values('listened_at').groupby('user_name').tail(1)[['listened_at', 'user_name']])
        # print(df.sort_values('listened_at').groupby('user_name').head(1)[['listened_at', 'user_name']])
        # print(df[['user_id', 'user_name']])

        # user history
        # user_id 달아줌
        history_ = (df.merge(df_, on='name')[['listened_at', 'user_id', 'recording_msid']]
                    .rename(columns={'listened_at': 'timestamp_', 'recording_msid': 'record_msid'}))
        history_['history_id'] = [uuid.uuid4().bytes for i in range(len(df.index))]
        # history_.to_sql(name='lb_user_history',
        #           con=db_connection,
        #           if_exists='append',
        #           index=False)

    else:  # old_user가 있을 때

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
                update {table}
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

        # new user 작업
        new_users = df[df.name.isin(old_users.name.values) == False][['name', 'listened_at']]
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

            # user history
            # user_id 달아줌
            # df['user_id'] = None
            # new_user의 id 채워넣음
            # df['user_id'] = np.where((df.user_id == new_users.user_id) & (df.user_id.isnull() | df.user_id.isna()),
            #                          new_users.user_id, df.user_id)
            df = df.merge(new_users, on='name', how='left')
            # print(df.columns)
            df = df[['listened_at', 'user_id', 'name', 'recording_msid']]
        
        # df['user_id'] = np.where((df.user_id == old_users.user_id) & (df.user_id.isnull() | df.user_id.isna()),
        #                          old_users.user_id, df.user_id)
        df = df.merge(old_users[['name','user_id']], on='name', how='left')
        print('after merge old_users: ', df.columns)
        if 'user_id_x' in df.columns:
            df['user_id_x'] = np.where((df.user_id_x.isnull() | df.user_id_x.isna()),
                                          df.user_id_y, df.user_id_x)
            df = df.rename(columns={'user_id_x':'user_id'})
        print(df.user_id.isnull().value_counts())
        print(df.user_id)
        history_ = df[['listened_at', 'user_id', 'recording_msid']]
        history_ = history_.rename(columns={'listened_at': 'timestamp_',
                                            'recording_msid': 'record_msid'})
        history_['history_id'] = [uuid.uuid4().bytes for i in range(len(history_.index))]
        print(history_.head())

    return history_


if __name__ == '__main__':
    # print(get_data_from_mariadb(table='orders'))
    # df = get_data_from_mariadb_v2()
    # print(df.head())

    df = utils_.get_data(test=True)
    df = preprocess(df)
    store(df)
