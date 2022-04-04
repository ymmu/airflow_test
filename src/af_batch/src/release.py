import pandas as pd
import utils_


def preprocess(df):
    df['timestamp'] = pd.to_datetime(df.listened_at)
    df = df.set_index('timestamp')
    df = df.rename(columns={'user_name': 'name'})

    df = df[['artist_msid', 'artist_name']].rename(columns={'artist_name': 'name'})
    df = df.drop_duplicates()  # 중복제거
    df['genres'] = None
    df['spotify_id'] = None
    print(df)

    # 128 이상 길이면 자르기
    def trunc_name(x):
        return x[:128] if len(x) > 128 else x

    df['name'] = df['name'].apply(trunc_name)
    return df


def store_artist(df):
    # 데이터를 저장하는 단계
    # 테이블에 없는 ID 테이블에 추가

    '''
    이미 데이터가 있으면 저장할 필요 없음
    없는 데이터만 insert
    '''

    q = f'''
    # 테이블에 있는 ID
    SELECT artist_msid
    FROM sample.lb_artist
    '''
    db_connection, _ = utils_.get_mariadb_conn()
    saved_artist = pd.read_sql(sql=q,
                               con=db_connection)

    if not saved_artist.empty:  # 테이블에 저장된 artist가 없을 때 (모두 새 artist일 때)
        print('get only new artists data ..')
        mask = ~(df.artist_msid.isin(list(saved_artist.artist_msid.values)))  # ~:not
        new_artists = df.loc[mask, :]
        print(new_artists)

        new_artists.to_sql(name='lb_artist',
                           con=db_connection,
                           if_exists='append',
                           index=False)
    else:
        df.to_sql(name='lb_artist',
                   con=db_connection,
                   if_exists='append',
                   index=False)


if __name__ == '__main__':
    # print(get_data_from_mariadb(table='orders'))
    # df = get_data_from_mariadb_v2()
    # print(df.head())

    df = utils_.get_data(test=True)
    print(df.shape, df.columns)
    # df = preprocess(df)
    # store_artist(df)

    # test mp ----
    # from multiprocessing import Pool, cpu_count
    # import spotify_
    # batch = df[['artist_name', 'track_name']].to_dict(orient='split')['data']
    # print(batch)
    # # global num_cores
    # num_cores = cpu_count()
    # with Pool(num_cores) as p:
    #     rst = p.starmap_async(spotify_.get_track_data, batch)
    #     print('*'*10)
    #     pprint(rst.get())

