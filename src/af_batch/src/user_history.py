import utils_

table = 'lb_user_history'


def preprocess(df):
    pass


def store(df):
    # 데이터를 저장하는 단계
    # 테이블에 없는 ID 테이블에 추가
    db_connection, _ = utils_.get_mariadb_conn()
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
    df = preprocess(df)
    store(df)
