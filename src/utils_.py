import os
import pandas as pd
#from af_batch import vars_
from src import vars_

def get_data_from_mariadb_v1(num_lines=1,
                             begin=0,
                             database='sample',
                             table='listenbrainz'):
    import pymysql
    conf = get_db_config()
    conn = pymysql.connect(host=conf['host'],
                           user=conf['user'],
                           password=conf['passwd'],
                           database=database,
                           port=conf['port'])

    cur = conn.cursor()
    q = f'''
    select * 
    from {table}
    limit 0,{num_lines}
    '''
    cur.execute(q)
    conn.commit()
    conn.close()
    rst = cur.fetchall()

    return rst


def get_data_from_mariadb_v2(num_lines=1,
                             begin=0,
                             table='listenbrainz'):
    db_connection, _ = get_mariadb_conn()
    q = f'''
    select * 
    from {table}
    limit {begin},{num_lines}
    '''
    return pd.read_sql(sql=q,
                       con=db_connection,
                       parse_dates=["listened_at"])


def get_data(test=True):
    '''
    test=True일 때는 mariaDB에서 가져옴
    False일 때는 kafka에서.
    '''

    if test:
        try:
            with open(os.path.join(vars_.dir_path, './af_batch/test_offset.txt'),
                      encoding='utf-8') as f:
                begin = int(f.readlines()[-1])
        except Exception as e:
            begin = 0

        # 테스트에서는 mariaDB에서 가져옴.
        # 전 작업의 offset을 불러다가 다음 로그를 가져옴
        from random import randint
        num_lines = randint(10, 60)  # (raw 데이터 분당 로그양에 기반해서) 랜덤으로 가져올 열
        print(begin, num_lines)
        df = get_data_from_mariadb_v2(begin=begin, num_lines=num_lines)

        begin += (int(num_lines) - 1)  # offset
        print(df.shape)
        with open(os.path.join(vars_.dir_path, './af_batch/test_offset.txt'), mode='a', encoding='utf-8') as f:
            f.write(f'{begin}\n')

        print(begin, num_lines)
        return df
        # return get_data_from_mariadb_v2(begin=begin, num_lines=num_lines)

    else:
        # 서버로 돌려서 던지..는건
        # 카프카로 돌리면 테스크가 돌때 이 dag만 subscribe하는 메세지 큐를 만들어둬서 하면 어떨까??
        return


def get_mariadb_conn(database=None):
    from sqlalchemy import create_engine
    from sqlalchemy import engine

    conf = get_db_config()
    if not database:
        database = conf["database"]

    db_connection_str = engine.url.URL(drivername='mysql+pymysql',
                                       username=conf["user"],
                                       password=conf["passwd"],
                                       host=conf["host"],
                                       database=database)


    # 비번이나 이름에 @있으면 접속에 문제 생김
    # db_connection_str = f'mysql+pymysql://{conf["user"]}:{conf["passwd"]}@{conf["host"]}/{database}'
    db_connection = create_engine(db_connection_str)
    conn = db_connection.connect()

    return db_connection, conn


def get_config():
    import yaml, os
    with open(os.path.join(vars_.dir_path, './config/config.yml'), mode='r') as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)
    return conf


def get_kafka_config():
    return get_config()['kafka']


def get_zookeeper_config():
    return get_config()['zookeeper']



def get_db_config():
    return get_config()['mysql']


def get_s3_config():
    return get_config()['s3']


def get_kinesis_config():
    return get_config()['kinesis']


def set_spotify_config():
    conf = get_config()['spotify']
    os.environ.update(conf)


def set_boto3_config():
    conf = get_config()['boto3']
    print(os.path.join(vars_.dir_path, conf['AWS_SHARED_CREDENTIALS_FILE']))
    conf['AWS_SHARED_CREDENTIALS_FILE'] = os.path.join(vars_.dir_path, conf['AWS_SHARED_CREDENTIALS_FILE'])
    os.environ.update(conf)



def default_plot_setting():
    import warnings
    import matplotlib.pyplot as plt

    # Set Matplotlib defaults
    plt.style.use("seaborn-whitegrid")
    plt.rc("figure", autolayout=True, figsize=(11, 4))
    plt.rc(
        "axes",
        labelweight="bold",
        labelsize="large",
        titleweight="bold",
        titlesize=16,
        titlepad=10,
    )
    plt.rc('font', family='NanumGothic')

    plot_params = dict(
        color="0.75",
        style=".-",
        markeredgecolor="0.25",
        markerfacecolor="0.25",
    )

    warnings.filterwarnings('ignore')

    return plt, plot_params
