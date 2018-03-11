# -*- coding: utf-8 -*-
"""
Created on Fri Mar  9 10:49:59 2018

@author: jl237561
"""

import argparse
import configparser
from dateutil import parser as datetime_parser

from talkdata.sql import get_conn
from talkdata.sql import init_tables_if_not_exist
from talkdata.sql import drop_all_the_data
from talkdata.sql import fill_training_in_db
from talkdata.sql import fill_test_in_db

desc = """

python ~/workspace/talkdata/bin/run_analysis.py \
    --config /home/jl237561/workspace/talkdata/config/is222239_sample.ini

"""

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--config', dest='config')
    args = parser.parse_args()
    config_path = args.config

    config = configparser.ConfigParser()
    config.sections()
    config.read(config_path)

    dbname = config['POSTGRESQL']["dbname"]
    username = config['POSTGRESQL']["username"]
    password = config['POSTGRESQL']["password"]

    training_csv = config['DATA']["training_csv"]
    test_csv = config['DATA']["test_csv"]

    need_refresh = int(config['DATA']["need_refresh"])

    conn = get_conn(dbname=dbname, username=username)
    init_tables_if_not_exist(conn)

    if need_refresh:
        drop_all_the_data(conn)
        fill_training_in_db(conn, training_csv)
        fill_test_in_db(conn, test_csv)

    sql_query = "select channel, count(*) from training where is_attributed=1 GROUP BY channel ORDER BY channel DESC;"
    cur = conn.cursor()
    cur.execute(sql_query)
    rows = cur.fetchall()
    
    import pdb
    pdb.set_trace()