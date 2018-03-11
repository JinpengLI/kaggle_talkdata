# -*- coding: utf-8 -*-
"""
Created on Fri Mar  9 11:48:03 2018

@author: jl237561
"""
from dateutil import parser as datetime_parser

import psycopg2

conn = None

def get_conn(dbname, username, password=""):
    global conn
    if conn is None:
        conn = psycopg2.connect(
            dbname=dbname,
            user=username,
            password=password)
    return conn

def init_tables_if_not_exist(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS training ( "
                "ip integer, "
                "app integer,"
                "device smallint,"
                "os smallint,"
                "channel smallint,"
                "click_time timestamp default NULL,"
                "attributed_time timestamp default NULL,"
                "is_attributed smallint);")
    cur.execute("CREATE TABLE IF NOT EXISTS test ( "
                "click_id integer,"
                "ip integer, "
                "app integer,"
                "device smallint,"
                "os smallint,"
                "channel smallint,"
                "click_time timestamp default NULL,"
                "is_attributed smallint default NULL);")
    conn.commit()

def drop_data_from_table(conn, table, commit=True):
    cur = conn.cursor()
    cur.execute("DELETE FROM %s;" % table)
    if commit:
        conn.commit()

def drop_all_the_data(conn, commit=True):
    drop_data_from_table(conn=conn, table="training", commit=False)
    drop_data_from_table(conn=conn, table="test", commit=False)

    if commit:
        conn.commit()

def fill_training_in_db(conn, csv, commit=True):
    cur = conn.cursor()
    nline = 0
    with open(csv) as infile:
        is_header = True
        for line in infile:
            if nline % 1000 == 0:
                print nline
            nline += 1
            if is_header:
                is_header = False
            else:
                words = line.split(",")
                ip = int(words[0])
                app = int(words[1])
                device = int(words[2])
                os = int(words[3])
                channel = int(words[4])
                click_time = words[5]
                click_time = datetime_parser.parse(click_time)
                attributed_time = words[6]
                if attributed_time.strip()=="":
                    attributed_time = None
                is_attributed = int(words[7])
                cur.execute('INSERT INTO training '
                    '(ip, app, device, os, channel, click_time, attributed_time, is_attributed)'
                    ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
                    (ip, app, device, os, channel, click_time, attributed_time, is_attributed)
                )
    if commit:
        conn.commit()

def fill_test_in_db(conn, csv, commit=True):
    cur = conn.cursor()
    nline = 0
    with open(csv) as infile:
        is_header = True
        for line in infile:
            if nline % 1000 == 0:
                print nline
            nline += 1
            if is_header:
                is_header = False
            else:
                words = line.split(",")
                click_id = int(words[0])
                ip = int(words[1])
                app = int(words[2])
                device = int(words[3])
                os = int(words[4])
                channel = int(words[5])
                click_time = words[6]
                click_time = datetime_parser.parse(click_time)
                cur.execute('INSERT INTO test '
                    '(click_id, ip, app, device, os, channel, click_time)'
                    ' VALUES (%s, %s, %s, %s, %s, %s, %s)',
                    (click_id, ip, app, device, os, channel, click_time, )
                )
    if commit:
        conn.commit()