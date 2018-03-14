# -*- coding: utf-8 -*-
"""
Created on Fri Mar  9 10:49:59 2018

@author: jinpeng
"""

import os
import argparse
import configparser
import shutil
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.svm import SVC
from sklearn.model_selection import GridSearchCV
import numpy as np
from dateutil import parser as dt_parser


def resampling(working_dir, training_csv, n_train_val=5, train_ratio=0.7):
    print("start to do resampling...")
    if not os.path.isdir(working_dir):
        os.makedirs(working_dir)
    is_header= True
    iline = 0
    for line in open(training_csv):
        if is_header:
            is_header = False
            iline += 1
            continue
        iline += 1
        if iline % 1000000 == 0:
            print iline

    nlines = iline
    nline_per_part = int(nlines / float(n_train_val))
    file_in = open(training_csv, "r")
    header = file_in.readline() ## skip header
    test_ratio = 1.0 - train_ratio

    print("start to write out the train and val parts!")
    for i in range(n_train_val):
        print("train part %d" % i)
        base_dir = os.path.join(working_dir, "%d" % i)
        if os.path.isdir(base_dir):
            shutil.rmtree(base_dir) 
        os.makedirs(base_dir)

        path_train = os.path.join(base_dir, "train.csv")
        out_file = open(path_train, "w+")
        out_file.write(header)
        for i_cur_part in range(int(nline_per_part * train_ratio)):
            out_file.write(file_in.readline())
        out_file.close()

        print("val part %d" % i)
        path_val = os.path.join(base_dir, "val.csv")
        out_file = open(path_val, "w+")
        out_file.write(header)
        for i_cur_part in range(int(nline_per_part * test_ratio)):
            out_file.write(file_in.readline())
        out_file.close()
    file_in.close()

def convert_line_to_features(line, headers, ):
    words = line.strip().split(",")
    vals = []
    idx = headers.index("app")
    vals.append(int(words[idx]))

    idx = headers.index("device")
    vals.append(int(words[idx]))

    idx = headers.index("os")
    vals.append(int(words[idx]))

    idx = headers.index("channel")
    vals.append(int(words[idx]))

    idx = headers.index("click_time")
    dt_val = dt_parser.parse(words[idx])
    vals.append(int(dt_val.day))
    vals.append(int(dt_val.hour))

    idx = headers.index("is_attributed")
    vals.append(int(words[idx]))
    return vals

def read_csv_to_mat(path_csv):
    file_in = open(path_csv, "r")
    header = file_in.readline()
    headers = header.strip().split(",")
    mat = []
    nline = 0
    for line in file_in:
        vals = convert_line_to_features(line, headers)
        mat.append(vals)
        nline += 1
        if nline % 10000 == 0:
            print("read_csv_to_mat read lines=", nline)
    file_in.close()
    mat = np.asarray(mat)
    return mat

desc = """

python ~/workspace/talkdata/bin/run_all.py \
    --config ~/workspace/talkdata/config/is222239.ini

"""

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--config', dest='config')
    args = parser.parse_args()
    config_path = args.config

    config = configparser.ConfigParser()
    config.sections()
    config.read(config_path)

    training_csv = os.path.expanduser(config['DATA']["training_csv"])
    working_dir = os.path.expanduser(config['DATA']["working_dir"])
    need_resampling = int(config['DATA']["need_resampling"])
    n_train_val = int(config['DATA']["n_train_val"])

    if need_resampling:
        resampling(working_dir, training_csv, n_train_val=n_train_val, )

    models_to_tune = [
        {
            "model": GradientBoostingRegressor,
            "tuned_parameters": [
                {
                    'n_estimators': [200, 150],
                    'learning_rate': [0.1, 0.05],
                }
            ],
        },
        {
            "model": SVC,
            "tuned_parameters": [
                {
                    'C': [1, 2],
                    'kernel': ["rbf", "linear"],
                }
            ],
        }
    ]

    for sub_dir in os.listdir(working_dir):
        path_train = os.path.join(working_dir, sub_dir, "train.csv")
        path_val = os.path.join(working_dir, sub_dir, "val.csv")

        mat_train = read_csv_to_mat(path_train)
        mat_val = read_csv_to_mat(path_train)

        import pdb
        pdb.set_trace()

