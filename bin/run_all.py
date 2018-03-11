# -*- coding: utf-8 -*-
"""
Created on Fri Mar  9 10:49:59 2018

@author: jl237561
"""

import argparse
import configparser
from dateutil import parser as datetime_parser
from pyspark.sql import SparkSession
from pyspark.ml.feature import SQLTransformer
from pyspark.ml.feature import VectorAssembler

from talkdata.sql import get_conn
from talkdata.sql import init_tables_if_not_exist
from talkdata.sql import drop_all_the_data
from talkdata.sql import fill_training_in_db
from talkdata.sql import fill_test_in_db

from pyspark.ml.classification import RandomForestClassifier

desc = """

spark-submit --packages org.postgresql:postgresql:42.2.1 \
    --master spark://is222239:7077 \
    --executor-memory 10g  \
    ~/workspace/talkdata/bin/run_all.py \
    --config /home/jl237561/workspace/talkdata/config/is222239.ini

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

    spark = SparkSession \
        .builder \
        .appName("talkdata") \
        .getOrCreate()

    url = 'jdbc:postgresql://is222239:5432/%s' % dbname

    jdbcDF_training = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "training") \
        .option("user", username) \
        .option("password", password) \
        .load()
    jdbcDF_training.printSchema()
    jdbcDF_training.show()

    assembler = VectorAssembler(
        inputCols=["app", "channel"],
        outputCol="features")
    jdbcDF_training = assembler.transform(jdbcDF_training)
    jdbcDF_training.select("features", "is_attributed").show()
    rf = RandomForestClassifier(
        labelCol="is_attributed", featuresCol="features", numTrees=10)
    print("!!! start to train rf")
    model = rf.fit(jdbcDF_training)
    print("!!! end to train")

    url = 'jdbc:postgresql:%s' % dbname
    jdbcDF_test = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "test") \
        .option("user", username) \
        .option("password", password) \
        .load()
    jdbcDF_test.printSchema()
    jdbcDF_test.show()
    jdbcDF_test = assembler.transform(jdbcDF_test)
    jdbcDF_test = model.transform(jdbcDF_test)

    print("prediction results!")

    jdbcDF_test.show()

    properties = {
        "user": username,
        "password": password
    }
    mode = "overwrite"

    url = 'jdbc:postgresql:%s' % dbname
    jdbcDF_test.select("click_id", "prediction").write.jdbc(url=url,
                           table="test_predict",
                           mode=mode, properties=properties)

#    jdbcDF_test.write \
#        .format("jdbc") \
#        .option("url", url) \
#        .option("dbtable", "test_predict") \
#        .option("user", username) \
#        .option("password", password) \
#        .load()
#    jdbcDF_training.printSchema()
#    jdbcDF_training.show()
