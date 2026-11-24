import os
import sys

from pyspark.sql import SparkSession

import config


def get_spark(app):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (SparkSession.builder.appName(app)
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
             .config("spark.sql.shuffle.partitions", "16")
             .config("spark.hadoop.fs.defaultFS", config.HDFS_URL)
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.addPyFile(os.path.join(config.BASE_DIR, "nba_parser.py"))
    return spark


def save_mongo(df, collection, key_fields=None):
    """Spark DataFrame -> MongoDB 컬렉션 (파티션별 bulk insert)"""
    from pymongo import MongoClient
    from pyspark.sql.types import DecimalType
    df = df.select([df[f.name].cast("double").alias(f.name) if isinstance(f.dataType, DecimalType)
                    else df[f.name] for f in df.schema.fields])
    MongoClient(config.MONGO_URI)[config.MONGO_DB][collection].drop()
    uri, dbn = config.MONGO_URI, config.MONGO_DB

    def write_part(rows):
        from pymongo import MongoClient as MC
        docs = [r.asDict(recursive=True) for r in rows]
        if docs:
            MC(uri)[dbn][collection].insert_many(docs, ordered=False)

    df.foreachPartition(write_part)
    if key_fields:
        MongoClient(config.MONGO_URI)[config.MONGO_DB][collection].create_index(
            [(k, 1) for k in key_fields])
