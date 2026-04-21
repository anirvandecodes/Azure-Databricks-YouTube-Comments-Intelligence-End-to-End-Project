# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Layer — Raw comments from Volume

# COMMAND ----------

from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, BooleanType

CATALOG        = "anirvansen_catalog"
BRONZE_SCHEMA  = "yt_bronze"
LANDING_SCHEMA = "yt_landing"

COMMENTS_RAW_JSON  = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/comments_raw"
SCHEMA_CP_PATH     = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/raw_api_responses/_schema_checkpoints"

# COMMAND ----------

_COMMENTS_SCHEMA = StructType([
    StructField("comment_id",     StringType()),
    StructField("video_id",       StringType()),
    StructField("channel_handle", StringType()),
    StructField("author_name",    StringType()),
    StructField("text",           StringType()),
    StructField("like_count",     LongType()),
    StructField("reply_count",    IntegerType()),
    StructField("published_at",   StringType()),
    StructField("is_reply",       BooleanType()),
    StructField("parent_id",      StringType()),
    StructField("video_title",    StringType()),
    StructField("video_url",      StringType()),
    StructField("ingested_at",    StringType()),
])

@dp.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.raw_comments",
    cluster_by=["channel_handle"],
    comment="Raw YouTube comments from commentThreads.list API"
)
def bronze_raw_comments():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{SCHEMA_CP_PATH}/comments")
            .option("cloudFiles.partitionColumns", "")
            .schema(_COMMENTS_SCHEMA)
            .load(COMMENTS_RAW_JSON)
            .withColumn("_ingested_at", F.current_timestamp())
    )
