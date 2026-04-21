# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # One-Time Setup — Catalog, Schemas & Volumes
# MAGIC Run this once manually before the first job run.

# COMMAND ----------

# ── Config ─────────────────────────────────────────────────────────────────────
CATALOG        = "anirvansen_catalog"
BRONZE_SCHEMA  = "yt_bronze"
SILVER_SCHEMA  = "yt_silver"
GOLD_SCHEMA    = "yt_gold"
LANDING_SCHEMA = "yt_landing"

# COMMAND ----------

# ── Schemas ────────────────────────────────────────────────────────────────────
for schema in [BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, LANDING_SCHEMA]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"  schema  ready : {CATALOG}.{schema}")

# ── Volumes ────────────────────────────────────────────────────────────────────
for vol, comment in [
    ("raw_api_responses", "Raw YouTube API JSON — video metadata per channel"),
    ("comments_raw",      "Raw NDJSON comment files per video — input to SDP pipeline"),
    ("comments",          "Per-channel .txt comment files — indexed by Knowledge Assistant"),
]:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{LANDING_SCHEMA}.{vol} COMMENT '{comment}'")
    print(f"  volume  ready : {CATALOG}.{LANDING_SCHEMA}.{vol}")

# ── Subdirectories ─────────────────────────────────────────────────────────────
for path in [
    f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/raw_api_responses/_schema_checkpoints",
    f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/comments_raw",
]:
    dbutils.fs.mkdirs(path)
    print(f"  dir     ready : {path}")

# COMMAND ----------

print("\nSchemas:")
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG} LIKE 'yt_*'"))
print("\nVolumes:")
display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{LANDING_SCHEMA}"))
print("\nSetup complete.")
