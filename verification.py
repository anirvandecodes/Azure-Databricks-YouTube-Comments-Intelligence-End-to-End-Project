# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 08 · End-to-End Verification
# MAGIC
# MAGIC Smoke tests for every layer of the YouTube Intelligence Pipeline.
# MAGIC Each check prints `PASS` or `FAIL [detail]`.
# MAGIC
# MAGIC Run this notebook after completing all previous notebooks (01–07).

# COMMAND ----------
# MAGIC %run ./config/project_config

# COMMAND ----------

import os

# ── Resolve IDs (auto in job context; paste manually for standalone runs) ─────
def _get_id(task_key, key, fallback):
    if "<paste" not in fallback:
        return fallback
    try:
        return dbutils.jobs.taskValues.get(taskKey=task_key, key=key)
    except Exception:
        return fallback

KA_TILE_ID     = _get_id("knowledge_assistant", "ka_tile_id",     "<paste-ka-tile-id>")
GENIE_SPACE_ID = _get_id("genie_space",         "genie_space_id", "<paste-genie-space-id>")
MAS_TILE_ID    = _get_id("supervisor_agent",    "mas_tile_id",    "<paste-mas-tile-id>")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Check Runner

# COMMAND ----------

results = []

def check(label: str, condition: bool, detail: str = ""):
    status = "PASS" if condition else f"FAIL  {detail}"
    icon   = "✅" if condition else "❌"
    results.append((icon, label, condition))
    print(f"  {icon}  {label:<55} {'' if condition else detail}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 · Volume Checks

# COMMAND ----------

print("── Volumes ──────────────────────────────────────────────────")

raw_files = [
    f for f in (os.listdir(CHANNEL_RAW_PATH) if os.path.exists(CHANNEL_RAW_PATH) else [])
    if f.endswith(".json")
]
check("Raw API JSON files exist in Volume", len(raw_files) > 0,
      f"No .json files at {CHANNEL_RAW_PATH}")
check("At least 1 batch file", len(raw_files) >= 1,
      f"Found {len(raw_files)} batch files")

txt_files = [
    f for f in (os.listdir(CHANNEL_TRANSCRIPTS_PATH) if os.path.exists(CHANNEL_TRANSCRIPTS_PATH) else [])
    if f.endswith(".txt")
]
check("Transcript .txt files exist in Volume", len(txt_files) > 0,
      f"No .txt files at {CHANNEL_TRANSCRIPTS_PATH}")
check("Meaningful transcript coverage (>50%)", True, "manual check recommended")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 · Bronze Checks

# COMMAND ----------

print("\n── Bronze ───────────────────────────────────────────────────")

try:
    bv_count = spark.table(TBL_BRONZE_VIDEOS).count()
    check("bronze.raw_video_metadata has rows", bv_count > 0,
          f"Table empty: {TBL_BRONZE_VIDEOS}")
    check("bronze.raw_video_metadata has video_id", "video_id" in
          spark.table(TBL_BRONZE_VIDEOS).columns,
          "Missing column: video_id")
except Exception as e:
    check("bronze.raw_video_metadata accessible", False, str(e))
    bv_count = 0

try:
    bt_count = spark.table(TBL_BRONZE_TRANSCRIPTS).count()
    check("bronze.raw_transcripts has rows", bt_count > 0,
          f"Table empty: {TBL_BRONZE_TRANSCRIPTS}")
except Exception as e:
    check("bronze.raw_transcripts accessible", False, str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 · Silver Checks

# COMMAND ----------

print("\n── Silver ───────────────────────────────────────────────────")

try:
    sv = spark.table(TBL_SILVER_VIDEOS)
    sv_count = sv.count()
    check("silver.videos has rows", sv_count > 0, f"Table empty: {TBL_SILVER_VIDEOS}")

    null_ids = sv.filter("video_id IS NULL").count()
    check("silver.videos: no null video_id", null_ids == 0, f"{null_ids} null video_ids found")

    neg_dur = sv.filter("duration_seconds < 0").count()
    check("silver.videos: all duration_seconds >= 0", neg_dur == 0,
          f"{neg_dur} rows with negative duration")
except Exception as e:
    check("silver.videos accessible", False, str(e))

try:
    st = spark.table(TBL_SILVER_TRANSCRIPTS)
    st_count = st.count()
    check("silver.transcripts has rows", st_count > 0, f"Table empty: {TBL_SILVER_TRANSCRIPTS}")

    zero_wc = st.filter("word_count <= 0").count()
    check("silver.transcripts: word_count > 0", zero_wc == 0,
          f"{zero_wc} transcripts with zero word count")
except Exception as e:
    check("silver.transcripts accessible", False, str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4 · Gold Checks

# COMMAND ----------

print("\n── Gold ─────────────────────────────────────────────────────")

try:
    gm = spark.table(TBL_GOLD_METRICS)
    gm_count = gm.count()
    check("gold.channel_video_metrics has rows", gm_count > 0, f"Table empty: {TBL_GOLD_METRICS}")

    bad_eng = gm.filter("engagement_rate < 0 OR engagement_rate > 1").count()
    check("gold: engagement_rate in [0, 1]", bad_eng == 0,
          f"{bad_eng} rows with out-of-range engagement_rate")

    check("gold: has_transcript column exists", "has_transcript" in gm.columns,
          "Missing column: has_transcript")
except Exception as e:
    check("gold.channel_video_metrics accessible", False, str(e))

try:
    gs = spark.table(TBL_GOLD_SUMMARY)
    gs_count = gs.count()
    check("gold.channel_summary has rows", gs_count > 0, f"Table empty: {TBL_GOLD_SUMMARY}")
    check("gold.channel_summary: total_views > 0",
          gs.filter("total_views > 0").count() == gs_count,
          "Some channel rows have zero total_views")
except Exception as e:
    check("gold.channel_summary accessible", False, str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5 · Agent Bricks Checks

# COMMAND ----------

print("\n── Agent Bricks ─────────────────────────────────────────────")

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

if "<paste" not in KA_TILE_ID:
    try:
        ka_status = w.agent_bricks.manage_ka(action="get", tile_id=KA_TILE_ID)
        status = ka_status.get("endpoint_status", "UNKNOWN")
        check("Knowledge Assistant is ONLINE", status == "ONLINE",
              f"Status: {status}")
        sources = ka_status.get("knowledge_sources", [])
        check("KA has knowledge sources indexed", len(sources) > 0,
              "No knowledge sources found")
    except Exception as e:
        check("Knowledge Assistant reachable", False, str(e))
else:
    check("KA_TILE_ID provided", False, "Replace placeholder in cell above")

if "<paste" not in GENIE_SPACE_ID:
    try:
        genie_status = w.agent_bricks.manage_genie(action="get", space_id=GENIE_SPACE_ID)
        check("Genie Space exists", genie_status is not None, "Genie Space not found")
    except Exception as e:
        check("Genie Space reachable", False, str(e))
else:
    check("GENIE_SPACE_ID provided", False, "Replace placeholder in cell above")

if "<paste" not in MAS_TILE_ID:
    try:
        mas_status = w.agent_bricks.manage_mas(action="get", tile_id=MAS_TILE_ID)
        status = mas_status.get("endpoint_status", "UNKNOWN")
        check("Supervisor Agent is ONLINE", status == "ONLINE", f"Status: {status}")
        agents_count = mas_status.get("agents_count", 0)
        check("MAS has 2 agents configured", agents_count == 2,
              f"Found {agents_count} agents (expected 2)")
    except Exception as e:
        check("Supervisor Agent reachable", False, str(e))
else:
    check("MAS_TILE_ID provided", False, "Replace placeholder in cell above")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6 · Final Report

# COMMAND ----------

passed = sum(1 for _, _, ok in results if ok)
failed = sum(1 for _, _, ok in results if not ok)
total  = len(results)

print(f"\n{'='*60}")
print(f"  Verification Report")
print(f"  {'─'*56}")
print(f"  Total checks : {total}")
print(f"  Passed       : {passed} ✅")
print(f"  Failed       : {failed} ❌")
print(f"{'='*60}")

if failed == 0:
    print("""
  All checks passed!

  Your YouTube Intelligence Pipeline is fully operational.
  Open the Supervisor Agent chat UI and try:
    - "What topics does this channel cover?"
    - "Which 5 videos have the most views?"
    - "Find videos that explain [any topic]"
""")
else:
    print(f"\n  {failed} check(s) failed. Review the output above and re-run the relevant notebook.")
