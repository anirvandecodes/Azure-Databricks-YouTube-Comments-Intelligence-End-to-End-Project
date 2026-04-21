# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 02 · YouTube API Ingestion
# MAGIC
# MAGIC Fetches all public video metadata for the target channel using the
# MAGIC **YouTube Data API v3** and writes raw JSON files to a Unity Catalog Volume.
# MAGIC
# MAGIC **What this notebook does**
# MAGIC 1. Resolves `@channel_handle` → `channel_id` + `uploads_playlist_id`
# MAGIC 2. Paginates through the uploads playlist to get **every** video ID (no cap)
# MAGIC 3. Fetches full metadata in batches of 50 (snippet, statistics, contentDetails, status, topicDetails, player, localizations, recordingDetails)
# MAGIC 4. Saves each batch as a newline-delimited JSON file in the Volume
# MAGIC
# MAGIC **API quota cost**: 1 unit/page for playlist pagination + 1 unit/batch for videos.list
# MAGIC (Previously used search.list at 100 units/page with a ~500-video cap — now removed.)
# MAGIC The free quota is 10,000 units/day — enough for channels with thousands of videos.

# COMMAND ----------
# MAGIC %pip install google-api-python-client==2.143.0 --quiet

# COMMAND ----------

# ── Config ─────────────────────────────────────────────────────────────────────
dbutils.widgets.text("channel_handle", "@databricks", "YouTube Channel Handle")
dbutils.widgets.text("max_videos",     "5",           "Max videos to fetch (0 = all)")

CHANNEL_HANDLE = dbutils.widgets.get("channel_handle").strip()
if not CHANNEL_HANDLE.startswith("@"):
    CHANNEL_HANDLE = "@" + CHANNEL_HANDLE
MAX_VIDEOS = int(dbutils.widgets.get("max_videos"))

CATALOG        = "anirvansen_catalog"
LANDING_SCHEMA = "yt_landing"
CHANNEL_SLUG   = CHANNEL_HANDLE.lstrip("@").replace("/", "_")
CHANNEL_RAW_PATH = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/raw_api_responses/{CHANNEL_SLUG}"

YOUTUBE_API_KEY = dbutils.secrets.get(scope="youtube-pipeline", key="youtube-api-key")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 · Install & Import

# COMMAND ----------

import json
import os
from datetime import datetime, timezone
from googleapiclient.discovery import build

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
print("YouTube API client ready.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 · Resolve Channel Handle → Channel ID + Uploads Playlist

# COMMAND ----------

def get_channel_info(handle: str) -> tuple[str, str, str]:
    """Return (channel_id, display_name, uploads_playlist_id) for a @handle.

    Using contentDetails gives us the uploads playlist, which is the only
    reliable way to retrieve ALL videos — search.list caps at ~500 and costs
    100 quota units per page vs 1 unit for playlistItems.list.
    """
    clean_handle = handle.lstrip("@")
    resp = youtube.channels().list(
        part="id,snippet,contentDetails",
        forHandle=clean_handle,
        maxResults=1
    ).execute()
    items = resp.get("items", [])
    if not items:
        raise ValueError(f"No channel found for handle: {handle}. Check the spelling and make sure the channel is public.")
    item = items[0]
    uploads_playlist_id = item["contentDetails"]["relatedPlaylists"]["uploads"]
    return item["id"], item["snippet"]["title"], uploads_playlist_id


channel_id, channel_name, uploads_playlist_id = get_channel_info(CHANNEL_HANDLE)
print(f"Channel handle      : {CHANNEL_HANDLE}")
print(f"Channel name        : {channel_name}")
print(f"Channel ID          : {channel_id}")
print(f"Uploads playlist ID : {uploads_playlist_id}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 · Collect All Video IDs via Uploads Playlist

# COMMAND ----------

def get_all_video_ids(uploads_playlist_id: str, limit: int = 0) -> list[str]:
    """Paginate playlistItems.list to collect video IDs (newest-first).

    Stops early once `limit` IDs are collected when limit > 0.
    Costs 1 quota unit per page vs 100 for search.list.
    """
    video_ids = []
    next_page_token = None
    page_num = 0

    while True:
        page_num += 1
        fetch_count = 50 if limit == 0 else min(50, limit - len(video_ids))
        resp = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=fetch_count,
            pageToken=next_page_token
        ).execute()

        ids = [item["contentDetails"]["videoId"] for item in resp.get("items", [])]
        video_ids.extend(ids)
        print(f"  Page {page_num}: fetched {len(ids)} IDs (total so far: {len(video_ids)})")

        if limit > 0 and len(video_ids) >= limit:
            break
        next_page_token = resp.get("nextPageToken")
        if not next_page_token:
            break

    return video_ids


video_ids = get_all_video_ids(uploads_playlist_id, limit=MAX_VIDEOS)
limit_msg = f"(limited to {MAX_VIDEOS} most recent)" if MAX_VIDEOS > 0 else "(all videos)"
print(f"\nTotal videos to process: {len(video_ids)} {limit_msg}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4 · Fetch Full Metadata in Batches & Save to Volume

# COMMAND ----------

def fetch_video_details(video_ids: list[str]) -> list[dict]:
    """Call videos.list for a batch of up to 50 IDs with maximum detail."""
    resp = youtube.videos().list(
        part="snippet,statistics,contentDetails,status,topicDetails,player,localizations,recordingDetails",
        id=",".join(video_ids)
    ).execute()
    return resp.get("items", [])


def save_batch_to_volume(batch_data: list[dict], batch_num: int, path: str):
    """Write a batch of video items as a JSON file to a Volume path."""
    dbutils.fs.mkdirs(path)
    file_path = f"{path}/batch_{batch_num:04d}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        for item in batch_data:
            record = {
                "video_id": item["id"],
                "channel_handle": CHANNEL_HANDLE,
                "channel_id": channel_id,
                "channel_name": channel_name,
                "raw_json": json.dumps(item),
                "ingested_at": datetime.now(timezone.utc).isoformat()
            }
            f.write(json.dumps(record) + "\n")  # newline-delimited JSON
    return file_path


BATCH_SIZE = 50
batches = [video_ids[i:i + BATCH_SIZE] for i in range(0, len(video_ids), BATCH_SIZE)]
total_saved = 0

for batch_num, batch in enumerate(batches, start=1):
    items = fetch_video_details(batch)
    file_path = save_batch_to_volume(items, batch_num, CHANNEL_RAW_PATH)
    total_saved += len(items)
    print(f"  Batch {batch_num}/{len(batches)}: {len(items)} videos → {file_path}")

print(f"\nDone! {total_saved} videos saved to {CHANNEL_RAW_PATH}")
print(f"Proceed to notebook 03 to fetch transcripts.")
