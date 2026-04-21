# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Comments Ingestion
# MAGIC
# MAGIC Fetches all public comments for the most recent videos on a YouTube channel
# MAGIC using the **YouTube Data API v3** (`commentThreads.list`).
# MAGIC
# MAGIC **What this notebook does**
# MAGIC 1. Reads the video list already fetched by `youtube_api_ingest.py` from the Volume
# MAGIC 2. For each video, paginates through all comments (top-level + replies)
# MAGIC 3. Writes two outputs per video:
# MAGIC    - `.txt` file for the Knowledge Assistant
# MAGIC    - NDJSON file for the SDP pipeline (Bronze layer)

# COMMAND ----------
# MAGIC %pip install google-api-python-client==2.143.0 --quiet

# COMMAND ----------

# ── Config ─────────────────────────────────────────────────────────────────────
dbutils.widgets.text("channel_handle",        "@databricks", "YouTube Channel Handle")
dbutils.widgets.text("max_videos",            "10",          "Max videos to process (0 = all)")
dbutils.widgets.text("max_comments_per_video", "0",          "Max comments per video (0 = all)")

CHANNEL_HANDLE         = dbutils.widgets.get("channel_handle").strip()
if not CHANNEL_HANDLE.startswith("@"):
    CHANNEL_HANDLE = "@" + CHANNEL_HANDLE
MAX_VIDEOS             = int(dbutils.widgets.get("max_videos"))
MAX_COMMENTS_PER_VIDEO = int(dbutils.widgets.get("max_comments_per_video"))

CATALOG        = "anirvansen_catalog"
LANDING_SCHEMA = "yt_landing"
CHANNEL_SLUG   = CHANNEL_HANDLE.lstrip("@").replace("/", "_")
CHANNEL_RAW_PATH      = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/raw_api_responses/{CHANNEL_SLUG}"
CHANNEL_COMMENTS_PATH = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/comments/{CHANNEL_SLUG}"
COMMENTS_RAW_JSON     = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/comments_raw"

YOUTUBE_API_KEY = dbutils.secrets.get(scope="youtube-pipeline", key="youtube-api-key")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 · Load Video List from Volume

# COMMAND ----------

import json
import os
import re
from datetime import datetime, timezone
from googleapiclient.discovery import build

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
print("YouTube API client ready.")


def load_videos(path: str) -> list[dict]:
    videos = []
    if not os.path.exists(path):
        raise FileNotFoundError(f"No raw API files at {path}. Run youtube_api_ingest first.")
    for filename in sorted(os.listdir(path)):
        if not filename.endswith(".json"):
            continue
        with open(f"{path}/{filename}", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                raw = json.loads(rec["raw_json"])
                videos.append({
                    "video_id":    rec["video_id"],
                    "title":       raw["snippet"]["title"],
                    "published_at": raw["snippet"]["publishedAt"],
                    "video_url":   f"https://www.youtube.com/watch?v={rec['video_id']}",
                })
    return videos


all_videos = load_videos(CHANNEL_RAW_PATH)
videos = all_videos[:MAX_VIDEOS] if MAX_VIDEOS > 0 else all_videos
print(f"Loaded {len(all_videos)} videos — processing {len(videos)}.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 · Fetch Comments & Write Outputs

# COMMAND ----------

def sanitise(title: str, max_len: int = 60) -> str:
    safe = re.sub(r'[^\w\s\-]', '', title).strip()
    return re.sub(r'\s+', '_', safe)[:max_len]


def fetch_comments(video_id: str, limit: int = 0) -> list[dict]:
    """Paginate commentThreads.list and return all comment records."""
    comments = []
    page_token = None
    while True:
        resp = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            maxResults=100,
            order="relevance",
            pageToken=page_token
        ).execute()

        for item in resp.get("items", []):
            top = item["snippet"]["topLevelComment"]["snippet"]
            comments.append({
                "comment_id":   item["id"],
                "video_id":     video_id,
                "channel_handle": CHANNEL_HANDLE,
                "author_name":  top["authorDisplayName"],
                "text":         top["textOriginal"],
                "like_count":   int(top.get("likeCount", 0)),
                "reply_count":  int(item["snippet"].get("totalReplyCount", 0)),
                "published_at": top["publishedAt"],
                "is_reply":     False,
                "parent_id":    None,
            })
            # Include replies if present
            for reply in item.get("replies", {}).get("comments", []):
                rs = reply["snippet"]
                comments.append({
                    "comment_id":   reply["id"],
                    "video_id":     video_id,
                    "channel_handle": CHANNEL_HANDLE,
                    "author_name":  rs["authorDisplayName"],
                    "text":         rs["textOriginal"],
                    "like_count":   int(rs.get("likeCount", 0)),
                    "reply_count":  0,
                    "published_at": rs["publishedAt"],
                    "is_reply":     True,
                    "parent_id":    item["id"],
                })

        page_token = resp.get("nextPageToken")
        if not page_token or (limit > 0 and len(comments) >= limit):
            break

    return comments[:limit] if limit > 0 else comments


def write_txt(video: dict, comments: list[dict], out_path: str):
    dbutils.fs.mkdirs(out_path)
    filepath = f"{out_path}/{video['video_id']}_{sanitise(video['title'])}.txt"
    top_level = [c for c in comments if not c["is_reply"]]
    lines = [
        f"Title: {video['title']}",
        f"Video URL: {video['video_url']}",
        f"Channel: {CHANNEL_HANDLE}",
        f"Published: {video['published_at'][:10]}",
        f"Total Comments Fetched: {len(top_level)}",
        "",
        "COMMENTS:",
    ]
    for c in top_level:
        lines.append(f"[{c['author_name']}]: {c['text']} (likes: {c['like_count']})")
        for r in [x for x in comments if x["is_reply"] and x["parent_id"] == c["comment_id"]]:
            lines.append(f"  -> [{r['author_name']}]: {r['text']} (likes: {r['like_count']})")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def write_ndjson(video: dict, comments: list[dict], out_path: str):
    dbutils.fs.mkdirs(out_path)
    filepath = f"{out_path}/{video['video_id']}.json"
    now = datetime.now(timezone.utc).isoformat()
    with open(filepath, "w", encoding="utf-8") as f:
        for c in comments:
            record = {**c, "video_title": video["title"],
                      "video_url": video["video_url"], "ingested_at": now}
            f.write(json.dumps(record) + "\n")


# ── Main loop ──────────────────────────────────────────────────────────────────
fetched = skipped = errors = 0

for i, video in enumerate(videos, 1):
    vid = video["video_id"]
    try:
        comments = fetch_comments(vid, limit=MAX_COMMENTS_PER_VIDEO)
        if not comments:
            skipped += 1
            print(f"  [{i}/{len(videos)}] {vid} — 0 comments, skipping")
            continue
        write_txt(video, comments, CHANNEL_COMMENTS_PATH)
        write_ndjson(video, comments, COMMENTS_RAW_JSON)
        fetched += 1
        print(f"  [{i}/{len(videos)}] {vid} — {len(comments)} comments")
    except Exception as e:
        if "commentsDisabled" in str(e) or "disabled comments" in str(e).lower():
            skipped += 1
            print(f"  [{i}/{len(videos)}] {vid} — comments disabled, skipping")
        else:
            errors += 1
            print(f"  ERROR {vid}: {type(e).__name__}: {e}")

print(f"\n{'='*50}")
print(f"  Comments ingestion complete")
print(f"  Fetched  : {fetched} videos")
print(f"  Skipped  : {skipped} (comments disabled)")
print(f"  Errors   : {errors}")
print(f"  .txt (KA): {CHANNEL_COMMENTS_PATH}")
print(f"  JSON(SDP): {COMMENTS_RAW_JSON}")
print(f"{'='*50}")
