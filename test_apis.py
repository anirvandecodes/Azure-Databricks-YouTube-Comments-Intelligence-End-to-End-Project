# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # API Test — YouTube Data API + Comments API
# MAGIC Quick smoke test. Run this before the full job to verify both APIs work.

# COMMAND ----------
# MAGIC %pip install google-api-python-client==2.143.0 --quiet

# COMMAND ----------

from googleapiclient.discovery import build

YOUTUBE_API_KEY = dbutils.secrets.get(scope="youtube-pipeline", key="youtube-api-key")
TEST_HANDLE     = "@databricks"
TEST_VIDEO_ID   = "2zLzCptIr9A"

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 · YouTube Data API — Video Metadata

# COMMAND ----------

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

channel_resp = youtube.channels().list(
    part="id,snippet,contentDetails",
    forHandle=TEST_HANDLE.lstrip("@"),
    maxResults=1
).execute()

channel  = channel_resp["items"][0]
uploads  = channel["contentDetails"]["relatedPlaylists"]["uploads"]
print(f"Channel : {channel['snippet']['title']}  (id={channel['id']})")
print(f"Uploads : {uploads}")

pl_resp  = youtube.playlistItems().list(part="contentDetails", playlistId=uploads, maxResults=3).execute()
vid_ids  = [i["contentDetails"]["videoId"] for i in pl_resp["items"]]
print(f"First 3 video IDs: {vid_ids}")

meta     = youtube.videos().list(part="snippet,statistics", id=",".join(vid_ids)).execute()
for v in meta["items"]:
    print(f"  {v['id']} | {v['snippet']['title'][:55]} | views={v['statistics'].get('viewCount','?')}")

print("\n✅ YouTube Data API — OK")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 · Comments API

# COMMAND ----------

print(f"Fetching comments for {TEST_VIDEO_ID}...\n")
try:
    resp = youtube.commentThreads().list(
        part="snippet",
        videoId=TEST_VIDEO_ID,
        maxResults=5,
        order="relevance"
    ).execute()

    comments = resp.get("items", [])
    print(f"Top {len(comments)} comments:")
    for item in comments:
        s = item["snippet"]["topLevelComment"]["snippet"]
        print(f"  [{s['authorDisplayName']}] (likes={s['likeCount']}): {s['textOriginal'][:80]}")

    print("\n✅ Comments API — OK")
except Exception as e:
    if "commentsDisabled" in str(e):
        print("Comments are disabled for this video — try a different video ID.")
    else:
        print(f"ERROR: {type(e).__name__}: {e}")
