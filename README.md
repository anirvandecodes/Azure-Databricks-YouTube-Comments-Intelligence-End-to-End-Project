# YouTube Comment Intelligence Pipeline
### End-to-End Databricks Project

A complete data + AI pipeline that turns **any public YouTube channel** into a
searchable intelligence system — built entirely on **Databricks Free Edition**.

---

## What It Does

Enter any YouTube channel handle (e.g. `@databricks`, `@mkbhd`) and the pipeline:

1. **Fetches** video metadata and all public comments via YouTube Data API v3
2. **Transforms** raw data through a Bronze → Silver → Gold medallion architecture using **Spark Declarative Pipelines**
3. **Scores** every comment for sentiment using `ai_analyze_sentiment()` — no model setup needed
4. **Indexes** comments into an Agent Bricks **Knowledge Assistant** for RAG-based Q&A
5. **Surfaces** analytics via a **Genie Space** (natural language → SQL)
6. **Orchestrates** both with a **Supervisor Agent** that routes questions intelligently

---

## Architecture

```
YouTube Data API v3
        │
        ▼
  Unity Catalog Volumes
  (raw JSON + .txt comment files)
        │
        ▼  Spark Declarative Pipelines
  ┌─────────────────────────────────────┐
  │  Bronze  raw_comments               │
  │       ↓  parse + clean              │
  │  Silver  comments                   │
  │       ↓  ai_analyze_sentiment()     │
  │  Gold    comment_metrics            │
  └─────────────────────────────────────┘
        │                    │
        ▼                    ▼
 Knowledge Assistant    Genie Space
 (RAG over comments)   (NL analytics)
        │                    │
        └──────────┬──────────┘
                   ▼
           Supervisor Agent
           (routes questions)
```

---

## Project Structure

```
├── setup/
│   └── catalog_schemas_volumes.py   # One-time setup — run manually once
├── ingestion/
│   ├── youtube_api_ingest.py        # Fetch video metadata → Volume JSON
│   └── comments_ingest.py           # Fetch all comments → Volume .txt + NDJSON
├── pipeline/
│   ├── bronze.py                    # Auto Loader: NDJSON → raw_comments table
│   ├── silver.py                    # Clean + type-cast → comments table
│   └── gold.py                      # Sentiment scoring → comment_metrics MV
├── resources/
│   ├── youtube_job.yml              # Job: api_ingest → comments_ingest → sdp_pipeline
│   ├── youtube_pipeline.yml         # Spark Declarative Pipeline definition
│   └── youtube_dashboard.yml        # AI/BI Dashboard
├── dashboards/
│   └── youtube_intelligence.lvdash.json
├── databricks.yml                   # Bundle config
├── AGENT_BRICKS_SETUP.md            # UI guide for KA + Genie + Supervisor Agent
└── test_apis.py                     # Quick API smoke test
```

---

## Prerequisites

1. **Databricks Free Edition** workspace
2. **YouTube Data API v3 key** — [get one free](https://console.cloud.google.com/apis/library/youtube.googleapis.com)
3. Store the key as a Databricks secret:
   ```bash
   databricks secrets create-scope youtube-pipeline
   databricks secrets put-secret youtube-pipeline youtube-api-key --string-value "<YOUR_API_KEY>"
   ```

---

## Setup (One Time)

Run `setup/catalog_schemas_volumes.py` **manually once** in your workspace.
This creates the `anirvansen_catalog` schemas and volumes — never needs to run again.

---

## Deploy

```bash
databricks bundle deploy
```

---

## Run the Job

```bash
databricks jobs run-now --no-wait --json '{
  "job_id": <JOB_ID>,
  "job_parameters": {
    "channel_handle": "@databricks",
    "max_videos": "10",
    "max_comments_per_video": "0"
  }
}'
```

| Parameter | Default | Description |
|---|---|---|
| `channel_handle` | `@databricks` | Any public YouTube channel |
| `max_videos` | `10` | Videos to process (0 = all) |
| `max_comments_per_video` | `0` | Comments per video (0 = all) |

---

## After the Job

Follow **`AGENT_BRICKS_SETUP.md`** to create:

| Component | Type | Points to |
|---|---|---|
| `youtube-comments-ka` | Knowledge Assistant | `/Volumes/anirvansen_catalog/yt_landing/comments` |
| `YouTube Comment Analytics` | Genie Space | `yt_silver.comments` + `yt_gold.comment_metrics` |
| `Youtube-Intelligence-Agent` | Supervisor Agent | Routes between KA and Genie |

> **Note:** The `comments/` volume contains only per-channel `.txt` subfolders — the pipeline NDJSON files live in `raw_api_responses/_comments_json/` so the KA never sees them. Run any channel handle through the job and it gets its own subfolder indexed automatically.

---

## Example Questions

**Content** (→ Knowledge Assistant):
- "What do people say about Delta Lake in the comments?"
- "Summarise viewer opinions on the most recent video"
- "What complaints appear most across all videos?"

**Analytics** (→ Genie):
- "Which video has the best sentiment score?"
- "Show positive vs negative comment counts per video"
- "Which video has the highest engagement (likes per comment)?"

---

## Tech Stack

| Layer | Technology |
|---|---|
| Storage | Unity Catalog Volumes + Delta Lake |
| Ingestion | YouTube Data API v3 (`commentThreads.list`) |
| Pipeline | Spark Declarative Pipelines (serverless) |
| Sentiment | Databricks `ai_analyze_sentiment()` |
| RAG | Agent Bricks Knowledge Assistant |
| Analytics | Agent Bricks Genie Space |
| Orchestration | Agent Bricks Supervisor Agent |
| Deployment | Databricks Asset Bundles |
| Platform | Databricks Free Edition |
