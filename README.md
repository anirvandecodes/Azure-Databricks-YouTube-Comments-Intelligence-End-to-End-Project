# YouTube Comment Intelligence Pipeline
### End-to-End Databricks Project

A complete data + AI pipeline that turns **any public YouTube channel** into a searchable intelligence system — built entirely on **Databricks Free Edition**.

---

## What It Does

Enter any YouTube channel handle (e.g. `@mkbhd`, `@fireship`) and the pipeline:

1. **Fetches** video metadata via YouTube Data API v3 (`search.list` + `videos.list`)
2. **Fetches** all public comments for each video (`commentThreads.list`) including replies
3. **Transforms** raw data through a Bronze → Silver → Gold medallion architecture using **Spark Declarative Pipelines**
4. **Scores** every comment for sentiment using `ai_analyze_sentiment()` — no model setup needed
5. **Indexes** comments into an Agent Bricks **Knowledge Assistant** for RAG-based Q&A
6. **Surfaces** analytics via a **Genie Space** (natural language → SQL)
7. **Orchestrates** both with a **Supervisor Agent** that routes questions intelligently
8. **Visualises** metrics on an **AI/BI Dashboard** — sentiment, engagement, channel comparison

---

## Architecture

```
YouTube Data API v3
        │
        ├─── video metadata (search.list / videos.list)
        │         └──► Volume: raw_api_responses/{channel_slug}/*.json
        │
        └─── comments (commentThreads.list)
                  ├──► Volume: comments_raw/{video_id}.json   (NDJSON → SDP)
                  └──► Volume: comments/{channel_slug}/*.txt  (text → KA)

                          │
                          ▼  Spark Declarative Pipelines
              ┌───────────────────────────────────────┐
              │  Bronze  yt_bronze.raw_comments        │
              │       ↓  Auto Loader (cloudFiles)      │
              │  Silver  yt_silver.comments            │
              │       ↓  type-cast + quality gates     │
              │  Gold    yt_gold.comment_metrics       │
              │       ↓  ai_analyze_sentiment()        │
              └───────────────────────────────────────┘
                          │                    │
                          ▼                    ▼
                 Knowledge Assistant      Genie Space
                 (RAG over .txt files)   (NL → SQL)
                          │                    │
                          └──────────┬──────────┘
                                     ▼
                             Supervisor Agent
                          (routes questions automatically)
                                     │
                                     ▼
                             AI/BI Dashboard
                          (sentiment + engagement)
```

---

## Project Structure

```
├── setup/
│   └── catalog_schemas_volumes.py    # One-time setup — run manually in workspace
├── ingestion/
│   ├── youtube_api_ingest.py         # Fetch video metadata → Volume JSON
│   └── comments_ingest.py            # Fetch all comments → .txt (KA) + NDJSON (SDP)
├── pipeline/
│   ├── bronze.py                     # Auto Loader: NDJSON → yt_bronze.raw_comments
│   ├── silver.py                     # Clean + type-cast → yt_silver.comments
│   └── gold.py                       # Sentiment aggregation → yt_gold.comment_metrics
├── resources/
│   ├── youtube_job.yml               # Workflow: api_ingest → comments_ingest → sdp_pipeline
│   ├── youtube_pipeline.yml          # Spark Declarative Pipeline definition
│   └── youtube_dashboard.yml         # AI/BI Dashboard resource
├── dashboards/
│   └── youtube_intelligence.lvdash.json
├── databricks.yml                    # Databricks Asset Bundle config
├── AGENT_BRICKS_SETUP.md             # Step-by-step UI guide: KA + Genie + Supervisor Agent
├── test_apis.py                      # Quick API smoke test
└── verification.py                   # Post-run data verification queries
```

---

## Prerequisites

1. **Databricks Free Edition** workspace with a Unity Catalog enabled
2. **YouTube Data API v3 key** — [get one free](https://console.cloud.google.com/apis/library/youtube.googleapis.com)
3. A Unity Catalog **catalog** pre-created in your workspace (e.g. `my_catalog`)
4. Databricks CLI installed and configured:
   ```bash
   databricks configure
   ```
5. Store your YouTube API key as a Databricks secret:
   ```bash
   databricks secrets create-scope youtube-pipeline
   databricks secrets put-secret youtube-pipeline youtube-api-key --string-value "<YOUR_API_KEY>"
   ```

---

## Configuration

Open `databricks.yml` and update the catalog variable to match your Unity Catalog:

```yaml
variables:
  catalog:
    default: your_catalog_name
```

All notebooks and pipeline files read from this variable — no other changes needed.

---

## Setup (One Time)

Run `setup/catalog_schemas_volumes.py` **manually once** in your Databricks workspace.

This creates:
- Schemas: `yt_bronze`, `yt_silver`, `yt_gold`, `yt_landing`
- Volumes:
  - `yt_landing.raw_api_responses` — video metadata JSON per channel
  - `yt_landing.comments_raw` — NDJSON comment files consumed by the SDP pipeline
  - `yt_landing.comments` — per-channel `.txt` files indexed by the Knowledge Assistant

> Only run this once. The job workflow does **not** include this step.

---

## Deploy

```bash
databricks bundle deploy
```

This deploys the job, pipeline, and dashboard to your workspace.

---

## Run the Job

Via CLI:
```bash
databricks jobs run-now --no-wait --json '{
  "job_id": <JOB_ID>,
  "job_parameters": {
    "channel_handle": "@mkbhd",
    "max_videos": "10",
    "max_comments_per_video": "0"
  }
}'
```

Or trigger it from the **Workflows** UI in Databricks.

| Parameter | Default | Description |
|---|---|---|
| `channel_handle` | `@databricks` | Any public YouTube channel handle |
| `max_videos` | `10` | Number of recent videos to process (`0` = all) |
| `max_comments_per_video` | `0` | Max comments per video (`0` = all) |

### Job Flow

```
api_ingest  →  comments_ingest  →  sdp_pipeline
```

| Task | What it does |
|---|---|
| `api_ingest` | Fetches recent video metadata for the channel → writes to `raw_api_responses/{channel}/` |
| `comments_ingest` | Fetches all comments per video → writes `.txt` to `comments/{channel}/` and NDJSON to `comments_raw/` |
| `sdp_pipeline` | Runs Bronze → Silver → Gold Spark Declarative Pipeline |

---

## Pipeline Stages

### Bronze — `yt_bronze.raw_comments`
Auto Loader streams NDJSON files from `yt_landing.comments_raw` into a raw Delta table. Clustered by `channel_handle`.

### Silver — `yt_silver.comments`
Cleans and type-casts the raw data:
- `like_count` → `LongType`
- `published_at` → `TimestampType`
- `is_reply` → `BooleanType`
- Quality gates: `comment_id IS NOT NULL`, `LENGTH(text) > 0`

### Gold — `yt_gold.comment_metrics`
Materialized View aggregated per video:
- `total_comments`, `total_replies`, `avg_likes_per_comment`
- `positive_comments`, `negative_comments`, `neutral_comments` — via `ai_analyze_sentiment()`
- `sentiment_score` — range `-1.0` (all negative) to `+1.0` (all positive)
- `latest_comment_at`, `metrics_computed_at`

---

## After the Job — Agent Bricks Setup

Follow **`AGENT_BRICKS_SETUP.md`** to create the three AI components in the Databricks UI:

| Component | Type | What it uses |
|---|---|---|
| `youtube-comments-ka` | Knowledge Assistant | `.txt` files in `yt_landing.comments` volume |
| `YouTube Comment Analytics — All Channels` | Genie Space | `yt_silver.comments` + `yt_gold.comment_metrics` |
| `Youtube-Intelligence-Agent` | Supervisor Agent | Routes between KA and Genie |

> The `comments/` volume contains only per-channel `.txt` subfolders. NDJSON pipeline files live in the separate `comments_raw/` volume so the Knowledge Assistant never sees them. Run any channel through the job and it gets its own subfolder automatically indexed.

---

## Example Questions

**Content** (→ Knowledge Assistant):
- "What do people say about the most recent video?"
- "Summarise viewer opinions on the most popular video"
- "What complaints appear most across all videos?"
- "Are viewers asking for follow-up content?"

**Analytics** (→ Genie):
- "Which video has the best sentiment score?"
- "Show positive vs negative comment counts per video"
- "Which video has the highest engagement (likes per comment)?"
- "Compare sentiment scores across all videos"
- "Show me the most liked comments"

**Supervisor Agent handles both automatically** — ask anything and it routes to the right agent.

---

## Dashboard

The AI/BI Dashboard (`youtube_intelligence.lvdash.json`) is deployed automatically via `databricks bundle deploy`.

Pages:
- **Sentiment Pulse** — sentiment score per video, positive/negative breakdown, trend over time
- **Engagement & Reach** — top videos by comment count, avg likes per comment, reply rate
- **Global Filters** — filter by channel handle across all charts

---

## Tech Stack

| Layer | Technology |
|---|---|
| Storage | Unity Catalog Volumes + Delta Lake |
| Ingestion | YouTube Data API v3 (`commentThreads.list`) |
| Pipeline | Spark Declarative Pipelines (serverless) |
| Sentiment Analysis | Databricks `ai_analyze_sentiment()` |
| RAG Q&A | Agent Bricks Knowledge Assistant |
| Natural Language Analytics | Agent Bricks Genie Space |
| Agent Orchestration | Agent Bricks Supervisor Agent |
| Visualisation | Databricks AI/BI Dashboard |
| Deployment | Databricks Asset Bundles |
| Platform | Databricks Free Edition |
