# Agent Bricks Setup — UI Guide

Run this after the job completes (`api_ingest → comments_ingest → sdp_pipeline`).

---

## Step 1 · Knowledge Assistant

Answers free-text questions about what people are saying in the comments (RAG over `.txt` files).

1. In the Databricks left sidebar click **Agent Bricks**
2. Click **Create** → **Knowledge Assistant**
3. Fill in:
   - **Name**: `youtube-comments-ka`
   - **Volume path**: `/Volumes/anirvansen_catalog/yt_landing/comments`
     > Parent directory — each channel gets its own subfolder (`databricks/`, `mkbhd/`, etc.) and all are indexed automatically.
   - **Description**:
     ```
     This knowledge source contains YouTube viewer comment data from ingested channels.
     Each file represents one YouTube video and contains the complete set of public comments
     posted on that video.

     Each file includes:
     - Video title and full YouTube URL
     - Channel handle and video publish date
     - All top-level viewer comments with author name and like count
     - Threaded replies to comments with author name and like count

     Topics in the comments reflect the content of the channel's videos —
     covering whatever subjects, products, or themes the channel covers.
     Comments include viewer reactions, questions, opinions, praise,
     complaints, feature requests, and discussion threads.

     Use this source to answer questions about:
     - What viewers think or say about a specific video or topic
     - Common themes, complaints, or praise across videos
     - Specific comments or opinions expressed by viewers
     - Whether viewers are asking for follow-up content
     - Overall tone and discussion quality for a given video
     ```
   - **Instructions**:
     ```
     Always cite the video title and YouTube URL when answering.
     When asked about opinions or sentiment, quote actual comments as evidence.
     If multiple videos are relevant, list all of them.
     When the user specifies a channel (e.g. "@databricks"), restrict answers to that channel only.
     If a question is not answered by the comments, say so clearly.
     ```
4. Click **Create** and wait for status **Online** (2–5 min)
5. **Copy the Tile ID** — needed in Step 3

---

## Step 2 · Genie Space

Answers structured analytics questions using the gold and silver tables.

1. In the Databricks left sidebar click **Agent Bricks** → **Genie**
2. Click **New Space**
3. Fill in:
   - **Name**: `YouTube Comment Analytics — All Channels`
   - **Tables**: Add both:
     - `anirvansen_catalog.yt_silver.comments`
     - `anirvansen_catalog.yt_gold.comment_metrics`
   - **Description**:
     ```
     Analytics over YouTube comments.
     - comment_metrics: per-video aggregates — comment counts, sentiment scores, engagement.
     - comments: every individual comment with full text, author name, video, likes, and timestamp.
     Filter by channel_handle to scope to a specific channel.
     ```
   - **Instructions**:
     ```
     Table: anirvansen_catalog.yt_gold.comment_metrics
     Use for aggregated / ranked questions.
     Columns:
     - sentiment_score: -1.0 (all negative) to +1.0 (all positive)
     - positive_comments / negative_comments / neutral_comments: counts by sentiment
     - total_comments: top-level comments only (replies excluded)
     - total_replies: reply count
     - avg_likes_per_comment: engagement proxy
     - channel_handle always starts with '@' (e.g. '@databricks')

     Table: anirvansen_catalog.yt_silver.comments
     Use for questions about individual comments.
     Columns:
     - text: the actual comment content
     - author_name: who posted it
     - title + video_url: which video it was on
     - like_count: how many likes the comment received
     - is_reply: true if it is a reply to another comment
     - published_at: when the comment was posted

     When asked about "most discussed", order by total_comments DESC on comment_metrics.
     When asked to show actual comments (e.g. "show negative comments", "most liked comment"),
     query the comments table and filter/order accordingly.
     When the user specifies a channel, always filter by channel_handle.
     ```
4. Add these **Sample Questions**:
   - `Which video has the most positive comments?`
   - `Show sentiment score for each video`
   - `Which video has the highest engagement (likes per comment)?`
   - `Compare positive vs negative comments across all videos`
   - `Which video has the most total comments?`
   - `Show me the most liked comments`
   - `Show all comments for the most recent video`
5. Click **Save**
6. **Copy the Space ID** from the URL — needed in Step 3

---

## Step 3 · Supervisor Agent

Routes questions to the right agent automatically.

1. In the Databricks left sidebar click **Agent Bricks**
2. Click **Create** → **Supervisor Agent**
3. Fill in:
   - **Name**: `Youtube-Intelligence-Agent`
   - **Description**:
     ```
     An AI agent that answers any question about YouTube channel comments.
     Routes content questions (what people say) to the Knowledge Assistant
     and analytics questions (metrics, counts, sentiment scores) to Genie.
     Works across all ingested channels.
     ```
4. Add **Agent 1 — Knowledge Assistant**:
   - Type: `Knowledge Assistant`
   - Tile ID: *(paste from Step 1)*
   - Description:
     ```
     Use for CONTENT questions: what do people say about X, what opinions are
     expressed, summarise the comments on a video, find comments mentioning
     a topic, what do viewers think about Y.
     ```
5. Add **Agent 2 — Genie Space**:
   - Type: `Genie Space`
   - Space ID: *(paste from Step 2)*
   - Description:
     ```
     Use for ANALYTICS questions: sentiment scores, comment counts, likes per
     comment, which video has best/worst sentiment, ranking videos by engagement,
     positive vs negative breakdown, show me actual comments for a video.
     ```
6. Add **Instructions**:
   ```
   Route to Knowledge Assistant for: "what do people say about", "summarise comments",
   "what opinions", "find comments that mention", "what do viewers think".

   Route to Genie for: "sentiment score", "most positive", "most negative",
   "how many comments", "highest engagement", "rank videos by",
   "show me comments", "most liked comment".

   For questions needing both: use Genie first to find the video, then KA for detail.
   ```
7. Add **Example Questions**:
   - `What do people say about the most recent video?`
   - `Which video has the most positive comments?`
   - `Summarise viewer opinions on the most popular video`
   - `Compare sentiment scores across all videos`
   - `Show me the most liked comments`
8. Click **Create** and wait for status **Online**

---

## Try It Out

| Question | Routes to |
|---|---|
| `What do viewers say about the most recent video?` | Knowledge Assistant |
| `Which video has the best sentiment score?` | Genie |
| `Summarise comments on the most popular video` | Knowledge Assistant |
| `Show positive vs negative comment counts` | Genie |
| `What complaints appear most in the comments?` | Knowledge Assistant |
| `Show me the most liked comment across all videos` | Genie |
| `Which video has the most engagement?` | Genie |
