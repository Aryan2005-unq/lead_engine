# 🚀 Unified Streaming Pipeline

This directory contains the lead scraping and verification engine built specifically with ServiceNow rendering safeguards node.

## 📋 Architecture Overview

The pipeline executes as a **Single-Phase Continuous Traversal loop**, explicitly designed to overcome ServiceNow's dynamic restrictions:

1. **Row Clicking Traversal:** Direct deep-link visitation is locked on Guest profiles. To populate complete profile forms, the pipeline reads the main tables, **clicks a row**, and extracts the element when loaded.
2. **Fixed Navigation (`goto_page`):** 
   ServiceNow breaks AngularJS pagination states on common browser `.go_back()` instructions. The script fixes this by **navigating fresh to the `/rmd_listings` index** and clicking "Next page" iterate loops sequentially up to the running page triggers absolute accurately!
3. **Fire-and-Forget Verification Process:** 
   - **DB Saving:** Lead is inserted immediately (state = `Pending`) as the extraction is confirmed offloaded live.
   - **FCC Validation:** Operates asynchronously inside curl background buffers without blocking page transitions forwards layout streamers!

---

## ⚙️ Configuration Parameters

| Parameter | Meaning |
| :--- | :--- |
| `FCC_URL` | Main index listings link for ServiceNow |
| `FCC_FORM_499_URL` | FCC Form 499 endpoint |
| `MAX_PAGES` | Total listing pages limits execution to avoid slamming |

---

## 🏃‍♂️ Execution Handles

To run node triggers securely from within your server environment containers:

```bash
docker exec crwm_app python /app/scripts/Pipeline/unified_async_pipeline.py
```

### 💡 Live Stream Integration
Your dashboard refreshes every 5 seconds. Leads hit the database offloaded seamlessly concurrently node transparently rendering live!

---

## 🛠 Troubleshooting Error Nodes

| Symptom | Cause | Remedy |
| :--- | :--- | :--- |
| **No email, skip** | Profile form doesn't provide an email index. | Native Accurate expected outcome. |
| **TimeoutError** | Network delay on angular nodes. | Handled automatically with fresh recoveries. |
| **Duplicate, skip** | Lead already added formerly. | Avoids clutter layout. |
