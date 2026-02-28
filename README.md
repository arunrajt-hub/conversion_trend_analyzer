# Conversion Trend Analyzer

Extracts Conversion % trend from a source Google Sheet, writes to a destination sheet, and sends the Conversion table (A1:S25) to WhatsApp.

## Schedule

Runs **daily at 1 PM IST** via GitHub Actions.

## Setup (GitHub)

1. **Create a new repository** on GitHub (e.g. `conversion-trend-analyzer`).

2. **Push this folder** to the repo:
   ```bash
   cd github_repos/conversion_trend_analyzer
   git init
   git add .
   git commit -m "Initial commit"
   git remote add origin https://github.com/YOUR_USERNAME/conversion-trend-analyzer.git
   git push -u origin main
   ```

3. **Add Secrets** in repo Settings → Secrets and variables → Actions:
   - `SERVICE_ACCOUNT_JSON` – Full JSON content of your Google service account key
   - `WHAPI_TOKEN` – Your WHAPI token from https://whapi.cloud/
   - `WHATSAPP_PHONE` – Recipient(s), comma-separated (e.g. `120363320457092145@g.us,919500055366`)

4. **Share the Google Sheets** with the service account email (Editor access):
   - Source sheet: Base Data worksheet
   - Destination sheet: Conversion worksheet

## Local Run

```bash
pip install -r requirements.txt
# Place service_account_key.json in this folder
# Set WHATSAPP_PHONE and WHAPI_TOKEN env vars if using WhatsApp
python conversion_trend_analyzer.py
```
