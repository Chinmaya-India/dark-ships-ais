# Dark Ships — AIS Collector

24/7 AIS vessel position collector for the [Dark Ships](https://young-snowflake-ed20.chinmaya-upmanue.workers.dev/) maritime intelligence platform.

Runs on **GitHub Actions** (free, unlimited minutes for public repos).  
Collects live vessel positions via [AisStream.io](https://aisstream.io) WebSocket across 6 AOIs in the Indian Ocean region.

## AOIs covered
- Strait of Malacca
- Strait of Hormuz  
- Gulf of Aden
- Gulf of Kutch
- English Channel / Dover Strait

## How it works
- GitHub Actions cron triggers every 5 hours
- Collector runs for ~4h 50min per job
- `ais_history.db` (SQLite, 30-day rolling window) persists between runs via Actions cache
- SAR pipeline queries the DB when correlating Sentinel-1 scene acquisitions

## Setup
Add `AISSTREAM_API_KEY` as a repository secret:  
Settings → Secrets and variables → Actions → New repository secret
