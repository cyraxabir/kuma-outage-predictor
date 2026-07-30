# outage-forecast

**AI-assisted predictive monitoring for Uptime Kuma — self-hosted, no SaaS, no cloud dependency.**

Uptime Kuma's dashboard only shows ~1 week of history and alerts you *after* something breaks. `outage-forecast` adds a layer on top: long-term retention, flapping detection, recurring-failure pattern detection, monitor-to-monitor correlation, and a lead-time early-warning signal — all backed by a local LLM that writes the alerts in plain Bangla.

Main script: [`ai-predictor.py`](https://github.com/cyraxabir/outage-forecast/blob/main/ai-monitor/ai-predictor.py)

![architecture](./upt-ss.png)



---

## What it actually does

Every signal below is **computed first, then explained by AI** — the model never invents a correlation or trend it wasn't given.

| Signal | Trigger | What makes it real |
|---|---|---|
| ⚠️ **Flapping alert** | 3+ status changes in 6h | AI reads the actual down/up timestamps + durations and judges normal-recovery vs genuine instability |
| 🔮 **Pattern alert** | Recurring failure window detected (needs 14+ days) | Peak hour/day and interval consistency computed from real history; AI only validates, doesn't guess |
| 🟡 **Early warning** | Response time rising *before* the outage | Only fires for a monitor with a **proven** history of RT-rise-then-outage (≥60% reliability from its own past events) |
| 🔗 **Overlap detection** | 2+ monitors down in the same 15-min window | Real timestamp bucketing — AI is told explicitly not to claim shared-infra causes when no overlap exists |
| 📊 **Daily digest** (08:00) | — | Bangla summary of the last 24h across all monitors |
| 📅 **Weekly report** (Mon 09:00) | — | Week-over-week comparison, downtime-duration trend, correlation table, lead-time reliability table |

---

## Stack

```
Uptime Kuma  →  Prometheus (180d retention)  →  Grafana (dashboards)
                        │
                        ▼
              ai-predictor.py (cron, every 30 min)
                        │
              ┌─────────┴─────────┐
              ▼                   ▼
        Ollama (qwen2.5:14b)   Discord webhook
```

- **Uptime Kuma** — service monitoring, exposes `/metrics`
- **Prometheus** — long-term metrics storage
- **Grafana** — SLA / latency dashboards
- **Ollama** — self-hosted LLM, no data leaves the network
- **Discord** — alert delivery, in Bangla

---

## Setup

### 1. Run the base stack (Docker)

```bash
# Uptime Kuma
docker run -d --name uptime-kuma --restart always -p 3001:3001 -v uptime-kuma:/app/data louislam/uptime-kuma:1

# Prometheus (180-day retention)
docker run -d --name prometheus --restart always -p 9091:9090 \
  -v /path/to/prometheus.yml:/etc/prometheus/prometheus.yml \
  -v prometheus_data:/prometheus \
  prom/prometheus:latest \
  --config.file=./prometheus/prometheus.yml \
  --storage.tsdb.path=/prometheus \
  --storage.tsdb.retention.time=180d

# Grafana
docker run -d --name grafana --restart always -p 3000:3000 -v grafana_data:/var/lib/grafana grafana/grafana:latest
```

Then in Uptime Kuma, enable the `/metrics` endpoint (Settings → Security), and point Prometheus at it via a `uptime-kuma` scrape job (`scheme: http`, `basic_auth`).

### 2. Configure the script

```bash
cp ai-monitor/config.example.py ai-monitor/config.py
```

```python
OLLAMA_API_KEY  = "your-ollama-api-key"
PROMETHEUS_URL  = "http://your-prometheus-host:9091"
OLLAMA_URL      = "https://your-ollama-endpoint/api/v1/chat/completions"
OLLAMA_MODEL    = "qwen2.5:14b"
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/..."
```

### 3. Install dependencies

```bash
pip install -r requirements.txt --break-system-packages
```

### 4. Test run

```bash
python3 ai-monitor/ai-predictor.py
```

### 5. Schedule via cron

```bash
crontab -e
```
```
*/30 * * * * /usr/bin/python3 /opt/outage-forecast/ai-monitor/ai-predictor.py >> /var/log/ai-predictor.log 2>&1
```

---

## How prediction stays honest

This project deliberately avoids the common failure mode of AI-monitoring tools: letting the model narrate confidence it doesn't have.

- **Early warning only fires for monitors with proven history** — a brand-new monitor never gets a fabricated prediction, it just shows "insufficient history."
- **Overlap/correlation claims are computed, not guessed** — the AI prompt explicitly forbids claiming shared-infrastructure causes unless real timestamp overlap was found.
- **Severity is capped by confidence** — low-confidence trend data can't produce a HIGH/CRITICAL alert.
- **Normal recovery cycles are filtered out** — if the AI judges a flap pattern as a normal recovery cycle with LOW severity, no alert is sent at all.

---

## Data files (created automatically)

| File | Purpose |
|---|---|
| `uptime_history.json` | Downtime timestamps per monitor — feeds pattern detection |
| `uptime_events.json` | Completed down-events with duration + pre-outage RT trend — feeds duration trend, correlation, lead-time |
| `uptime_state.json` | Alert cooldowns + last digest/report dates |

These accumulate over time. Pattern detection needs 14+ days; correlation and lead-time need 5+ recorded events per monitor.

---

## Roadmap

- [ ] Facebook Prophet-based probabilistic forecasting once 180 days of history accumulates
- [ ] Optional Loki log-pull on downtime for real root-cause context
- [ ] Configurable alert channels (Slack / Telegram in addition to Discord)

---

## License

MIT
