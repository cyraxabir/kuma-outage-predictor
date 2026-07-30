#!/usr/bin/env python3
"""
Uptime Kuma — AI-Based Monitoring, Prediction & Correlation
─────────────────────────────────────────────────────────────────────────────
Real-time (every 30 min):
  1. Flapping analysis   — AI judges: real instability vs normal recovery
  2. Pattern validation  — AI confirms recurring failure windows are genuine
  3. Early warning        — if a monitor has a PROVEN lead-time signal
                            (RT reliably rises before its past outages),
                            and RT is rising right now, fire a predictive
                            alert BEFORE the outage happens.

Scheduled:
  4. Daily digest  (08:00)        — Bangla summary of last 24h
  5. Weekly report (Mon 09:00)    — week-over-week comparison, duration
                                    trend per monitor, monitor-to-monitor
                                    correlation, lead-time reliability table

All "prediction" claims are computed from real stored history first;
AI only comments on numbers that were actually calculated — it never
invents correlation, trend, or lead-time that wasn't measured.

Cron:
  */30 * * * * /usr/bin/python3 /opt/ai-predictor.py >> /var/log/ai_predictor.log 2>&1
"""

import requests
import json
import time
import os
from datetime import datetime
from collections import defaultdict

# ─── CONFIG ───────────────────────────────────────────────────────────────────
OLLAMA_API_KEY   = "<key>"
PROMETHEUS_URL   = "http://server-ip:9091"
OLLAMA_URL       = "https://<ollama-api-endpoint>"
OLLAMA_MODEL     = "qwen2.5:14b"  # change to whatever model you have loaded
DISCORD_WEBHOOK  = "https://discord.com/api/webhooks/**************"

HISTORY_FILE = "/opt/uptime_history.json"   # downtime timestamps per monitor (for pattern detection)
EVENTS_FILE  = "/opt/uptime_events.json"    # completed down-events with duration + pre-RT trend
STATE_FILE   = "/opt/uptime_state.json"     # cooldowns + last digest dates

LOOKBACK_HOURS       = 6
STEP_SECONDS         = 120
FLAP_THRESHOLD       = 3
ALERT_COOLDOWN       = 3600

PATTERN_MIN_DAYS     = 14
PATTERN_WINDOW_HOURS = 2

DIGEST_HOUR          = 8

OVERLAP_BUCKET_MIN   = 15
OVERLAP_MIN_MONITORS = 2

# duration trend
DURATION_TREND_MIN_EVENTS = 4

# correlation
CORRELATION_WINDOW_MIN = 10
CORRELATION_MIN_EVENTS = 5
CORRELATION_MIN_PCT    = 40

# lead-time / early warning
LEAD_TIME_MIN_EVENTS = 5
LEAD_TIME_HIGH_PCT   = 60
EARLY_WARN_RT_DELTA  = 100   # ms rise in last 30 min to count as "rising"

# weekly report
WEEKLY_DIGEST_DAY  = 0   # Monday
WEEKLY_DIGEST_HOUR = 9
# ──────────────────────────────────────────────────────────────────────────────


# ─── STATE & HISTORY ──────────────────────────────────────────────────────────

def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return default


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def load_state():
    return load_json(STATE_FILE, {"cooldowns": {}, "last_digest_date": "", "last_weekly_date": ""})


def load_history():
    return load_json(HISTORY_FILE, {})


def load_events():
    return load_json(EVENTS_FILE, {})


def record_downtime(history, name, ts):
    if name not in history:
        history[name] = []
    if history[name] and abs(history[name][-1] - ts) < 300:
        return
    history[name].append(ts)
    history[name] = history[name][-2000:]


def record_completed_event(events_store, name, event):
    """Persist a completed (non-ongoing) down event, deduped by start_epoch."""
    if name not in events_store:
        events_store[name] = []
    for existing in events_store[name]:
        if abs(existing["start_epoch"] - event["start_epoch"]) < 60:
            return
    events_store[name].append(event)
    events_store[name] = events_store[name][-500:]


# ─── PROMETHEUS ───────────────────────────────────────────────────────────────

def prom_range(query, hours, step=STEP_SECONDS):
    end   = int(time.time())
    start = end - int(hours * 3600)
    return prom_range_window(query, start, end, step)


def prom_range_window(query, start_ts, end_ts, step=300):
    r = requests.get(
        f"{PROMETHEUS_URL}/api/v1/query_range",
        params={"query": query, "start": int(start_ts), "end": int(end_ts), "step": step},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["data"]["result"]


# ─── BASIC HELPERS ────────────────────────────────────────────────────────────

def count_flaps(statuses):
    return sum(1 for i in range(1, len(statuses)) if statuses[i] != statuses[i - 1])


def trend_label(vals):
    """Simple early/late half comparison. Returns বৃদ্ধি/স্থির/হ্রাস or None."""
    if len(vals) < 4:
        return None
    half  = len(vals) // 2
    early = sum(vals[:half]) / half
    late  = sum(vals[half:]) / (len(vals) - half)
    diff  = late - early
    if abs(diff) < 30:
        return "স্থির"
    return "বৃদ্ধি" if diff > 0 else "হ্রাস"


def rt_trend(rt_raw):
    """Human-readable RT trend string for a full window."""
    vals = [float(v[1]) for v in rt_raw if float(v[1]) > 0]
    if len(vals) < 6:
        return "insufficient data"
    third = len(vals) // 3
    early = sum(vals[:third]) / third
    late  = sum(vals[-third:]) / third
    diff  = late - early
    if abs(diff) < 50:
        return f"stable (~{round(late)}ms)"
    elif diff > 0:
        return f"increasing (+{round(diff)}ms, now ~{round(late)}ms)"
    else:
        return f"decreasing ({round(diff)}ms, now ~{round(late)}ms)"


def short_term_rising(rt_raw, minutes=30, threshold_ms=EARLY_WARN_RT_DELTA):
    """Check if RT has risen by >= threshold in the last `minutes`. Returns (bool, delta_ms)."""
    cutoff = time.time() - minutes * 60
    vals = [float(v[1]) for v in rt_raw if float(v[0]) >= cutoff and float(v[1]) > 0]
    if len(vals) < 4:
        return False, 0
    half  = len(vals) // 2
    early = sum(vals[:half]) / half
    late  = sum(vals[half:]) / (len(vals) - half)
    diff  = late - early
    return diff >= threshold_ms, round(diff)


def extract_down_events(status_raw, rt_raw):
    """
    Extract each down→up cycle from a raw Prometheus status series.
    Each event includes duration, average RT just before the drop, and
    whether RT was trending up in the 30 min before the drop (pre_rt_trend).
    """
    events   = []
    in_down  = False
    down_start = None

    rt_lookup     = {float(v[0]): float(v[1]) for v in rt_raw} if rt_raw else {}
    rt_times_sorted = sorted(rt_lookup.keys())

    for i, (ts_str, val_str) in enumerate(status_raw):
        ts, val = float(ts_str), float(val_str)

        if val == 0.0 and not in_down:
            in_down    = True
            down_start = ts

        elif val == 1.0 and in_down:
            in_down  = False
            duration = round((ts - down_start) / 60, 1)

            pre_rts    = [rt_lookup[t] for t in rt_times_sorted if down_start - 1800 <= t < down_start]
            pre_rt_avg = round(sum(pre_rts) / len(pre_rts), 0) if pre_rts else None
            pre_trend  = trend_label(pre_rts) if pre_rts else None

            events.append({
                "start":        datetime.fromtimestamp(down_start).strftime("%H:%M"),
                "end":          datetime.fromtimestamp(ts).strftime("%H:%M"),
                "duration_m":   duration,
                "pre_rt_ms":    pre_rt_avg,
                "start_epoch":  down_start,
                "pre_rt_trend": pre_trend,
            })

    if in_down:
        events.append({
            "start":        datetime.fromtimestamp(down_start).strftime("%H:%M"),
            "end":          "ongoing",
            "duration_m":   round((float(status_raw[-1][0]) - down_start) / 60, 1),
            "pre_rt_ms":    None,
            "start_epoch":  down_start,
            "pre_rt_trend": None,
            "ongoing":      True,
        })

    return events


def detect_overlaps(st_by_name):
    """Real overlap detection via timestamp bucketing — not AI guesswork."""
    bucket_sec = OVERLAP_BUCKET_MIN * 60
    buckets    = defaultdict(set)
    for name, values in st_by_name.items():
        for ts_str, val in values:
            if float(val) == 0.0:
                bucket = int(float(ts_str) // bucket_sec) * bucket_sec
                buckets[bucket].add(name)
    return [
        {"time": datetime.fromtimestamp(b).strftime("%Y-%m-%d %H:%M"),
         "monitors": sorted(m), "count": len(m)}
        for b, m in sorted(buckets.items()) if len(m) >= OVERLAP_MIN_MONITORS
    ]


# ─── PATTERN DETECTION (math only) ───────────────────────────────────────────

def compute_pattern(history, name):
    events = history.get(name, [])
    if len(events) < 5:
        return None

    now       = time.time()
    data_days = (now - min(events)) / 86400

    if data_days < PATTERN_MIN_DAYS:
        return {"status": "accumulating", "days": round(data_days, 1),
                "needed": PATTERN_MIN_DAYS, "events": len(events)}

    day_names   = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    hours       = [datetime.fromtimestamp(ts).hour      for ts in events]
    days        = [datetime.fromtimestamp(ts).weekday() for ts in events]
    hour_counts = defaultdict(int)
    day_counts  = defaultdict(int)
    for h in hours: hour_counts[h] += 1
    for d in days:  day_counts[d]  += 1

    peak_hour     = max(hour_counts, key=hour_counts.get)
    peak_hour_pct = hour_counts[peak_hour] / len(events) * 100
    peak_day      = max(day_counts,  key=day_counts.get)
    peak_day_pct  = day_counts[peak_day]  / len(events) * 100

    sorted_ev    = sorted(events)
    intervals_h  = [(sorted_ev[i+1] - sorted_ev[i]) / 3600 for i in range(len(sorted_ev) - 1)]
    avg_interval = sum(intervals_h) / len(intervals_h)
    stddev       = (sum((x - avg_interval) ** 2 for x in intervals_h) / len(intervals_h)) ** 0.5
    consistency  = round((1 - min(stddev / avg_interval, 1)) * 100, 1) if avg_interval > 0 else 0

    next_pred   = sorted_ev[-1] + avg_interval * 3600
    hours_until = (next_pred - now) / 3600

    return {
        "status":           "active",
        "total_events":     len(events),
        "data_days":        round(data_days, 1),
        "peak_hour":        peak_hour,
        "peak_hour_pct":    round(peak_hour_pct, 1),
        "peak_day":         day_names[peak_day],
        "peak_day_pct":     round(peak_day_pct, 1),
        "avg_interval_h":   round(avg_interval, 1),
        "consistency_pct":  consistency,
        "hours_until_next": round(hours_until, 1),
        "last_down":        datetime.fromtimestamp(sorted_ev[-1]).strftime("%Y-%m-%d %H:%M"),
        "recent_events":    [datetime.fromtimestamp(ts).strftime("%m-%d %H:%M") for ts in sorted_ev[-5:]],
    }


# ─── DURATION TREND (real, computed) ─────────────────────────────────────────

def duration_trend(name, events_store):
    """Compare avg down-duration of recent half of events vs older half."""
    events = [e for e in events_store.get(name, []) if "duration_m" in e and not e.get("ongoing")]
    if len(events) < DURATION_TREND_MIN_EVENTS:
        return None

    sorted_ev = sorted(events, key=lambda e: e["start_epoch"])
    half      = len(sorted_ev) // 2
    older, recent = sorted_ev[:half], sorted_ev[half:]

    older_avg  = sum(e["duration_m"] for e in older)  / len(older)
    recent_avg = sum(e["duration_m"] for e in recent) / len(recent)
    diff = recent_avg - older_avg

    trend = "স্থির" if abs(diff) < 1 else ("বাড়ছে" if diff > 0 else "কমছে")

    return {"trend": trend, "recent_avg": round(recent_avg, 1),
            "older_avg": round(older_avg, 1), "n": len(sorted_ev)}


# ─── CORRELATION (real, computed) ────────────────────────────────────────────

def compute_correlations(events_store, window_min=CORRELATION_WINDOW_MIN,
                         min_events=CORRELATION_MIN_EVENTS, min_pct=CORRELATION_MIN_PCT):
    """
    For each monitor pair (A, B): what % of A's down events had B also go
    down within `window_min` minutes? Real computed correlation, not AI guess.
    """
    names = [n for n, evs in events_store.items() if len(evs) >= min_events]
    window_sec = window_min * 60
    results = []

    for a in names:
        a_events = events_store[a]
        for b in names:
            if a == b:
                continue
            b_starts = [e["start_epoch"] for e in events_store[b]]
            match = sum(
                1 for e in a_events
                if any(abs(e["start_epoch"] - bs) <= window_sec for bs in b_starts)
            )
            pct = round(match / len(a_events) * 100, 1)
            if pct >= min_pct:
                results.append({"a": a, "b": b, "pct": pct, "n": len(a_events)})

    results.sort(key=lambda r: -r["pct"])
    return results[:10]


# ─── LEAD-TIME RELIABILITY (real, computed) ──────────────────────────────────

def lead_time_reliability(name, events_store, min_events=LEAD_TIME_MIN_EVENTS):
    """
    % of this monitor's past down-events that were preceded by a rising
    response-time trend in the 30 min before the drop. If this % is high,
    a live RT-rise becomes a genuine predictive signal for THIS monitor.
    """
    events = [e for e in events_store.get(name, []) if e.get("pre_rt_trend")]
    if len(events) < min_events:
        return None
    increasing = sum(1 for e in events if e["pre_rt_trend"] == "বৃদ্ধি")
    pct = round(increasing / len(events) * 100, 1)
    return {"pct": pct, "n": len(events), "reliable": pct >= LEAD_TIME_HIGH_PCT}


# ─── OLLAMA ───────────────────────────────────────────────────────────────────

def ollama(prompt, temperature=0.2):
    r = requests.post(
        OLLAMA_URL,
        json={"model": OLLAMA_MODEL,
              "messages": [{"role": "user", "content": prompt}],
              "temperature": temperature, "stream": False},
        headers={"Authorization": f"Bearer {OLLAMA_API_KEY}"},
        timeout=120,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()


# ─── AI DECISION: FLAP ANALYSIS ───────────────────────────────────────────────

def ai_analyze_flap(name, flap_count, down_events, rt_trend_str, pattern, dur_trend):
    if down_events:
        events_block = "\n".join(
            f"  • {e['start']} → {e['end']} ({e['duration_m']} মিনিট ডাউন)"
            + (f", আগের RT: {e['pre_rt_ms']}ms" if e['pre_rt_ms'] else "")
            for e in down_events
        )
    else:
        events_block = "  কোনো সম্পূর্ণ ডাউন ইভেন্ট শনাক্ত হয়নি"

    if pattern and pattern["status"] == "active":
        pattern_block = (
            f"মোট ইভেন্ট: {pattern['total_events']} টি, {pattern['data_days']} দিনে\n"
            f"সর্বোচ্চ ব্যর্থতার সময়: {pattern['peak_hour']}:00 ({pattern['peak_hour_pct']}%)\n"
            f"গড় বিরতি: {pattern['avg_interval_h']} ঘণ্টা (নিয়মিততা: {pattern['consistency_pct']}%)"
        )
    elif pattern and pattern["status"] == "accumulating":
        pattern_block = f"ইতিহাস সংগ্রহ হচ্ছে — {pattern['days']}/{pattern['needed']} দিন"
    else:
        pattern_block = "পর্যাপ্ত ইতিহাস নেই"

    if dur_trend:
        dur_block = (
            f"ডাউনটাইমের দৈর্ঘ্য {dur_trend['trend']} — আগে গড়ে {dur_trend['older_avg']} মিনিট, "
            f"এখন গড়ে {dur_trend['recent_avg']} মিনিট (n={dur_trend['n']})"
        )
    else:
        dur_block = "পর্যাপ্ত ইতিহাস নেই duration trend-এর জন্য"

    prompt = f"""তুমি একজন senior SRE যে নিচের monitoring data বিশ্লেষণ করছ।

মনিটর: {name}
৬ ঘণ্টায় status পরিবর্তন: {flap_count} বার
Response time trend: {rt_trend_str}
ডাউনটাইম দৈর্ঘ্যের প্রবণতা: {dur_block}

প্রতিটি ডাউন ইভেন্টের বিস্তারিত:
{events_block}

ঐতিহাসিক প্যাটার্ন:
{pattern_block}

নিচের ৪টি প্রশ্নের উত্তর দাও, শুধুমাত্র দেওয়া data-র উপর ভিত্তি করে:

SEVERITY: (LOW / MEDIUM / HIGH / CRITICAL)
TYPE: (নিয়মিত_বিরতি / এলোমেলো / একক_দীর্ঘ_আউটেজ / recovery_cycle)
NORMAL: (হ্যাঁ / না)
ANALYSIS: (২ বাক্য বাংলায় — ডাউনটাইমের ধরন, দৈর্ঘ্যের প্রবণতা এবং এটি কী ইঙ্গিত করছে)

শুধু উপরের ৪টি লাইন লেখো, কোনো ভূমিকা নয়।"""

    response = ollama(prompt)
    result = {"severity": "MEDIUM", "type": "এলোমেলো", "normal": "না", "analysis": response}
    for line in response.splitlines():
        if line.startswith("SEVERITY:"):  result["severity"] = line.split(":", 1)[1].strip()
        elif line.startswith("TYPE:"):    result["type"]     = line.split(":", 1)[1].strip()
        elif line.startswith("NORMAL:"):  result["normal"]   = line.split(":", 1)[1].strip()
        elif line.startswith("ANALYSIS:"): result["analysis"] = line.split(":", 1)[1].strip()
    return result


# ─── AI DECISION: PATTERN VALIDATION ─────────────────────────────────────────

def ai_validate_pattern(name, pattern):
    prompt = f"""তুমি একজন data analyst। নিচের statistical pattern দেখে সিদ্ধান্ত নাও এটি প্রকৃত recurring pattern কিনা।

মনিটর: {name}
মোট ইভেন্ট: {pattern['total_events']} টি, {pattern['data_days']} দিনে
গড় বিরতি: {pattern['avg_interval_h']} ঘণ্টা
নিয়মিততা: {pattern['consistency_pct']}%
Peak hour: {pattern['peak_hour']}:00 ({pattern['peak_hour_pct']}%)
সাম্প্রতিক ৫টি: {', '.join(pattern['recent_events'])}

GENUINE: (হ্যাঁ / না)
CONFIDENCE: (LOW / MEDIUM / HIGH)
COMMENT: (১ বাক্য বাংলায়)

শুধু উপরের ৩টি লাইন।"""

    response = ollama(prompt)
    result = {"genuine": True, "confidence": "LOW", "comment": response}
    for line in response.splitlines():
        if line.startswith("GENUINE:"):    result["genuine"] = line.split(":", 1)[1].strip() == "হ্যাঁ"
        elif line.startswith("CONFIDENCE:"): result["confidence"] = line.split(":", 1)[1].strip()
        elif line.startswith("COMMENT:"):  result["comment"] = line.split(":", 1)[1].strip()
    return result


# ─── AI: DAILY DIGEST ─────────────────────────────────────────────────────────

def ai_daily_digest(stats, overlaps, rt_trends):
    problem_monitors = {n: s for n, s in stats.items() if s["down_events"] > 0 or s["flaps"] > 1}
    healthy_count = len(stats) - len(problem_monitors)

    monitor_block = "\n".join(
        f"- {n}: {s['down_events']} বার ডাউন, {s['flaps']} বার ফ্ল্যাপ, "
        f"গড় RT {s['avg_rt']:.0f}ms, RT trend: {rt_trends.get(n, 'N/A')}"
        for n, s in sorted(problem_monitors.items(), key=lambda x: -x[1]["down_events"])
    ) if problem_monitors else "কোনো সমস্যা নেই"

    if overlaps:
        ov_lines = "\n".join(
            f"- {o['time']}: {o['count']} টি মনিটর একসাথে ডাউন — {', '.join(o['monitors'])}"
            for o in overlaps[:10]
        )
        overlap_instruction = f"{ov_lines}\nএগুলো প্রকৃত একযোগ আউটেজ।"
    else:
        overlap_instruction = "কোনো একযোগ আউটেজ পাওয়া যায়নি। shared infrastructure কারণ দাবি করো না।"

    prompt = f"""তুমি একজন monitoring analyst। নিচের ২৪ ঘণ্টার data দেখে বাংলায় সংক্ষিপ্ত daily digest লেখো।

তারিখ: {datetime.now().strftime('%Y-%m-%d')}
মোট মনিটর: {len(stats)} টি, সুস্থ: {healthy_count} টি

সমস্যাযুক্ত মনিটর:
{monitor_block}

একযোগ আউটেজ:
{overlap_instruction}

নিয়ম: শুধু data থেকে সিদ্ধান্ত নাও, অনুমান নয়। ৩-৫ বাক্যে, plain paragraph, সম্পূর্ণ বাংলায়।"""

    return ollama(prompt, temperature=0.3)


# ─── AI: WEEKLY REPORT ────────────────────────────────────────────────────────

def ai_weekly_report(comp_block, dur_block, corr_block, lead_block):
    prompt = f"""তুমি একজন monitoring analyst। নিচের সাপ্তাহিক তুলনামূলক তথ্য দেখে বাংলায় weekly report লেখো।

সপ্তাহ তুলনা (এই সপ্তাহ vs গত সপ্তাহ):
{comp_block}

ডাউনটাইম duration trend:
{dur_block}

Monitor correlation (একসাথে ডাউন হওয়ার প্রবণতা):
{corr_block}

Lead-time indicator (RT বৃদ্ধি কি outage পূর্বাভাস দেয়):
{lead_block}

নিয়ম:
- শুধু দেওয়া data থেকে সিদ্ধান্ত নাও
- কোনো section-এ তথ্য না থাকলে সেটা নিয়ে জোর করে মন্তব্য করো না
- সবচেয়ে গুরুত্বপূর্ণ finding আগে বলো
- ৪-৬ বাক্যে, plain paragraph, সম্পূর্ণ বাংলায়"""

    return ollama(prompt, temperature=0.3)


# ─── DISCORD ──────────────────────────────────────────────────────────────────

def send_discord(embeds):
    r = requests.post(DISCORD_WEBHOOK, json={"embeds": embeds}, timeout=10)
    r.raise_for_status()


def severity_color(severity):
    s = severity.upper()
    if "CRITICAL" in s: return 0xFF0000, "🔴"
    if "HIGH"     in s: return 0xFF6600, "🟠"
    if "MEDIUM"   in s: return 0xFFCC00, "🟡"
    return 0x2ECC71, "🟢"


def flap_embed(name, flap_count, down_events, pattern, current_status, ai, dur_trend):
    color, emoji = severity_color(ai.get("severity", "MEDIUM"))
    status_txt   = "🟢 UP" if current_status == 1 else "🔴 DOWN" if current_status == 0 else "🟡 PENDING"

    events_str = "\n".join(
        f"{e['start']}→{e['end']} ({e['duration_m']}m)" + (f" pre-RT:{e['pre_rt_ms']}ms" if e['pre_rt_ms'] else "")
        for e in down_events[-6:]
    ) if down_events else "কোনো সম্পূর্ণ ডাউন ইভেন্ট নেই"

    if pattern and pattern["status"] == "active":
        pat_str = (f"Peak: {pattern['peak_hour']}:00 ({pattern['peak_hour_pct']}%)\n"
                   f"Interval: ~{pattern['avg_interval_h']}h (নিয়মিততা {pattern['consistency_pct']}%)\n"
                   f"Next: {pattern['hours_until_next']:+.1f}h")
    elif pattern and pattern["status"] == "accumulating":
        pat_str = f"সংগ্রহ হচ্ছে — {pattern['days']}/{pattern['needed']} দিন"
    else:
        pat_str = "পর্যাপ্ত ইতিহাস নেই"

    dur_str = (f"{dur_trend['trend']} — আগে {dur_trend['older_avg']}m, এখন {dur_trend['recent_avg']}m"
              if dur_trend else "পর্যাপ্ত ইতিহাস নেই")

    normal_tag = "✅ স্বাভাবিক recovery" if ai.get("normal") == "হ্যাঁ" else "⚠️ প্রকৃত অস্থিরতা"

    return {
        "title": f"{emoji} Flapping — {name}  [{ai.get('severity','?')}]",
        "color": color,
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [
            {"name": "Status",       "value": status_txt,  "inline": True},
            {"name": "Flaps / 6h",   "value": str(flap_count), "inline": True},
            {"name": "AI রায়",       "value": normal_tag,   "inline": True},
            {"name": "ধরন",          "value": ai.get("type", "—"), "inline": True},
            {"name": "Duration trend", "value": dur_str,    "inline": True},
            {"name": "ডাউন ইভেন্ট", "value": f"```{events_str}```", "inline": False},
            {"name": "প্যাটার্ন",    "value": f"```{pat_str}```",    "inline": False},
            {"name": "🤖 AI বিশ্লেষণ", "value": ai.get("analysis", "—")[:1000], "inline": False},
        ],
        "footer": {"text": f"Uptime Kuma • {datetime.now().strftime('%Y-%m-%d %H:%M')}"}
    }


def pattern_embed(name, pattern, ai_validation):
    confidence_emoji = {"HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🔴"}.get(ai_validation.get("confidence", "LOW"), "⚪")
    pat_str = (
        f"মোট ইভেন্ট: {pattern['total_events']} টি, {pattern['data_days']} দিনে\n"
        f"Peak hour: {pattern['peak_hour']}:00 ({pattern['peak_hour_pct']}%)\n"
        f"বিরতি: প্রতি ~{pattern['avg_interval_h']}h (নিয়মিততা {pattern['consistency_pct']}%)\n"
        f"পরবর্তী উইন্ডো: {pattern['hours_until_next']:+.1f}h"
    )
    genuine_tag = "✅ প্রকৃত pattern" if ai_validation.get("genuine") else "⚠️ সম্ভবত noise"
    return {
        "title": f"🔮 Pattern Alert — {name}",
        "color": 0x9B59B6,
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [
            {"name": "AI যাচাই",  "value": f"{confidence_emoji} {genuine_tag}", "inline": True},
            {"name": "Confidence", "value": ai_validation.get("confidence", "—"), "inline": True},
            {"name": "Pattern data", "value": f"```{pat_str}```", "inline": False},
            {"name": "🤖 AI মূল্যায়ন", "value": ai_validation.get("comment", "—"), "inline": False},
        ],
        "footer": {"text": f"Uptime Kuma • {datetime.now().strftime('%Y-%m-%d %H:%M')}"}
    }


def early_warning_embed(name, lead_time, rt_diff, trend_str):
    return {
        "title": f"🟡 Early Warning — {name}",
        "color": 0xF1C40F,
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [
            {"name": "কারণ", "value": f"Response time গত ৩০ মিনিটে +{rt_diff}ms বেড়েছে", "inline": False},
            {"name": "Lead-time নির্ভরযোগ্যতা",
             "value": f"অতীতে {lead_time['pct']}% ক্ষেত্রে এই ধরনের বৃদ্ধির পর outage হয়েছে (n={lead_time['n']} ঘটনা থেকে যাচাইকৃত)",
             "inline": False},
            {"name": "বর্তমান RT ট্রেন্ড", "value": trend_str, "inline": False},
        ],
        "footer": {"text": "Predictive early warning — শুধু প্রমাণিত historical correlation থেকে"}
    }


def digest_embed(ai_summary, stats, overlaps):
    monitor_lines = [
        f"{name}: {s['down_events']} down, {s['flaps']} flaps, avg {s['avg_rt']:.0f}ms"
        for name, s in sorted(stats.items(), key=lambda x: -x[1]["down_events"])
        if s["down_events"] > 0 or s["flaps"] > 0
    ]
    raw_stats = "\n".join(monitor_lines) if monitor_lines else "সব মনিটর সুস্থ"
    fields = [{"name": "গত ২৪ ঘণ্টা", "value": f"```{raw_stats[:900]}```", "inline": False}]
    if overlaps:
        ov_lines = "\n".join(f"{o['time']} — {o['count']} মনিটর: {', '.join(o['monitors'])}" for o in overlaps[:8])
        fields.append({"name": "🔗 নিশ্চিত একযোগ আউটেজ", "value": f"```{ov_lines[:900]}```", "inline": False})
    fields.append({"name": "🤖 AI সারসংক্ষেপ", "value": ai_summary[:1000], "inline": False})
    return {"title": "📊 দৈনিক মনিটর ডাইজেস্ট", "color": 0x3498DB,
            "timestamp": datetime.utcnow().isoformat(), "fields": fields,
            "footer": {"text": f"Uptime Kuma • {datetime.now().strftime('%Y-%m-%d %H:%M')}"}}


def weekly_embed(comp_block, dur_block, corr_block, lead_block, ai_summary):
    return {
        "title": "📅 সাপ্তাহিক মনিটরিং রিপোর্ট",
        "color": 0x1ABC9C,
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [
            {"name": "সপ্তাহ তুলনা (এই সপ্তাহ vs গত সপ্তাহ)", "value": f"```{comp_block[:900]}```", "inline": False},
            {"name": "ডাউনটাইম দৈর্ঘ্যের প্রবণতা", "value": f"```{dur_block[:900]}```", "inline": False},
            {"name": "Monitor Correlation", "value": f"```{corr_block[:900]}```", "inline": False},
            {"name": "Lead-time Indicator", "value": f"```{lead_block[:900]}```", "inline": False},
            {"name": "🤖 AI সারসংক্ষেপ", "value": ai_summary[:1000], "inline": False},
        ],
        "footer": {"text": f"Uptime Kuma Weekly • {datetime.now().strftime('%Y-%m-%d')}"}
    }


# ─── DAILY DIGEST RUNNER ──────────────────────────────────────────────────────

def run_daily_digest():
    print(f"[{datetime.now()}] Running daily digest...")
    rt_results = prom_range('monitor_response_time{job="uptime-kuma"}', 24, step=300)
    st_results = prom_range('monitor_status{job="uptime-kuma"}',        24, step=300)

    rt_by_name = {r["metric"].get("monitor_name"): r["values"] for r in rt_results}
    st_by_name = {r["metric"].get("monitor_name"): r["values"] for r in st_results}

    stats, rt_trends = {}, {}
    for name in rt_by_name:
        rt_vals = [float(v[1]) for v in rt_by_name[name]]
        st_vals = [float(v[1]) for v in st_by_name.get(name, [])]
        stats[name] = {
            "avg_rt": sum(rt_vals) / len(rt_vals) if rt_vals else 0,
            "max_rt": max(rt_vals) if rt_vals else 0,
            "down_events": sum(1 for v in st_vals if v == 0.0),
            "flaps": count_flaps(st_vals),
        }
        rt_trends[name] = rt_trend(rt_by_name[name])

    overlaps = detect_overlaps(st_by_name)

    try:
        ai_summary = ai_daily_digest(stats, overlaps, rt_trends)
    except Exception as e:
        ai_summary = f"AI অনুপলব্ধ: {e}"

    try:
        send_discord([digest_embed(ai_summary, stats, overlaps)])
        print(f"[{datetime.now()}] Digest sent ({len(overlaps)} overlaps)")
    except Exception as e:
        print(f"[{datetime.now()}] Digest Discord error: {e}")


# ─── WEEKLY REPORT RUNNER ─────────────────────────────────────────────────────

def run_weekly_report(events_store):
    print(f"[{datetime.now()}] Running weekly report...")
    now = time.time()

    this_st = prom_range_window('monitor_status{job="uptime-kuma"}', now - 7*86400, now, step=300)
    last_st = prom_range_window('monitor_status{job="uptime-kuma"}', now - 14*86400, now - 7*86400, step=300)

    this_by_name = {r["metric"].get("monitor_name"): r["values"] for r in this_st}
    last_by_name = {r["metric"].get("monitor_name"): r["values"] for r in last_st}

    comparison = {}
    for name in this_by_name:
        this_vals = [float(v[1]) for v in this_by_name[name]]
        last_vals = [float(v[1]) for v in last_by_name.get(name, [])]
        this_down, last_down = sum(1 for v in this_vals if v == 0.0), sum(1 for v in last_vals if v == 0.0)
        this_flap, last_flap = count_flaps(this_vals), count_flaps(last_vals)
        if this_down or last_down or this_flap > 1 or last_flap > 1:
            comparison[name] = {"this_down": this_down, "last_down": last_down,
                               "this_flap": this_flap, "last_flap": last_flap}

    duration_trends = {n: v for n, v in ((n, duration_trend(n, events_store)) for n in events_store) if v}
    correlations    = compute_correlations(events_store)
    lead_times      = {n: v for n, v in ((n, lead_time_reliability(n, events_store)) for n in events_store)
                       if v and v["reliable"]}

    comp_block = "\n".join(
        f"- {n}: এই সপ্তাহে {c['this_down']} down/{c['this_flap']} flap, "
        f"গত সপ্তাহে {c['last_down']} down/{c['last_flap']} flap"
        for n, c in sorted(comparison.items(), key=lambda x: -x[1]['this_down'])
    ) if comparison else "উল্লেখযোগ্য কোনো পরিবর্তন নেই"

    dur_block = "\n".join(
        f"- {n}: {d['trend']} — আগে {d['older_avg']}m, এখন {d['recent_avg']}m (n={d['n']})"
        for n, d in duration_trends.items()
    ) if duration_trends else "পর্যাপ্ত ইতিহাস নেই"

    corr_block = "\n".join(
        f"- {c['a']} ডাউন হলে {c['pct']}% ক্ষেত্রে {c['b']}ও {CORRELATION_WINDOW_MIN} মিনিটের মধ্যে ডাউন হয় (n={c['n']})"
        for c in correlations
    ) if correlations else "কোনো শক্তিশালী correlation পাওয়া যায়নি"

    lead_block = "\n".join(
        f"- {n}: {v['pct']}% ক্ষেত্রে outage-এর আগে RT বৃদ্ধি পেয়েছে (n={v['n']}) — নির্ভরযোগ্য early-warning signal"
        for n, v in lead_times.items()
    ) if lead_times else "কোনো নির্ভরযোগ্য leading indicator পাওয়া যায়নি"

    try:
        ai_summary = ai_weekly_report(comp_block, dur_block, corr_block, lead_block)
    except Exception as e:
        ai_summary = f"AI অনুপলব্ধ: {e}"

    try:
        send_discord([weekly_embed(comp_block, dur_block, corr_block, lead_block, ai_summary)])
        print(f"[{datetime.now()}] Weekly report sent")
    except Exception as e:
        print(f"[{datetime.now()}] Weekly report Discord error: {e}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print(f"[{datetime.now()}] Run started")
    state        = load_state()
    history      = load_history()
    events_store = load_events()
    now          = time.time()

    today_str    = datetime.now().strftime("%Y-%m-%d")
    current_hour = datetime.now().hour
    weekday      = datetime.now().weekday()

    if current_hour == DIGEST_HOUR and state.get("last_digest_date") != today_str:
        run_daily_digest()
        state["last_digest_date"] = today_str
        save_json(STATE_FILE, state)

    if (weekday == WEEKLY_DIGEST_DAY and current_hour == WEEKLY_DIGEST_HOUR
            and state.get("last_weekly_date") != today_str):
        run_weekly_report(events_store)
        state["last_weekly_date"] = today_str
        save_json(STATE_FILE, state)

    st_results = prom_range('monitor_status{job="uptime-kuma"}',        LOOKBACK_HOURS)
    rt_results = prom_range('monitor_response_time{job="uptime-kuma"}', LOOKBACK_HOURS)

    st_by_name = {r["metric"].get("monitor_name"): r["values"] for r in st_results}
    rt_by_name = {r["metric"].get("monitor_name"): r["values"] for r in rt_results}

    for name, vals in st_by_name.items():
        for ts_str, val in vals:
            if float(val) == 0.0:
                record_downtime(history, name, float(ts_str))
    save_json(HISTORY_FILE, history)

    embeds_to_send = []

    for name, st_raw in st_by_name.items():
        st_vals        = [float(v[1]) for v in st_raw]
        flaps          = count_flaps(st_vals)
        current_status = st_vals[-1] if st_vals else 1.0
        pattern        = compute_pattern(history, name)
        rt_raw         = rt_by_name.get(name, [])

        # compute down events once, persist completed ones for trend/correlation/lead-time
        down_events = extract_down_events(st_raw, rt_raw)
        for e in down_events:
            if not e.get("ongoing"):
                record_completed_event(events_store, name, e)

        dur_trend = duration_trend(name, events_store)

        cooldown_key = f"flap:{name}"
        pat_key      = f"pattern:{name}"
        warn_key     = f"earlywarn:{name}"

        # ── SIGNAL 1: flapping (AI decides severity + normal/abnormal) ──
        if flaps >= FLAP_THRESHOLD:
            last_alert = state["cooldowns"].get(cooldown_key, 0)
            if now - last_alert >= ALERT_COOLDOWN:
                rt_trend_str = rt_trend(rt_raw)
                try:
                    ai = ai_analyze_flap(name, flaps, down_events, rt_trend_str, pattern, dur_trend)
                except Exception as e:
                    ai = {"severity": "MEDIUM", "type": "অজানা", "normal": "না", "analysis": f"AI অনুপলব্ধ: {e}"}

                if ai.get("normal") == "হ্যাঁ" and "LOW" in ai.get("severity", ""):
                    print(f"[{datetime.now()}] Skipped {name} — AI: normal recovery, LOW severity")
                else:
                    embeds_to_send.append(
                        flap_embed(name, flaps, down_events, pattern, current_status, ai, dur_trend)
                    )
                    state["cooldowns"][cooldown_key] = now
                    print(f"[{datetime.now()}] Flap alert: {name} ({flaps} flaps, {ai['severity']})")

        # ── SIGNAL 2: pattern window imminent (AI validates it's genuine) ──
        if (pattern and pattern.get("status") == "active"
                and 0 < pattern["hours_until_next"] <= PATTERN_WINDOW_HOURS):
            last_alert = state["cooldowns"].get(pat_key, 0)
            if now - last_alert >= ALERT_COOLDOWN:
                try:
                    ai_val = ai_validate_pattern(name, pattern)
                except Exception as e:
                    ai_val = {"genuine": True, "confidence": "LOW", "comment": f"AI অনুপলব্ধ: {e}"}

                if ai_val.get("genuine"):
                    embeds_to_send.append(pattern_embed(name, pattern, ai_val))
                    state["cooldowns"][pat_key] = now
                    print(f"[{datetime.now()}] Pattern alert: {name}")
                else:
                    print(f"[{datetime.now()}] Skipped pattern {name} — AI: not genuine")

        # ── SIGNAL 3: early warning (proven lead-time + live RT rise) ──
        lt = lead_time_reliability(name, events_store)
        if lt and lt["reliable"]:
            rising, diff = short_term_rising(rt_raw)
            if rising:
                last_alert = state["cooldowns"].get(warn_key, 0)
                if now - last_alert >= ALERT_COOLDOWN:
                    embeds_to_send.append(early_warning_embed(name, lt, diff, rt_trend(rt_raw)))
                    state["cooldowns"][warn_key] = now
                    print(f"[{datetime.now()}] Early warning: {name} (+{diff}ms, lead-time {lt['pct']}%)")

    if embeds_to_send:
        for i in range(0, len(embeds_to_send), 10):
            send_discord(embeds_to_send[i:i+10])
        print(f"[{datetime.now()}] Sent {len(embeds_to_send)} alert(s)")
    else:
        print(f"[{datetime.now()}] No alerts")

    save_json(EVENTS_FILE, events_store)
    save_json(STATE_FILE, state)
    print(f"[{datetime.now()}] Done")


if __name__ == "__main__":
    main()