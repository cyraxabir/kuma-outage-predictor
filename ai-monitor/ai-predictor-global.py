#!/usr/bin/env python3
"""
Kuma Foresight — AI-Based Outage Prediction, Correlation & SRE Triage
─────────────────────────────────────────────────────────────────────────────
Real-time (every 30 min):
  1. Compute layer       — Python computes every stat for every monitor
                           (flaps, duration trend, relative RT trend + jitter,
                           clean outage episodes, per-monitor history baseline).
  2. Batch AI triage     — Monitors that cross the flap threshold are sent to
                           an OpenAI/Ollama endpoint in ONE combined call
                           (/no_think + JSON mode) and judged against their
                           own historical baseline.
  3. Hard veto layer     — Python-only, cannot be overridden by AI:
                             - severity HIGH/CRITICAL requires real evidence
                             - normal-recovery + LOW severity suppressed
                             - per-monitor fallback if batch JSON fails/skips.
  4. Early warning       — Relative RT % spike, absolute delta, or RT jitter
                           backed by proven historical lead-time reliability.
  5. Cascading alert     — Directional Leader -> Follower correlation alerts
                           when an upstream monitor drops.
  6. Shared Infra /      — Consolidated alert when monitors in the same
     Co-location issue     datacenter, cloud region, or provider drop together.
  7. Forecast alert      — Heads-up before an accepted forecast window opens,
                           plus Hybrid Watch + Live RT promotion.

Learning loop (same run, AFTER real-time alerts):
  8. Score               — Past forecast windows marked HIT / MISS / UNKNOWN.
  9. Learn + Predict     — AI analyzes history (with midnight wrap-around &
                           configurable workday awareness) + forecast scorecard.
 10. Back-test           — Per-monitor observation span, bimodal peak support,
                           workday calendar math, and overdue interval decay.

Scheduled:
 11. Daily digest  (08:00)     — 24h health summary + upcoming forecast windows.
 12. Weekly report (Mon 09:00) — Week-over-week stats, duration trends,
                                 directional correlations, lead-time, AI accuracy.

Cron:
  */30 * * * * /usr/bin/python3 /opt/kuma-foresight.py >> /var/log/kuma_foresight.log 2>&1
Dry-run test (reads real state, calls AI, sends/saves nothing):
  python3 /opt/kuma-foresight.py --dry-run
"""

import sys
import requests
import json
import time
import os
import re
import math
import bisect
import fcntl
import statistics
from datetime import datetime, timedelta
from collections import defaultdict, Counter

DRY_RUN = "--dry-run" in sys.argv

# ─── CONFIGURATION (Supports Environment Variables for Docker / K8s) ──────────
OLLAMA_API_KEY   = os.getenv("LLM_API_KEY", "sk-your-api-key-here")
PROMETHEUS_URL   = os.getenv("PROMETHEUS_URL", "http://127.0.0.1:9090")
OLLAMA_URL       = os.getenv("LLM_API_URL", "http://127.0.0.1:11434/v1/chat/completions")
OLLAMA_MODEL     = os.getenv("LLM_MODEL", "qwen3:8b")
DISCORD_WEBHOOK  = os.getenv("DISCORD_WEBHOOK", "https://discord.com/api/webhooks/YOUR_ID/YOUR_TOKEN")
PROM_JOB_NAME    = os.getenv("PROM_JOB_NAME", "uptime-kuma")

# Extra fields merged into every LLM request (e.g., {"options": {"num_ctx": 8192}})
OLLAMA_EXTRA_PARAMS = json.loads(os.getenv("LLM_EXTRA_PARAMS", "{}"))

DATA_DIR       = os.getenv("DATA_DIR", "/opt")
HISTORY_FILE   = os.path.join(DATA_DIR, "uptime_history.json")
EVENTS_FILE    = os.path.join(DATA_DIR, "uptime_events.json")
STATE_FILE     = os.path.join(DATA_DIR, "uptime_state.json")
FORECASTS_FILE = os.path.join(DATA_DIR, "uptime_forecasts.json")
LOCK_FILE      = "/tmp/kuma_foresight.lock"

LOOKBACK_HOURS       = int(os.getenv("LOOKBACK_HOURS", "6"))
STEP_SECONDS         = int(os.getenv("STEP_SECONDS", "120"))
FLAP_THRESHOLD       = int(os.getenv("FLAP_THRESHOLD", "3"))
ALERT_COOLDOWN       = int(os.getenv("ALERT_COOLDOWN", "3600"))

DIGEST_HOUR          = int(os.getenv("DIGEST_HOUR", "8"))
WEEKLY_DIGEST_DAY    = int(os.getenv("WEEKLY_DIGEST_DAY", "0"))   # 0 = Monday
WEEKLY_DIGEST_HOUR   = int(os.getenv("WEEKLY_DIGEST_HOUR", "9"))

OVERLAP_BUCKET_MIN   = 15
OVERLAP_MIN_MONITORS = 2

# Duration trend
DURATION_TREND_MIN_EVENTS = 4

# Correlation & live cascading
CORRELATION_WINDOW_MIN  = 10
CORRELATION_MIN_EVENTS  = 5
CORRELATION_MIN_PCT     = 40
CASCADE_TRIGGER_MIN_PCT = 60   # Directional A->B confidence required for live cascade alert
CASCADE_RECENT_DROP_MIN = 20   # Leader outage must have started within this many minutes

# Lead-time / early warning (Relative % + Absolute ms + Jitter CV)
LEAD_TIME_MIN_EVENTS    = 5
LEAD_TIME_HIGH_PCT      = 60
EARLY_WARN_RT_DELTA     = 100   # Absolute ms rise threshold
EARLY_WARN_RT_PCT       = 45    # Relative % rise threshold
EARLY_WARN_MIN_FLOOR_MS = 35    # Minimum ms delta required when triggering via % or jitter
EARLY_WARN_JITTER_CV    = 0.55  # Coefficient of variation (stdev/mean) threshold for jitter

# ─── SHARED INFRASTRUCTURE / CO-LOCATION GROUPS ──────────────────────────────
# How other users configure shared hosting / co-location in their environment:
#   Option A: Edit INFRA_GROUPS below with exact Uptime Kuma monitor names.
#   Option B: Pass INFRA_GROUPS_JSON='{"DC-East": ["App-1", "App-2"]}' via env var.
#   Option C: Set AUTO_GROUP_BY_LABEL = "datacenter" (or "group") to auto-group
#             monitors by their Prometheus metric labels.
DEFAULT_INFRA_GROUPS = {
    "Primary-Colocation": [
        "<Site-name-1>",
        "<name-2>",
        "<name-3>",
        "<n-4>",
        "<n-5>",
        "Land-project",
    ],
    # You can add as many datacenters, cloud regions, or ISP groups as needed:
    # "AWS-us-east-1": ["API-Gateway", "Auth-Service", "Billing-Worker"],
}
INFRA_GROUPS          = json.loads(os.getenv("INFRA_GROUPS_JSON", "null")) or DEFAULT_INFRA_GROUPS
AUTO_GROUP_BY_LABEL   = os.getenv("AUTO_GROUP_BY_LABEL", "")  # e.g., "datacenter" or "group"
INFRA_BUCKET_MIN      = 30     # Bucket size (minutes) for checking "down together"
INFRA_REQUIRE_ALL     = False  # True = ALL monitors in the group must be down together
INFRA_MIN_DOWN        = 4      # Minimum monitors down in a group if INFRA_REQUIRE_ALL is False
INFRA_PERSIST_WINDOWS = 2      # Consecutive buckets required before firing a group alert

# ─── WORKDAY CONFIGURATION (Global Standard) ─────────────────────────────────
# Python weekday numbers: 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri, 5=Sat, 6=Sun
# Default is Mon-Fri ("0,1,2,3,4"). For Sun-Thu regions (e.g., Bangladesh/MENA),
# set env WORKDAYS="6,0,1,2,3" or edit the default string below.
WORKDAYS_RAW  = os.getenv("WORKDAYS", "0,1,2,3,4")
WORKDAYS      = {int(x.strip()) for x in WORKDAYS_RAW.split(",") if x.strip().isdigit()}
WORKDAYS_NAME = os.getenv("WORKDAYS_LABEL", "Mon-Fri" if WORKDAYS == {0, 1, 2, 3, 4} else "Sun-Thu")

# Batch AI triage
BATCH_TRIAGE_MODEL = OLLAMA_MODEL
SEVERITY_EVIDENCE_FLAP_MULTIPLIER = 2

# ─── AI FORECASTING (learn from history -> predict -> back-test -> score) ────
EPISODE_GAP_MIN              = 20
FORECAST_MIN_EPISODES        = 6
FORECAST_MIN_DAYS            = 5
FORECAST_REFRESH_HOURS       = 6
FORECAST_NONE_RECHECK_HOURS  = 24
FORECAST_MAX_HORIZON_HOURS   = 24 * 7 + 2
FORECAST_ALERT_LEAD_HOURS    = 2
EPISODES_IN_PROMPT           = 25
GAPS_IN_PROMPT               = 15
FORECAST_PROMPT_CHAR_BUDGET  = 6000
FORECAST_MAX_BATCHES_PER_RUN = 3
FORECAST_TIME_BUDGET_S       = 480
FORECAST_TIMEOUT_S           = 240
FORECAST_TEMPERATURE         = 0.1

# Hard veto (Python) back-test settings
FORECAST_LOOKBACK_DAYS       = 60
FORECAST_MIN_HITS            = 3
FORECAST_MIN_LIFT            = 2.0
FORECAST_MIN_PROB_ALERT      = 50
FORECAST_WATCH_MIN_PROB      = 30   # Minimum prob for 'watch' status (eligible for Hybrid RT promotion)
FORECAST_MAX_PROB            = 95

# Scoring / learning feedback
FORECAST_SCORE_GRACE_MIN     = 30
FORECAST_SCORE_MAX_DELAY_H   = LOOKBACK_HOURS - 1
FORECAST_TRACK_MIN_SCORED    = 3
FORECAST_TRACK_MIN_HITRATE   = 40
# ──────────────────────────────────────────────────────────────────────────────

WEEKDAYS      = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
PATTERN_TYPES = ("time_of_day", "workday", "weekly", "interval", "burst", "none")
THINK_RE      = re.compile(r"<think>.*?</think>", re.S | re.I)


def fmt_dt(ts, fmt="%m-%d %H:%M"):
    return datetime.fromtimestamp(ts).strftime(fmt)


def fmt_day(ts):
    return f"{WEEKDAYS[datetime.fromtimestamp(ts).weekday()]} {fmt_dt(ts)}"


# ─── STATE & HISTORY ──────────────────────────────────────────────────────────

def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception as e:
            bad = f"{path}.corrupt-{int(time.time())}"
            try:
                os.replace(path, bad)
                print(f"[{datetime.now()}] {path} unreadable ({e}) — kept as {bad}")
            except OSError:
                pass
    return default


def save_json(path, data):
    if DRY_RUN:
        return
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def load_state():
    s = load_json(STATE_FILE, {})
    s.setdefault("cooldowns", {})
    s.setdefault("first_seen", {})
    s.setdefault("last_digest_date", "")
    s.setdefault("last_weekly_date", "")
    return s


def load_history():
    return load_json(HISTORY_FILE, {})


def load_events():
    return load_json(EVENTS_FILE, {})


def load_forecasts():
    f = load_json(FORECASTS_FILE, {})
    f.setdefault("current", {})
    f.setdefault("scored", {})
    return f


def acquire_lock():
    try:
        fh = open(LOCK_FILE, "w")
    except OSError:
        return True
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fh
    except OSError:
        fh.close()
        return None


def record_downtime(history, name, ts):
    if name not in history:
        history[name] = []
    if history[name] and ts - history[name][-1] < 300:
        return
    history[name].append(ts)
    history[name] = history[name][-2000:]


def record_completed_event(events_store, name, event):
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


# ─── BASIC HELPERS & ENHANCED RT / JITTER DETECTION ───────────────────────────

def count_flaps(statuses):
    return sum(1 for i in range(1, len(statuses)) if statuses[i] != statuses[i - 1])


def evaluate_rt_window(vals):
    """
    Evaluates a list of positive RT samples for:
      1. Absolute delta (>= EARLY_WARN_RT_DELTA)
      2. Relative percentage rise (>= EARLY_WARN_RT_PCT and >= EARLY_WARN_MIN_FLOOR_MS)
      3. High jitter / variance spike (CV >= EARLY_WARN_JITTER_CV and stdev >= EARLY_WARN_MIN_FLOOR_MS)
    """
    clean = [v for v in vals if v > 0]
    if len(clean) < 4:
        return {"trend": None, "rising": False, "delta_ms": 0, "pct_rise": 0.0, "jitter_cv": 0.0, "reason": None}

    half  = len(clean) // 2
    early = sum(clean[:half]) / half
    late  = sum(clean[half:]) / (len(clean) - half)
    diff  = late - early
    pct   = (diff / max(early, 1.0)) * 100.0

    mean_all = sum(clean) / len(clean)
    stdev    = statistics.pstdev(clean) if len(clean) > 1 else 0.0
    cv       = stdev / max(mean_all, 1.0)

    abs_spike    = diff >= EARLY_WARN_RT_DELTA
    rel_spike    = (pct >= EARLY_WARN_RT_PCT) and (diff >= EARLY_WARN_MIN_FLOOR_MS)
    jitter_spike = (cv >= EARLY_WARN_JITTER_CV) and (stdev >= EARLY_WARN_MIN_FLOOR_MS)

    is_rising = abs_spike or rel_spike or jitter_spike
    if is_rising:
        trend = "increasing"
    elif diff <= -30 or pct <= -25:
        trend = "decreasing"
    else:
        trend = "stable"

    reasons = []
    if abs_spike or rel_spike:
        reasons.append(f"+{round(diff)}ms (+{round(pct)}%)")
    if jitter_spike:
        reasons.append(f"unstable RT jitter (±{round(stdev)}ms, CV={cv:.2f})")

    return {
        "trend":     trend,
        "rising":    is_rising,
        "delta_ms":  round(diff),
        "pct_rise":  round(pct, 1),
        "jitter_cv": round(cv, 2),
        "reason":    ", ".join(reasons) if reasons else None,
    }


def trend_label(vals):
    return evaluate_rt_window(vals)["trend"]


def rt_trend(rt_raw):
    vals = [float(v[1]) for v in rt_raw if float(v[1]) > 0]
    if len(vals) < 6:
        return "insufficient data"
    third = len(vals) // 3
    early = sum(vals[:third]) / third
    late  = sum(vals[-third:]) / third
    diff  = late - early
    pct   = (diff / max(early, 1.0)) * 100.0
    if abs(diff) < 35 and abs(pct) < 30:
        return f"stable (~{round(late)}ms)"
    elif diff > 0:
        return f"increasing (+{round(diff)}ms / +{round(pct)}%, now ~{round(late)}ms)"
    else:
        return f"decreasing ({round(diff)}ms / {round(pct)}%, now ~{round(late)}ms)"


def short_term_rising(rt_raw, minutes=30):
    cutoff = time.time() - minutes * 60
    vals = [float(v[1]) for v in rt_raw if float(v[0]) >= cutoff and float(v[1]) > 0]
    info = evaluate_rt_window(vals)
    return info["rising"], info["delta_ms"], info["reason"]


def extract_down_events(status_raw, rt_raw):
    """
    Extracts each down->up cycle from Prometheus status series.
    Filters out <= 0 failed-scrape RT values so pre_rt_trend is never poisoned.
    """
    events     = []
    in_down    = False
    down_start = None
    left_trunc = False

    rt_lookup       = {float(v[0]): float(v[1]) for v in (rt_raw or []) if float(v[1]) > 0}
    rt_times_sorted = sorted(rt_lookup.keys())

    for i, (ts_str, val_str) in enumerate(status_raw):
        ts, val = float(ts_str), float(val_str)

        if val == 0.0 and not in_down:
            in_down    = True
            down_start = ts
            left_trunc = (i == 0)

        elif val == 1.0 and in_down:
            in_down  = False
            duration = round((ts - down_start) / 60, 1)

            pre_rts    = [rt_lookup[t] for t in rt_times_sorted if down_start - 1800 <= t < down_start]
            pre_rt_avg = round(sum(pre_rts) / len(pre_rts), 0) if pre_rts else None
            rt_eval    = evaluate_rt_window(pre_rts) if pre_rts else {"trend": None, "jitter_cv": 0.0}

            ev = {
                "start":        datetime.fromtimestamp(down_start).strftime("%H:%M"),
                "end":          datetime.fromtimestamp(ts).strftime("%H:%M"),
                "duration_m":   duration,
                "pre_rt_ms":    pre_rt_avg,
                "start_epoch":  down_start,
                "pre_rt_trend": rt_eval["trend"],
                "pre_jitter":   rt_eval["jitter_cv"],
            }
            if left_trunc:
                ev["left_truncated"] = True
            events.append(ev)

    if in_down:
        ev = {
            "start":        datetime.fromtimestamp(down_start).strftime("%H:%M"),
            "end":          "ongoing",
            "duration_m":   round((float(status_raw[-1][0]) - down_start) / 60, 1),
            "pre_rt_ms":    None,
            "start_epoch":  down_start,
            "pre_rt_trend": None,
            "ongoing":      True,
        }
        if left_trunc:
            ev["left_truncated"] = True
        events.append(ev)

    return events


def detect_overlaps(st_by_name):
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


# ─── MULTI-GROUP SHARED INFRASTRUCTURE / CO-LOCATION DETECTION ───────────────

def build_effective_infra_groups(st_results, configured_groups, auto_label=AUTO_GROUP_BY_LABEL):
    """
    Merges manual INFRA_GROUPS with optional Prometheus label auto-discovery.
    Allows any user in any environment to group monitors either via config dict
    or automatically via Prometheus labels (e.g., datacenter, provider, group).
    """
    groups = {k: list(v) for k, v in (configured_groups or {}).items()}
    if auto_label:
        auto_map = defaultdict(list)
        for r in st_results:
            m_name = r["metric"].get("monitor_name")
            l_val  = r["metric"].get(auto_label)
            if m_name and l_val:
                auto_map[f"{auto_label}:{l_val}"].append(m_name)
        for g_name, members in auto_map.items():
            if len(members) >= 2:
                groups[g_name] = sorted(set(groups.get(g_name, []) + members))
    return groups


def detect_infra_group_issues(st_by_name, groups_dict,
                              require_all=INFRA_REQUIRE_ALL,
                              min_monitors=INFRA_MIN_DOWN,
                              persist_windows=INFRA_PERSIST_WINDOWS,
                              bucket_min=INFRA_BUCKET_MIN):
    """
    Evaluates every configured co-location / shared-infrastructure group.
    Returns a list of active group outage dicts.
    """
    active_issues = []
    bucket_sec = bucket_min * 60

    for group_name, monitor_list in (groups_dict or {}).items():
        monitored_members = [n for n in monitor_list if n in st_by_name]
        if not monitored_members:
            continue

        required = len(monitored_members) if require_all else min(min_monitors, len(monitored_members))
        if len(monitored_members) < 2 or len(monitored_members) < required:
            continue

        buckets = defaultdict(set)
        for name in monitored_members:
            for ts_str, val in st_by_name[name]:
                if float(val) == 0.0:
                    b = int(float(ts_str) // bucket_sec) * bucket_sec
                    buckets[b].add(name)

        if not buckets:
            continue

        sorted_buckets = sorted(buckets.items())
        last_bucket_time, last_bucket_monitors = sorted_buckets[-1]
        if len(last_bucket_monitors) < required:
            continue

        consecutive = 0
        since_bucket_time = last_bucket_time
        for b_time, monitors in reversed(sorted_buckets):
            if len(monitors) >= required:
                consecutive += 1
                since_bucket_time = b_time
            else:
                break

        if consecutive >= persist_windows:
            active_issues.append({
                "group_name":          group_name,
                "monitors":            sorted(last_bucket_monitors),
                "count":               len(last_bucket_monitors),
                "consecutive_windows": consecutive,
                "since":               datetime.fromtimestamp(since_bucket_time).strftime("%Y-%m-%d %H:%M"),
                "total_monitored":     len(monitored_members),
            })

    return active_issues


# ─── OUTAGE EPISODES & PER-MONITOR BASELINE ──────────────────────────────────

def build_episodes(history, events_store, name, gap_min=EPISODE_GAP_MIN):
    gap       = gap_min * 60
    intervals = [[t, t + STEP_SECONDS] for t in history.get(name, [])]
    ev_list   = [e for e in events_store.get(name, []) if "start_epoch" in e and "duration_m" in e]
    for e in ev_list:
        intervals.append([e["start_epoch"], e["start_epoch"] + e["duration_m"] * 60])
    if not intervals:
        return []

    intervals.sort()
    merged = [intervals[0][:]]
    for s, e in intervals[1:]:
        if s - merged[-1][1] <= gap:
            if e > merged[-1][1]:
                merged[-1][1] = e
        else:
            merged.append([s, e])

    ev_pre   = sorted((x["start_epoch"], x["pre_rt_trend"]) for x in ev_list if x.get("pre_rt_trend"))
    ev_pre_t = [t for t, _ in ev_pre]

    episodes = []
    for s, e in merged:
        ep = {"start_epoch": s, "end_epoch": e, "duration_m": round((e - s) / 60, 1), "pre_rt_trend": None}
        i = bisect.bisect_left(ev_pre_t, s - 600)
        j = bisect.bisect_right(ev_pre_t, s + 600)
        if i < j:
            ep["pre_rt_trend"] = min(ev_pre[i:j], key=lambda x: abs(x[0] - s))[1]
        episodes.append(ep)
    return episodes


def recording_start(history, events_store, fallback):
    firsts = [min(v) for v in history.values() if v]
    for v in events_store.values():
        starts = [e["start_epoch"] for e in v if "start_epoch" in e]
        if starts:
            firsts.append(min(starts))
    return min(firsts) if firsts else fallback


def monitor_data_start(name, state, history, events_store, global_start, now):
    """Tracks per-monitor observation start so new monitors aren't penalized by older global history."""
    candidates = []
    if name in state.get("first_seen", {}):
        candidates.append(state["first_seen"][name])
    if history.get(name):
        candidates.append(min(history[name]))
    ev_starts = [e["start_epoch"] for e in events_store.get(name, []) if "start_epoch" in e]
    if ev_starts:
        candidates.append(min(ev_starts))

    if not candidates:
        state.setdefault("first_seen", {})[name] = now
        return now

    earliest = min(candidates)
    state.setdefault("first_seen", {})[name] = earliest
    return max(earliest, global_start)


def per_day_counts(episodes, now, days=14):
    today  = datetime.fromtimestamp(now).date()
    counts = Counter(datetime.fromtimestamp(e["start_epoch"]).date() for e in episodes)
    return [counts.get(today - timedelta(days=d), 0) for d in range(days - 1, -1, -1)]


def rolling_2h_clusters(hour_counts):
    """Detects 2-hour adjacent clusters including midnight wrap-around (23h+00h)."""
    pairs = []
    for h in range(24):
        nh = (h + 1) % 24
        c  = hour_counts.get(h, 0) + hour_counts.get(nh, 0)
        if c > 0:
            pairs.append((c, f"{h:02d}h-{nh:02d}h({c})"))
    pairs.sort(reverse=True)
    return [p[1] for p in pairs[:3]]


def history_baseline(episodes, mon_start, now):
    if not episodes:
        return None
    span_days   = max((now - mon_start) / 86400, 1.0)
    durs        = sorted(e["duration_m"] for e in episodes)
    starts      = [datetime.fromtimestamp(e["start_epoch"]) for e in episodes]
    hour_counts = Counter(s.hour for s in starts)
    day_counts  = Counter(s.weekday() for s in starts)
    workday_eps = sum(1 for s in starts if s.weekday() in WORKDAYS)
    gaps = [(episodes[i + 1]["start_epoch"] - episodes[i]["start_epoch"]) / 3600
            for i in range(len(episodes) - 1)]
    return {
        "n":            len(episodes),
        "span_days":    round(span_days, 1),
        "per_day":      round(len(episodes) / span_days, 2),
        "dur_median":   round(statistics.median(durs), 1),
        "dur_p90":      round(durs[min(len(durs) - 1, int(len(durs) * 0.9))], 1),
        "dur_max":      round(durs[-1], 1),
        "hour_counts":  dict(hour_counts),
        "day_counts":   dict(day_counts),
        "top_hours":    hour_counts.most_common(3),
        "clusters_2h":  rolling_2h_clusters(hour_counts),
        "workday_pct":  round(workday_eps / len(episodes) * 100),
        "gap_median_h": round(statistics.median(gaps), 1) if gaps else None,
        "last_start":   episodes[-1]["start_epoch"],
    }


def baseline_text(b):
    if not b:
        return "no outage history"
    hours = " ".join(f"{h:02d}h({c})" for h, c in b["top_hours"])
    gap   = f", median gap {b['gap_median_h']}h" if b["gap_median_h"] is not None else ""
    return (f"{b['n']} outage episodes in {b['span_days']:.0f} days ({b['per_day']}/day, "
            f"{b['workday_pct']}% on {WORKDAYS_NAME} workdays), "
            f"median duration {b['dur_median']:.0f}m, p90 {b['dur_p90']:.0f}m; "
            f"top hours {hours}{gap}")


# ─── DURATION TREND ──────────────────────────────────────────────────────────

def duration_trend(name, events_store):
    events = [e for e in events_store.get(name, []) if "duration_m" in e and not e.get("ongoing")]
    if len(events) < DURATION_TREND_MIN_EVENTS:
        return None

    sorted_ev = sorted(events, key=lambda e: e["start_epoch"])
    half      = len(sorted_ev) // 2
    older, recent = sorted_ev[:half], sorted_ev[half:]

    older_avg  = sum(e["duration_m"] for e in older)  / len(older)
    recent_avg = sum(e["duration_m"] for e in recent) / len(recent)
    diff = recent_avg - older_avg

    trend = "stable" if abs(diff) < 1 else ("increasing" if diff > 0 else "decreasing")

    return {"trend": trend, "recent_avg": round(recent_avg, 1),
            "older_avg": round(older_avg, 1), "n": len(sorted_ev)}


# ─── CORRELATION & DIRECTIONAL CASCADING ALERTS ──────────────────────────────

def compute_correlations(events_store, window_min=CORRELATION_WINDOW_MIN,
                         min_events=CORRELATION_MIN_EVENTS, min_pct=CORRELATION_MIN_PCT):
    names = [n for n, evs in events_store.items() if len(evs) >= min_events]
    window_sec = window_min * 60
    results = []

    for a in names:
        a_events = events_store[a]
        for b in names:
            if a == b:
                continue
            b_starts = [e["start_epoch"] for e in events_store[b]]
            co_match = 0
            lead_lags = []
            for e in a_events:
                a_ts = e["start_epoch"]
                if any(abs(a_ts - bs) <= window_sec for bs in b_starts):
                    co_match += 1
                lags = [bs - a_ts for bs in b_starts if 0 <= (bs - a_ts) <= window_sec]
                if lags:
                    lead_lags.append(min(lags))

            pct = round(co_match / len(a_events) * 100, 1)
            lead_pct = round(len(lead_lags) / len(a_events) * 100, 1)
            avg_lag_m = round((sum(lead_lags) / len(lead_lags)) / 60, 1) if lead_lags else 0.0

            if pct >= min_pct:
                results.append({
                    "a": a, "b": b, "pct": pct, "lead_pct": lead_pct,
                    "avg_lag_m": avg_lag_m, "n": len(a_events)
                })

    results.sort(key=lambda r: (-r["lead_pct"], -r["pct"]))
    return results[:10]


def detect_cascading_risks(events_store, live, episodes_by_name, infra_affected_names, state, now):
    corrs = compute_correlations(events_store, min_pct=CASCADE_TRIGGER_MIN_PCT)
    alerts = []
    for c in corrs:
        if c["lead_pct"] < CASCADE_TRIGGER_MIN_PCT:
            continue
        leader, follower = c["a"], c["b"]
        if leader in infra_affected_names or follower in infra_affected_names:
            continue
        if live.get(follower, {}).get("status") == 0.0:
            continue

        leader_eps = episodes_by_name.get(leader, [])
        if not leader_eps:
            continue
        last_leader_start = leader_eps[-1]["start_epoch"]
        is_leader_down = live.get(leader, {}).get("status") == 0.0
        recent_drop = (now - last_leader_start) <= CASCADE_RECENT_DROP_MIN * 60

        if is_leader_down or recent_drop:
            cd_key = f"cascade:{leader}->{follower}"
            if now - state["cooldowns"].get(cd_key, 0) >= ALERT_COOLDOWN:
                alerts.append((cd_key, cascading_alert_embed(leader, follower, c, last_leader_start)))
    return alerts


# ─── LEAD-TIME RELIABILITY ───────────────────────────────────────────────────

def lead_time_reliability(name, events_store, min_events=LEAD_TIME_MIN_EVENTS):
    events = [e for e in events_store.get(name, []) if e.get("pre_rt_trend")]
    if len(events) < min_events:
        return None
    increasing = sum(1 for e in events if e["pre_rt_trend"] in ("increasing", "বৃদ্ধি"))
    pct = round(increasing / len(events) * 100, 1)
    return {"pct": pct, "n": len(events), "reliable": pct >= LEAD_TIME_HIGH_PCT}


# ─── LLM / OLLAMA CLIENT ─────────────────────────────────────────────────────

def ollama(prompt, temperature=0.2, timeout=120, model=None, json_mode=False):
    final_prompt = f"{prompt}\n/no_think" if json_mode else prompt
    payload = {
        "model": model or OLLAMA_MODEL,
        "messages": [{"role": "user", "content": final_prompt}],
        "temperature": temperature,
        "stream": False,
    }
    if json_mode:
        payload["response_format"] = {"type": "json_object"}
    payload.update(OLLAMA_EXTRA_PARAMS)

    r = requests.post(
        OLLAMA_URL,
        json=payload,
        headers={"Authorization": f"Bearer {OLLAMA_API_KEY}"},
        timeout=timeout,
    )
    if r.status_code >= 400 and json_mode and "response_format" in payload:
        payload.pop("response_format", None)
        r = requests.post(
            OLLAMA_URL,
            json=payload,
            headers={"Authorization": f"Bearer {OLLAMA_API_KEY}"},
            timeout=timeout,
        )
    r.raise_for_status()
    text = r.json()["choices"][0]["message"]["content"].strip()
    return THINK_RE.sub("", text).strip()


def extract_json(raw):
    raw = THINK_RE.sub("", raw or "").strip()
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.I).strip()
    for opener, closer in (("[", "]"), ("{", "}")):
        i, j = raw.find(opener), raw.rfind(closer)
        if i != -1 and j > i:
            try:
                parsed = json.loads(raw[i:j + 1])
                if isinstance(parsed, dict):
                    for k, v in parsed.items():
                        if isinstance(v, list):
                            return v
                return parsed
            except Exception:
                pass
    return None


def norm_name(s):
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()


# ─── AI FORECASTING: LEARN FROM HISTORY -> PREDICT -> BACK-TEST -> SCORE ─────

FORECAST_PROMPT = f"""You are a Principal SRE. Your job is to LEARN each monitor's outage pattern from its own history and to PREDICT its next outage.

Current time (server local): @@NOW@@. All times below are server local time, 24h clock.
Configured standard workdays for this environment: {WORKDAYS_NAME}.

For every monitor you receive its outage history: individual outage episodes (start, weekday, duration), hourly & 2h-cluster histograms (including 23h-00h midnight wrap-around), gaps between episodes, and how YOUR previous forecasts for it turned out.

Perform these steps for EACH monitor:
1. Study the history and decide whether a REAL recurring pattern exists. Allowed pattern types:
   time_of_day - outages start at about the same clock time on most days (all 7 days)
   workday     - outages start at about the same clock time on configured workdays ({WORKDAYS_NAME}), rare on weekends
   weekly      - outages start on one specific weekday at about the same clock time
   interval    - roughly constant gap (hours) between consecutive outage starts, not tied to the clock
   burst       - outages cluster: after one outage another usually follows after a typical short gap
   none        - no reliable pattern (random, too few episodes, or the pattern recently broke)
2. If a pattern exists, state the rule. Do NOT calculate dates - Python turns your rule into the next time window.
   - time_of_day / workday / weekly: "time_of_day" = clock time "HH:MM" (or up to two daily peaks separated by a comma, e.g. "11:00, 15:30" if bimodal). weekly also requires "weekday".
   - interval / burst: "interval_hours" = typical gap between consecutive outage starts.
3. Provide "window_minutes" (15-180, smaller = sharper), "probability_pct" (0-100), and "expected_duration_min".

Rules:
- Use ONLY the data shown. "evidence" must cite concrete numbers from the data (counts, dates, gaps, 2h clusters).
- Check midnight wrap-around: if outages cluster at 23h and 00h, pick the true center (e.g. "23:55") with an appropriate window_minutes.
- Learn from your previous forecasts: if they MISSED, lower the probability or answer "none".
- Every rule is back-tested automatically against the raw history. "none" is always better than a guess.

Reply with ONLY a JSON array (no markdown fences, no text before or after), one object per monitor:
[
  {{
    "monitor": "<exact monitor name as written after ###>",
    "evidence": "<max 2 short sentences citing numbers from the data>",
    "pattern_type": "time_of_day|workday|weekly|interval|burst|none",
    "time_of_day": "<HH:MM or HH:MM, HH:MM, or null>",
    "weekday": "<Mon|Tue|Wed|Thu|Fri|Sat|Sun, or null>",
    "interval_hours": <number, or null>,
    "window_minutes": <15-180>,
    "probability_pct": <0-100>,
    "expected_duration_min": <number, or null>,
    "confidence": "LOW|MEDIUM|HIGH",
    "reasoning": "<1-2 concise English sentences: the pattern learned and what to expect next>"
  }}
]

MONITORS:
@@MONITORS@@
"""


def to_num(x, default=None):
    if isinstance(x, bool) or x is None:
        return default
    if isinstance(x, (int, float)):
        return float(x)
    m = re.search(r"-?\d+(?:\.\d+)?", str(x))
    return float(m.group()) if m else default


def parse_clock_times(raw_str):
    if not raw_str:
        return []
    out = []
    for part in str(raw_str).split(","):
        m = re.match(r"\s*(\d{1,2}):(\d{2})\s*([ap]m)?", part.strip(), re.I)
        if not m:
            continue
        hh, mm, ap = int(m.group(1)), int(m.group(2)), (m.group(3) or "").lower()
        if ap == "pm" and hh < 12:
            hh += 12
        if ap == "am" and hh == 12:
            hh = 0
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            t_str = f"{hh:02d}:{mm:02d}"
            if t_str not in out:
                out.append(t_str)
    return out[:2]


def parse_ai_rule(d):
    pt_raw = str(d.get("pattern_type") or "none").strip().lower().replace("-", "_").replace(" ", "_")
    reasoning = d.get("reasoning") or d.get("reasoning_bn") or ""
    rule = {
        "pattern_type":          pt_raw if pt_raw in PATTERN_TYPES else "none",
        "time_of_day":           None,
        "times_of_day":          [],
        "weekday":               None,
        "interval_hours":        None,
        "window_minutes":        int(min(180, max(15, to_num(d.get("window_minutes"), 60)))),
        "probability_pct":       None,
        "expected_duration_min": to_num(d.get("expected_duration_min")),
        "confidence":            str(d.get("confidence") or "LOW").strip().upper(),
        "evidence":              str(d.get("evidence") or "")[:400],
        "reasoning":             str(reasoning)[:600],
    }
    p = to_num(d.get("probability_pct"))
    if p is not None:
        rule["probability_pct"] = min(100.0, max(0.0, p))
    if rule["confidence"] not in ("LOW", "MEDIUM", "HIGH"):
        rule["confidence"] = "LOW"

    if pt_raw not in PATTERN_TYPES:
        return rule, f"unknown pattern_type '{pt_raw}'"
    if pt_raw == "none":
        return rule, None

    if pt_raw in ("time_of_day", "workday", "weekly"):
        times = parse_clock_times(d.get("time_of_day"))
        if not times:
            return rule, "missing/invalid time_of_day"
        rule["times_of_day"] = times
        rule["time_of_day"]  = ", ".join(times)

    if pt_raw == "weekly":
        wd = str(d.get("weekday") or "").strip()[:3].title()
        if wd not in WEEKDAYS:
            return rule, "missing/invalid weekday"
        rule["weekday"] = WEEKDAYS.index(wd)

    if pt_raw in ("interval", "burst"):
        ih = to_num(d.get("interval_hours"))
        if ih is None or not (0.25 <= ih <= 24 * 14):
            return rule, "missing/invalid interval_hours"
        rule["interval_hours"] = round(ih, 2)

    return rule, None


def rule_text(rule):
    if not rule:
        return "none"
    t, w = rule.get("pattern_type"), rule.get("window_minutes", 60)
    tod  = rule.get("time_of_day")
    if t == "time_of_day":
        return f"daily ~{tod} ±{w}m"
    if t == "workday":
        return f"workdays ({WORKDAYS_NAME}) ~{tod} ±{w}m"
    if t == "weekly":
        wd  = rule.get("weekday")
        day = WEEKDAYS[wd] if isinstance(wd, int) and 0 <= wd < 7 else "?"
        return f"weekly {day} ~{tod} ±{w}m"
    ih = rule.get("interval_hours")
    ih_txt = f"{ih:g}" if isinstance(ih, (int, float)) else "?"
    if t == "interval":
        return f"every ~{ih_txt}h ±{w}m"
    if t == "burst":
        return f"burst: next outage ~{ih_txt}h after previous ±{w}m"
    return "none"


def next_occurrence(rule, starts, now):
    t = rule["pattern_type"]
    if t in ("time_of_day", "workday", "weekly"):
        times = rule.get("times_of_day") or [rule["time_of_day"]]
        candidates = []
        base_dt = datetime.fromtimestamp(now)
        for t_str in times:
            hh, mm = (int(x) for x in t_str.split(":"))
            cand = base_dt.replace(hour=hh, minute=mm, second=0, microsecond=0)
            for _ in range(9):
                wd = cand.weekday()
                valid_day = (
                    (t == "time_of_day") or
                    (t == "workday" and wd in WORKDAYS) or
                    (t == "weekly" and wd == rule["weekday"])
                )
                if cand.timestamp() > now and valid_day:
                    candidates.append(cand.timestamp())
                    break
                cand += timedelta(days=1)
        return min(candidates) if candidates else None

    if t in ("interval", "burst"):
        if not starts:
            return None
        period = rule["interval_hours"] * 3600
        cand   = starts[-1] + period
        if t == "burst":
            return cand if cand + rule["window_minutes"] * 60 > now else None
        while cand <= now:
            cand += period
        return cand
    return None


def clock_support(starts, center, half_min, mon_start, now, mode="time_of_day"):
    weekly  = (mode == "weekly")
    workday = (mode == "workday")
    step  = 7 * 86400 if weekly else 86400
    max_k = max(1, FORECAST_LOOKBACK_DAYS // 7) if weekly else FORECAST_LOOKBACK_DAYS
    half  = half_min * 60
    wins  = []

    for k in range(1, max_k + 1):
        t = center - k * step
        if t + half > now:
            continue
        if t - half < mon_start:
            break
        if workday and datetime.fromtimestamp(t).weekday() not in WORKDAYS:
            continue
        wins.append(t)

    if len(wins) < FORECAST_MIN_HITS:
        return None

    hit_flags, matched = [], set()
    for t in wins:
        m = [s for s in starts if abs(s - t) <= half]
        hit_flags.append(bool(m))
        matched.update(m)

    hits = sum(hit_flags)
    if workday:
        in_range = [s for s in starts if wins[-1] - half <= s <= now
                    and datetime.fromtimestamp(s).weekday() in WORKDAYS]
    else:
        in_range = [s for s in starts if wins[-1] - half <= s <= now]

    lam    = len(in_range) / len(wins)
    chance = 1 - math.exp(-lam * (2 * half / (86400 if workday else step)))
    p      = hits / len(wins)
    recent = hit_flags[:3] if weekly else hit_flags[:5] if workday else hit_flags[:7]

    return {
        "kind": mode, "windows": len(wins), "hits": hits,
        "p": round(p, 3), "coverage": round(len(matched) / max(1, len(in_range)), 3),
        "chance": round(chance, 4), "lift": round(min(999.0, p / max(chance, 1e-6)), 1),
        "recent_hits": sum(recent), "recent_windows": len(recent),
    }


def gap_support(starts, interval_h, tol_h, now, is_interval=True):
    gaps = [(starts[i + 1] - starts[i]) / 3600 for i in range(len(starts) - 1)][-30:]
    if len(gaps) < FORECAST_MIN_HITS:
        return None
    mean = sum(gaps) / len(gaps)
    if mean <= 0:
        return None

    ok = [abs(g - interval_h) <= tol_h for g in gaps]

    if is_interval and starts:
        elapsed_h = (now - starts[-1]) / 3600.0
        if elapsed_h > interval_h + tol_h:
            missed_cycles = min(5, int(elapsed_h // max(interval_h, 0.5)))
            ok.extend([False] * missed_cycles)

    hits   = sum(ok)
    p      = hits / len(ok)
    lo, hi = max(interval_h - tol_h, 0.0), interval_h + tol_h
    chance = math.exp(-lo / mean) - math.exp(-hi / mean)
    recent = ok[-5:]
    return {
        "kind": "gap", "windows": len(ok), "hits": hits, "p": round(p, 3), "coverage": round(p, 3),
        "chance": round(chance, 4), "lift": round(min(999.0, p / max(chance, 1e-6)), 1),
        "recent_hits": sum(recent), "recent_windows": len(recent),
    }


def make_forecast_record(name, d, episodes, mon_start, now):
    rule, err = parse_ai_rule(d)
    starts = [e["start_epoch"] for e in episodes]
    rec = {
        "id": f"{name}@{int(now)}", "made_at": now,
        "last_ep_start": starts[-1] if starts else 0, "n_episodes": len(episodes),
        "status": "rejected", "reason": None, "rule": rule, "window": None,
        "prob_pct": 0, "ai_prob_pct": rule["probability_pct"], "backtest": None,
        "alerted_at": 0, "scored": False,
        "evidence": rule["evidence"], "reasoning": rule["reasoning"],
        "expected_duration_min": rule["expected_duration_min"], "confidence": rule["confidence"],
    }
    if err:
        rec["reason"] = err
        return rec
    t = rule["pattern_type"]
    if t == "none":
        rec["status"] = "none"
        return rec

    center = next_occurrence(rule, starts, now)
    if center is None:
        rec["reason"] = "rule gives no upcoming window (burst follow-up already overdue)"
        return rec
    if center - now > FORECAST_MAX_HORIZON_HOURS * 3600:
        rec["reason"] = "next window is beyond the forecast horizon"
        return rec

    half_min = rule["window_minutes"]
    if t in ("time_of_day", "workday", "weekly"):
        bt = clock_support(starts, center, half_min, mon_start, now, mode=t)
    else:
        bt = gap_support(starts, rule["interval_hours"], half_min / 60.0, now, is_interval=(t == "interval"))

    if bt is None:
        rec["reason"] = "not enough comparable history to back-test this rule"
        return rec
    rec["backtest"] = bt

    if bt["hits"] < FORECAST_MIN_HITS:
        rec["reason"] = (f"pattern happened only {bt['hits']}x in {bt['windows']} comparable windows "
                         f"(need {FORECAST_MIN_HITS}+)")
        return rec
    if bt["lift"] < FORECAST_MIN_LIFT:
        rec["reason"] = (f"hit-rate {bt['p'] * 100:.0f}% is only {bt['lift']:.1f}x better than chance "
                         f"(need {FORECAST_MIN_LIFT}x)")
        return rec
    if bt["recent_windows"] >= 3 and bt["recent_hits"] == 0:
        rec["reason"] = "pattern not seen in the most recent comparable windows (it may have stopped)"
        return rec

    ai_p = rule["probability_pct"] if rule["probability_pct"] is not None else 100.0
    rec["prob_pct"] = int(round(min(FORECAST_MAX_PROB, bt["p"] * 100, ai_p)))
    rec["window"]   = [center - half_min * 60, center + half_min * 60]
    if rec["prob_pct"] >= FORECAST_MIN_PROB_ALERT:
        rec["status"] = "accepted"
    elif rec["prob_pct"] >= FORECAST_WATCH_MIN_PROB:
        rec["status"] = "watch"
    else:
        rec["status"] = "rejected"
        rec["reason"] = f"capped probability {rec['prob_pct']}% is below minimum watch threshold ({FORECAST_WATCH_MIN_PROB}%)"
    return rec


def forecast_log_line(name, rec):
    if rec["status"] in ("accepted", "watch"):
        bt = rec["backtest"]
        return (f"Forecast {name}: {rec['status']} {rule_text(rec['rule'])} p={rec['prob_pct']}% "
                f"(AI claimed {rec['ai_prob_pct']}, back-test {bt['hits']}/{bt['windows']}, lift {bt['lift']}x) "
                f"window {fmt_dt(rec['window'][0])}..{fmt_dt(rec['window'][1], '%H:%M')}")
    if rec["status"] == "none":
        return f"Forecast {name}: AI sees no reliable pattern"
    return f"Forecast {name}: REJECTED {rule_text(rec['rule'])} — {rec['reason']}"


# ── accuracy tracking ──

def track_record(forecasts, name, last=10):
    items = [x for x in forecasts["scored"].get(name, []) if x.get("outcome") in ("HIT", "MISS")][-last:]
    return sum(1 for x in items if x["outcome"] == "HIT"), len(items)


def score_forecasts(forecasts, episodes_by_name, now):
    g = FORECAST_SCORE_GRACE_MIN * 60
    for name, cur in forecasts["current"].items():
        if cur.get("scored") or not cur.get("window"):
            continue
        ws, we = cur["window"]
        if now < we + g:
            continue
        if name not in episodes_by_name or now - (we + g) > FORECAST_SCORE_MAX_DELAY_H * 3600:
            outcome, actual = "UNKNOWN", None
        else:
            lo   = max(ws - g, cur["made_at"])
            hits = [e["start_epoch"] for e in episodes_by_name[name] if lo <= e["start_epoch"] <= we + g]
            outcome, actual = ("HIT", hits[0]) if hits else ("MISS", None)
        cur["scored"], cur["outcome"] = True, outcome
        lst = forecasts["scored"].setdefault(name, [])
        lst.append({"made_at": cur["made_at"], "ws": ws, "we": we, "prob_pct": cur["prob_pct"],
                    "rule_text": rule_text(cur["rule"]), "outcome": outcome, "actual_start": actual})
        forecasts["scored"][name] = lst[-30:]
        print(f"[{datetime.now()}] Scored forecast {name}: {outcome} ({rule_text(cur['rule'])}, "
              f"window {fmt_dt(ws)}..{fmt_dt(we, '%H:%M')})")


def forecast_feedback_text(forecasts, name):
    lines = []
    for x in forecasts["scored"].get(name, [])[-4:]:
        line = (f"  - made {fmt_dt(x['made_at'])}: predicted {x['rule_text']}, "
                f"window {fmt_dt(x['ws'])}..{fmt_dt(x['we'], '%H:%M')}, p={x['prob_pct']}% -> {x['outcome']}")
        if x["outcome"] == "HIT" and x.get("actual_start"):
            line += f" (outage began {fmt_dt(x['actual_start'], '%H:%M')})"
        lines.append(line)
    cur = forecasts["current"].get(name)
    if cur:
        if cur["status"] in ("accepted", "watch") and cur.get("window") and not cur.get("scored"):
            lines.append(f"  - PENDING (made {fmt_dt(cur['made_at'])}): {rule_text(cur['rule'])}, "
                         f"window {fmt_dt(cur['window'][0])}..{fmt_dt(cur['window'][1], '%H:%M')}, p={cur['prob_pct']}%")
        elif cur["status"] == "rejected":
            lines.append(f"  - your last rule ({rule_text(cur['rule'])}) was REJECTED by back-test: {cur.get('reason')}")
        elif cur["status"] == "none":
            lines.append("  - your last answer was: none (no reliable pattern)")
        if cur.get("evidence"):
            lines.append(f"  - your last hypothesis (re-verify against the data): {cur['evidence']}")
    if not lines:
        return "your_previous_forecasts: none yet (first time evaluating this monitor)\n"
    return "your_previous_forecasts:\n" + "\n".join(lines) + "\n"


def learned_pattern_text(forecasts, name):
    cur = forecasts["current"].get(name)
    if not cur or cur.get("status") not in ("accepted", "watch"):
        return "none learned"
    bt   = cur.get("backtest") or {}
    h, n = track_record(forecasts, name)
    tr   = f"{h}/{n} past forecasts hit" if n else "not yet verified"
    return f"{rule_text(cur['rule'])} (back-test {bt.get('hits')}/{bt.get('windows')}, {tr})"


# ── building the AI's view of one monitor ──

def build_forecast_block(name, episodes, mon_start, now, live, forecasts):
    b        = history_baseline(episodes, mon_start, now)
    hrs      = " ".join(f"{h:02d}h={b['hour_counts'][h]}" for h in sorted(b["hour_counts"]))
    clusters = " ".join(b["clusters_2h"]) or "-"
    days     = " ".join(f"{WEEKDAYS[d]}={b['day_counts'].get(d, 0)}" for d in range(7))
    per_day  = " ".join(str(x) for x in per_day_counts(episodes, now, 14))
    gaps     = [(episodes[i + 1]["start_epoch"] - episodes[i]["start_epoch"]) / 3600
                for i in range(len(episodes) - 1)][-GAPS_IN_PROMPT:]
    gaps_tx  = " ".join(f"{g:.1f}" for g in gaps) or "-"

    ep_lines = []
    for e in episodes[-EPISODES_IN_PROMPT:]:
        dt = datetime.fromtimestamp(e["start_epoch"])
        rt = " rt_rising_before" if e.get("pre_rt_trend") in ("increasing", "বৃদ্ধি") else ""
        ep_lines.append(f"  {dt:%m-%d} {WEEKDAYS[dt.weekday()]} {dt:%H:%M} {e['duration_m']:.0f}m{rt}")
    ep_text = "\n".join(ep_lines)

    state_tx = {1.0: "UP", 2.0: "PENDING", 3.0: "MAINTENANCE"}.get(live["status"], "UP")
    ago_h    = (now - episodes[-1]["end_epoch"]) / 3600
    feedback = forecast_feedback_text(forecasts, name)

    return (
        f"### {name}\n"
        f"now_state: {state_tx}, last outage ended {max(ago_h, 0):.1f}h ago, "
        f"flaps_last_6h={live['flaps']}, response_time: {live['rt_trend']}\n"
        f"history: {b['n']} outage episodes over {b['span_days']:.0f} observed days ({b['per_day']}/day, "
        f"{WORKDAYS_NAME} workday share={b['workday_pct']}%); "
        f"duration_min median={b['dur_median']:.0f} p90={b['dur_p90']:.0f} max={b['dur_max']:.0f}\n"
        f"starts_by_hour: {hrs}\n"
        f"top_2h_rolling_clusters (includes 23h-00h midnight wrap): {clusters}\n"
        f"starts_by_weekday: {days}\n"
        f"episodes_per_day_last14 (oldest..newest): {per_day}\n"
        f"gaps_between_starts_h (last {len(gaps)}, oldest..newest): {gaps_tx}\n"
        f"episodes (oldest..newest, last {len(ep_lines)}: date weekday start duration):\n{ep_text}\n"
        f"{feedback}"
    )


def pack_batches(blocks, budget=None):
    budget = budget or FORECAST_PROMPT_CHAR_BUDGET
    batches, cur, size = [], [], 0
    for name, text in blocks:
        if cur and size + len(text) > budget:
            batches.append(cur)
            cur, size = [], 0
        cur.append((name, text))
        size += len(text)
    if cur:
        batches.append(cur)
    return batches


def ai_forecast_batch(batch, now):
    now_txt = f"{fmt_dt(now, '%Y-%m-%d %H:%M')} ({WEEKDAYS[datetime.fromtimestamp(now).weekday()]})"
    prompt  = (FORECAST_PROMPT.replace("@@NOW@@", now_txt)
                              .replace("@@MONITORS@@", "\n".join(text for _, text in batch)))
    raw  = ollama(prompt, temperature=FORECAST_TEMPERATURE, timeout=FORECAST_TIMEOUT_S, json_mode=True)
    data = extract_json(raw)
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        return None
    return [d for d in data if isinstance(d, dict)]


def ask_forecast(batch, now, t0):
    names   = {norm_name(n): n for n, _ in batch}
    answers = {}

    def absorb(data):
        for d in data or []:
            key = names.get(norm_name(d.get("monitor")))
            if key and key not in answers:
                answers[key] = d

    try:
        data = ai_forecast_batch(batch, now)
    except Exception as e:
        print(f"[{datetime.now()}] Forecast AI call failed ({e}) — skipping the rest of this run's forecasts")
        return answers, False

    if data is None and len(batch) > 1:
        print(f"[{datetime.now()}] Forecast reply wasn't valid JSON — retrying one monitor per call")
        for item in batch:
            if time.time() - t0 > FORECAST_TIME_BUDGET_S:
                break
            try:
                absorb(ai_forecast_batch([item], now))
            except Exception as e:
                print(f"[{datetime.now()}] Forecast AI call failed ({e})")
                return answers, False
    else:
        absorb(data)
    return answers, True


def forecast_due(cur, episodes, now):
    if not cur:
        return True
    age_h    = (now - cur.get("made_at", 0)) / 3600
    new_data = episodes[-1]["start_epoch"] > cur.get("last_ep_start", 0) + 60
    if cur.get("status") in ("none", "rejected"):
        return new_data or age_h >= FORECAST_NONE_RECHECK_HOURS
    if not cur.get("scored"):
        return new_data and age_h >= 1
    return new_data or age_h >= FORECAST_REFRESH_HOURS


def refresh_forecasts(forecasts, episodes_by_name, live, mon_starts, now):
    due = []
    for name, eps in episodes_by_name.items():
        if len(eps) < FORECAST_MIN_EPISODES:
            continue
        m_start = mon_starts.get(name, now)
        if (now - m_start) / 86400 < FORECAST_MIN_DAYS:
            continue
        if live.get(name, {}).get("status") == 0.0:
            continue
        if forecast_due(forecasts["current"].get(name), eps, now):
            due.append(name)

    if not due:
        print(f"[{datetime.now()}] Forecasting: nothing due")
        return 0

    due.sort(key=lambda n: -episodes_by_name[n][-1]["start_epoch"])
    blocks  = [(n, build_forecast_block(n, episodes_by_name[n], mon_starts[n], now, live[n], forecasts)) for n in due]
    batches = pack_batches(blocks)
    print(f"[{datetime.now()}] Forecasting: {len(due)} monitor(s) due, {len(batches)} batch(es), "
          f"running up to {FORECAST_MAX_BATCHES_PER_RUN}")

    t0, done = time.time(), 0
    for batch in batches[:FORECAST_MAX_BATCHES_PER_RUN]:
        if time.time() - t0 > FORECAST_TIME_BUDGET_S:
            print(f"[{datetime.now()}] Forecasting: time budget used up — remaining monitors wait for next run")
            break
        answers, ok = ask_forecast(batch, now, t0)
        for name, _ in batch:
            d = answers.get(name)
            if d is None:
                print(f"[{datetime.now()}] Forecast {name}: no usable answer — will retry next run")
                continue
            rec = make_forecast_record(name, d, episodes_by_name[name], mon_starts[name], now)
            old = forecasts["current"].get(name)
            if old and old.get("window") and rec.get("window") and abs(old["window"][0] - rec["window"][0]) < 3600:
                rec["alerted_at"] = old.get("alerted_at", 0)
            forecasts["current"][name] = rec
            done += 1
            print(f"[{datetime.now()}] {forecast_log_line(name, rec)}")
        save_json(FORECASTS_FILE, forecasts)
        if not ok:
            break
    return done


def collect_forecast_alerts(forecasts, live, now):
    """
    Fires alerts for 'accepted' forecasts, AND promotes 'watch' forecasts (prob 30-49%)
    if live RT or Jitter is actively rising right as the forecast window opens.
    """
    out  = []
    lead = FORECAST_ALERT_LEAD_HOURS * 3600
    for name, cur in forecasts["current"].items():
        status = cur.get("status")
        if status not in ("accepted", "watch") or cur.get("alerted_at") or cur.get("scored") or not cur.get("window"):
            continue
        ws, we = cur["window"]
        if not (ws - lead <= now <= we):
            continue
        m_live = live.get(name, {})
        if m_live.get("status") == 0.0:
            continue

        hybrid_rt_reason = None
        if status == "watch":
            if not m_live.get("rt_rising"):
                continue
            hybrid_rt_reason = m_live.get("rt_reason") or f"+{m_live.get('rt_delta', 0)}ms"

        hits, n = track_record(forecasts, name)
        if n >= FORECAST_TRACK_MIN_SCORED and hits * 100 / n < FORECAST_TRACK_MIN_HITRATE:
            if not cur.get("mute_logged"):
                print(f"[{datetime.now()}] Forecast {name}: MUTED — only {hits}/{n} recent forecasts hit")
                cur["mute_logged"] = True
            continue
        out.append((name, forecast_embed(name, cur, hits, n, hybrid_rt_reason=hybrid_rt_reason)))
    return out


def upcoming_forecasts_text(forecasts, now, hours=24):
    lines = []
    for name, cur in sorted(forecasts["current"].items()):
        if cur.get("status") != "accepted" or cur.get("scored") or not cur.get("window"):
            continue
        ws, we = cur["window"]
        if we < now or ws > now + hours * 3600:
            continue
        lines.append(f"{name}: {fmt_dt(ws)}–{fmt_dt(we, '%H:%M')} (prob {cur['prob_pct']}%, {rule_text(cur['rule'])})")
    return "\n".join(lines)


def forecast_report_block(forecasts, now, days=7):
    cutoff, lines, hit_all, n_all = now - days * 86400, [], 0, 0
    for name in sorted(set(forecasts["scored"]) | set(forecasts["current"])):
        recent = [x for x in forecasts["scored"].get(name, [])
                  if x["we"] >= cutoff and x.get("outcome") in ("HIT", "MISS")]
        h   = sum(1 for x in recent if x["outcome"] == "HIT")
        cur = forecasts["current"].get(name)
        learned = ""
        if cur and cur.get("status") in ("accepted", "watch"):
            bt = cur.get("backtest") or {}
            learned = f"pattern: {rule_text(cur['rule'])} (back-test {bt.get('hits')}/{bt.get('windows')})"
        if not recent and not learned:
            continue
        acc = f"{h}/{len(recent)} hit in last {days}d" if recent else "not yet scored"
        lines.append(f"- {name}: {learned or 'no reliable pattern'}; {acc}")
        hit_all += h
        n_all   += len(recent)
    if not lines:
        return "No forecasts learned or scored yet"
    head = (f"Total: {hit_all}/{n_all} forecasts hit ({round(100 * hit_all / n_all)}%)"
            if n_all else "Total: no forecast windows have elapsed yet")
    return head + "\n" + "\n".join(lines)


# ─── AI DECISION: FLAP ANALYSIS (Fallback Path) ──────────────────────────────

def ai_analyze_flap(name, flap_count, down_events, rt_trend_str, baseline, learned, dur_trend):
    if down_events:
        events_block = "\n".join(
            f"  • {e['start']} -> {e['end']} ({e['duration_m']}m down)"
            + (f", pre-RT: {e['pre_rt_ms']}ms" if e['pre_rt_ms'] else "")
            for e in down_events
        )
    else:
        events_block = "  No completed down events detected"

    hist_block = f"{baseline_text(baseline)}\nLearned pattern: {learned}"

    if dur_trend:
        dur_block = (
            f"Outage duration is {dur_trend['trend']} — older avg {dur_trend['older_avg']}m, "
            f"recent avg {dur_trend['recent_avg']}m (n={dur_trend['n']})"
        )
    else:
        dur_block = "Insufficient history for duration trend"

    now_txt = f"{datetime.now():%Y-%m-%d %H:%M} ({WEEKDAYS[datetime.now().weekday()]})"

    prompt = f"""You are a Principal SRE analyzing monitor instability.

Current time: {now_txt}
Monitor: {name}
Status changes in last 6h: {flap_count}
Response time trend: {rt_trend_str}
Duration trend: {dur_block}

Down events in last 6h:
{events_block}

Monitor historical baseline:
{hist_block}

Answer the 5 fields below using ONLY the provided data. Judge whether the current flapping matches this monitor's historical behavior; if learned pattern is "none learned", do not invent one:

SEVERITY: (LOW / MEDIUM / HIGH / CRITICAL)
TYPE: (recurring_interval / random_instability / single_long_outage / recovery_cycle)
NORMAL: (yes / no)
MATCHES_HISTORY: (yes / no)
ANALYSIS: (2 concise English sentences explaining the outage behavior, comparison to baseline, and operational impact)

Output ONLY the 5 lines above with no intro or markdown."""

    response = ollama(prompt)
    result = {
        "severity": "MEDIUM",
        "type": "random_instability",
        "normal": "no",
        "matches_history": "—",
        "analysis": response,
    }
    for line in response.splitlines():
        if line.startswith("SEVERITY:"):          result["severity"]        = line.split(":", 1)[1].strip()
        elif line.startswith("TYPE:"):            result["type"]            = line.split(":", 1)[1].strip()
        elif line.startswith("NORMAL:"):          result["normal"]          = line.split(":", 1)[1].strip().lower()
        elif line.startswith("MATCHES_HISTORY:"): result["matches_history"] = line.split(":", 1)[1].strip().lower()
        elif line.startswith("ANALYSIS:"):        result["analysis"]        = line.split(":", 1)[1].strip()
    return result


# ─── BATCH AI TRIAGE ─────────────────────────────────────────────────────────

def gather_triage_candidates(name, flaps, current_status, baseline, learned, down_events,
                             rt_trend_str, dur_trend):
    return {
        "name": name,
        "flaps": flaps,
        "current_status": current_status,
        "down_events": down_events,
        "rt_trend": rt_trend_str,
        "dur_trend": dur_trend,
        "baseline": baseline,
        "learned": learned,
    }


def build_triage_prompt_block(c):
    events_txt = "; ".join(
        f"{e['start']}-{e['end']}({e['duration_m']}m)" for e in c["down_events"][-4:]
    ) or "no completed down events"

    dt = c["dur_trend"]
    dur_txt = (f"{dt['trend']} ({dt['older_avg']}m -> {dt['recent_avg']}m, n={dt['n']})"
               if dt else "insufficient history")

    return (
        f"### {c['name']}\n"
        f"flaps_6h={c['flaps']}, rt_trend={c['rt_trend']}, duration_trend={dur_txt}\n"
        f"down_events_6h={events_txt}\n"
        f"history_baseline: {baseline_text(c['baseline'])}\n"
        f"learned_pattern: {c['learned']}"
    )


def ai_batch_triage(candidates):
    if not candidates:
        return []

    prompt_body = "\n\n".join(build_triage_prompt_block(c) for c in candidates)
    now_txt     = f"{datetime.now():%Y-%m-%d %H:%M} ({WEEKDAYS[datetime.now().weekday()]})"

    prompt = f"""You are a Principal SRE. Below is computed telemetry for monitors that crossed their flap threshold. Evaluate each monitor against its own historical baseline and compare across monitors to prioritize severity.

Current time: {now_txt}

Guidelines:
- If current flapping matches the monitor's typical time-of-day or duration baseline, set matches_history = true.
- If behavior is anomalous compared to baseline (unusual hour, much longer duration, heavy spike in flaps), set matches_history = false and highlight the anomaly in reasoning.
- If learned_pattern is "none learned", do not invent a recurring pattern.

{prompt_body}

Reply ONLY with a valid JSON array (no markdown fences, no extra text):

[
  {{
    "monitor": "<exact monitor name after ###>",
    "flap_alert": true/false,
    "severity": "LOW|MEDIUM|HIGH|CRITICAL",
    "type": "recurring_interval|random_instability|single_long_outage|recovery_cycle",
    "normal": true/false,
    "matches_history": true/false,
    "reasoning": "<max 2 concise English sentences>"
  }}
]"""

    raw = ollama(prompt, temperature=0.2, timeout=120, model=BATCH_TRIAGE_MODEL, json_mode=True)
    decisions = extract_json(raw)
    if isinstance(decisions, dict):
        decisions = [decisions]
    if not isinstance(decisions, list):
        print(f"[{datetime.now()}] Batch triage reply wasn't a JSON array — falling back to per-monitor calls")
        return None
    return [d for d in decisions if isinstance(d, dict)]


def apply_severity_hard_veto(severity, down_events, flaps):
    severity = str(severity or "MEDIUM").upper()
    has_completed_event = any(not e.get("ongoing") for e in down_events)
    heavy_flapping = flaps >= FLAP_THRESHOLD * SEVERITY_EVIDENCE_FLAP_MULTIPLIER
    if severity in ("HIGH", "CRITICAL") and not has_completed_event and not heavy_flapping:
        return "MEDIUM"
    return severity


# ─── AI: SHARED INFRA, DAILY DIGEST & WEEKLY REPORT ──────────────────────────

def ai_infra_group_issue(issue):
    prompt = f"""Summarize this confirmed shared-infrastructure / co-location outage in ONE concise English sentence for an SRE alert channel.

Infrastructure Group: {issue['group_name']}
Affected Monitors ({issue['count']} / {issue['total_monitored']}): {', '.join(issue['monitors'])}
Consecutive Check Windows Down: {issue['consecutive_windows']} (checked every {INFRA_BUCKET_MIN}m)
Outage Started At: {issue['since']}

Write ONLY 1 sentence indicating a likely shared network, hosting, or upstream provider issue for {issue['group_name']} without guessing unverified root causes."""

    return ollama(prompt, temperature=0.2)


def ai_daily_digest(stats, overlaps, rt_trends, upcoming_text):
    problem_monitors = {n: s for n, s in stats.items() if s["down_events"] > 0 or s["flaps"] > 1}
    healthy_count = len(stats) - len(problem_monitors)

    monitor_block = "\n".join(
        f"- {n}: {s['down_events']} down samples, {s['flaps']} flaps, "
        f"avg RT {s['avg_rt']:.0f}ms, RT trend: {rt_trends.get(n, 'N/A')}"
        for n, s in sorted(problem_monitors.items(), key=lambda x: -x[1]["down_events"])
    ) if problem_monitors else "No monitor issues in the last 24h"

    if overlaps:
        ov_lines = "\n".join(
            f"- {o['time']}: {o['count']} monitors down simultaneously — {', '.join(o['monitors'])}"
            for o in overlaps[:10]
        )
        overlap_instruction = f"{ov_lines}\nThese are verified simultaneous outage buckets."
    else:
        overlap_instruction = "No simultaneous multi-monitor outages detected."

    upcoming_block = upcoming_text or "No verified forecast windows in the next 24h."

    prompt = f"""You are an SRE reliability analyst. Write a concise 3-5 sentence plain-paragraph executive summary of the last 24 hours based strictly on the telemetry below.

Date: {datetime.now().strftime('%Y-%m-%d')}
Total Monitors: {len(stats)}, Healthy: {healthy_count}

Monitors with Incidents:
{monitor_block}

Simultaneous Outage Overlaps:
{overlap_instruction}

Upcoming 24h Verified AI Forecasts:
{upcoming_block}

Rules: Cite only the provided numbers. Do not invent root causes or new forecasts. Keep it to 3-5 clear English sentences in a single paragraph."""

    return ollama(prompt, temperature=0.3)


def ai_weekly_report(comp_block, dur_block, corr_block, lead_block, forecast_block):
    prompt = f"""You are an SRE reliability analyst. Write a 4-6 sentence plain-paragraph weekly reliability summary based strictly on the computed telemetry below.

Week-over-Week Comparison (This Week vs Last Week):
{comp_block}

Outage Duration Trends:
{dur_block}

Directional Monitor Correlations (Leader -> Follower):
{corr_block}

Lead-Time Indicators (Pre-Outage Latency Spikes):
{lead_block}

AI Forecast Accuracy & Learned Patterns:
{forecast_block}

Rules:
- Lead with the most critical operational finding.
- State forecast accuracy honestly, noting which patterns held and which drifted.
- Do not speculate on sections with no data.
- Output 4-6 concise English sentences in a single plain paragraph."""

    return ollama(prompt, temperature=0.3)


# ─── DISCORD EMBEDS ──────────────────────────────────────────────────────────

def embed_size(e):
    n = len(e.get("title", "")) + len(e.get("description", "")) + len(e.get("footer", {}).get("text", ""))
    for f in e.get("fields", []):
        n += len(f["name"]) + len(f["value"])
    return n


def send_discord(embeds):
    if DRY_RUN:
        for e in embeds:
            print(f"[{datetime.now()}] [dry-run] would send: {e.get('title')}")
        return
    group, size = [], 0
    for e in embeds:
        s = embed_size(e)
        if group and (len(group) >= 10 or size + s > 5500):
            r = requests.post(DISCORD_WEBHOOK, json={"embeds": group}, timeout=10)
            r.raise_for_status()
            group, size = [], 0
        group.append(e)
        size += s
    if group:
        r = requests.post(DISCORD_WEBHOOK, json={"embeds": group}, timeout=10)
        r.raise_for_status()


def severity_color(severity):
    s = severity.upper()
    if "CRITICAL" in s: return 0xFF0000, "🔴"
    if "HIGH"     in s: return 0xFF6600, "🟠"
    if "MEDIUM"   in s: return 0xFFCC00, "🟡"
    return 0x2ECC71, "🟢"


def flap_embed(name, flap_count, down_events, baseline, learned, current_status, ai, dur_trend):
    color, emoji = severity_color(ai.get("severity", "MEDIUM"))
    status_txt   = "🟢 UP" if current_status == 1 else "🔴 DOWN" if current_status == 0 else "🟡 PENDING"

    events_str = "\n".join(
        f"{e['start']}->{e['end']} ({e['duration_m']}m)" + (f" pre-RT:{e['pre_rt_ms']}ms" if e['pre_rt_ms'] else "")
        for e in down_events[-6:]
    ) if down_events else "No completed down events"

    if baseline:
        hours = " ".join(f"{h:02d}h({c})" for h, c in baseline["top_hours"])
        hist_str = (f"History: {baseline['n']} outages / {baseline['span_days']:.0f}d ({baseline['per_day']}/day)\n"
                    f"Workdays ({WORKDAYS_NAME}): {baseline['workday_pct']}%\n"
                    f"Typical Duration: ~{baseline['dur_median']:.0f}m (p90 {baseline['dur_p90']:.0f}m)\n"
                    f"Top Start Hours: {hours}\n"
                    f"Learned Pattern: {learned}")
    else:
        hist_str = f"Insufficient history\nLearned Pattern: {learned}"

    dur_str = (f"{dur_trend['trend']} — older {dur_trend['older_avg']}m -> recent {dur_trend['recent_avg']}m"
               if dur_trend else "Insufficient history")

    is_norm = str(ai.get("normal", "")).lower() in ("yes", "true")
    normal_tag = "✅ Normal Recovery" if is_norm else "⚠️ Active Instability"

    return {
        "title": f"{emoji} Flapping — {name}  [{ai.get('severity','?')}]",
        "color": color,
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [
            {"name": "Status",           "value": status_txt,       "inline": True},
            {"name": "Flaps / 6h",       "value": str(flap_count),  "inline": True},
            {"name": "AI Verdict",       "value": normal_tag,       "inline": True},
            {"name": "Pattern Type",     "value": ai.get("type", "—"), "inline": True},
            {"name": "Matches Baseline", "value": str(ai.get("matches_history", "—")), "inline": True},
            {"name": "Duration Trend",   "value": dur_str,          "inline": True},
            {"name": "Recent Down Events", "value": f"```{events_str}```", "inline": False},
            {"name": "Historical Baseline", "value": f"```{hist_str[:900]}```", "inline": False},
            {"name": "🤖 AI Triage Analysis", "value": ai.get("analysis", "—")[:1000], "inline": False},
        ],
        "footer": {"text": f"Kuma Foresight • {datetime.now().strftime('%Y-%m-%d %H:%M')}"}
    }


def backtest_text(bt, rule):
    if not bt:
        return "—"
    p = round(bt["p"] * 100)
    if bt["kind"] == "time_of_day":
        return (f"Outage occurred in {bt['hits']} of {bt['windows']} comparable daily windows ({p}%); "
                f"{bt['lift']}× above random Poisson chance")
    if bt["kind"] == "workday":
        return (f"Outage occurred in {bt['hits']} of {bt['windows']} {WORKDAYS_NAME} workday windows ({p}%); "
                f"{bt['lift']}× above random Poisson chance")
    if bt["kind"] == "weekly":
        return (f"Outage occurred in {bt['hits']} of {bt['windows']} comparable weekly windows ({p}%); "
                f"{bt['lift']}× above random Poisson chance")
    return (f"Outage recurred in {bt['hits']} of {bt['windows']} intervals within ~{rule.get('interval_hours'):g}h "
            f"±{rule.get('window_minutes')}m ({p}%); {bt['lift']}× above random chance")


def forecast_embed(name, f, hits, n, hybrid_rt_reason=None):
    ws, we = f["window"]
    when   = f"{fmt_day(ws)} – {fmt_dt(we, '%H:%M')}"
    prob   = f"{f['prob_pct']}%"
    if f.get("ai_prob_pct") is not None and round(f["ai_prob_pct"]) != f["prob_pct"]:
        prob += f" (AI claimed {round(f['ai_prob_pct'])}%, capped by back-test)"
    dur   = f"~{f['expected_duration_min']:.0f} min" if f.get("expected_duration_min") else "—"
    track = f"{hits}/{n} hits (last {n})" if n else "New — awaiting first window verification"

    title_prefix = "⚡ Hybrid AI + Live RT Forecast" if hybrid_rt_reason else "🔮 AI Outage Forecast"
    fields = [
        {"name": "Expected Outage Window (Server Time)", "value": when, "inline": False},
        {"name": "Probability", "value": prob, "inline": True},
        {"name": "Expected Duration", "value": dur, "inline": True},
        {"name": "Learned Rule", "value": rule_text(f["rule"]), "inline": True},
    ]
    if hybrid_rt_reason:
        fields.append({
            "name": "📈 Live RT Precursor (Watch -> Alert Promotion)",
            "value": f"Live latency/jitter is spiking as forecast window opens: {hybrid_rt_reason}",
            "inline": False,
        })
    fields.extend([
        {"name": "Historical Back-Test", "value": backtest_text(f["backtest"], f["rule"])[:1000], "inline": False},
        {"name": "Forecast Track Record", "value": track, "inline": True},
        {"name": "Training Sample", "value": f"{f['n_episodes']} historical episodes", "inline": True},
        {"name": "🤖 AI Reasoning", "value": (f.get("reasoning") or f.get("reasoning_bn") or "—")[:1000], "inline": False},
    ])
    return {
        "title": f"{title_prefix} — {name}",
        "color": 0xE67E22 if hybrid_rt_reason else 0x9B59B6,
        "timestamp": datetime.utcnow().isoformat(),
        "fields": fields,
        "footer": {"text": "Neuro-Symbolic Forecast: AI-proposed rule verified against empirical history by Python"}
    }


def early_warning_embed(name, lead_time, rt_reason, trend_str):
    return {
        "title": f"🟡 Pre-Outage Early Warning — {name}",
        "color": 0xF1C40F,
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [
            {"name": "Trigger (Last 30m)", "value": f"Abnormal response time / jitter degradation: {rt_reason}", "inline": False},
            {"name": "Historical Lead-Time Reliability",
             "value": f"{lead_time['pct']}% of past outages on this monitor were preceded by rising RT (n={lead_time['n']} events)",
             "inline": False},
            {"name": "6h Response Time Trend", "value": trend_str, "inline": False},
        ],
        "footer": {"text": "Predictive Early Warning — gated by proven historical lead-time correlation"}
    }


def cascading_alert_embed(leader, follower, corr, leader_drop_ts):
    return {
        "title": f"⛓️ Cascading Outage Risk — {follower}",
        "color": 0xE74C3C,
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [
            {"name": "Upstream Trigger (Leader Down)", "value": f"**{leader}** dropped at {fmt_dt(leader_drop_ts, '%H:%M')}", "inline": False},
            {"name": "Historical Dependency",
             "value": f"When {leader} goes down, **{follower}** follows within {CORRELATION_WINDOW_MIN}m in **{corr['lead_pct']}%** of cases (n={corr['n']})",
             "inline": False},
            {"name": "Expected Lag", "value": f"~{corr['avg_lag_m']} minutes", "inline": True},
        ],
        "footer": {"text": "Directional Leader -> Follower correlation alert"}
    }


def infra_group_issue_embed(issue, ai_comment):
    monitors_str = "\n".join(f"• {m}" for m in issue["monitors"])
    return {
        "title": f"🔴 Shared Infrastructure Outage — {issue['group_name']}",
        "color": 0xB91C1C,
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [
            {"name": "Simultaneously Down Monitors", "value": f"```{monitors_str}```", "inline": False},
            {"name": "Group Coverage",
             "value": f"{issue['count']} / {issue['total_monitored']} monitors down", "inline": True},
            {"name": "Persistence",
             "value": f"{issue['consecutive_windows']} consecutive windows ({INFRA_BUCKET_MIN}m each)", "inline": True},
            {"name": "Outage Started", "value": issue["since"], "inline": True},
            {"name": "🤖 AI Summary", "value": ai_comment[:1000], "inline": False},
        ],
        "footer": {"text": "Individual monitor alerts suppressed in favor of consolidated infrastructure group alert"}
    }


def digest_embed(ai_summary, stats, overlaps, upcoming_text):
    monitor_lines = [
        f"{name}: {s['down_events']} down, {s['flaps']} flaps, avg {s['avg_rt']:.0f}ms"
        for name, s in sorted(stats.items(), key=lambda x: -x[1]["down_events"])
        if s["down_events"] > 0 or s["flaps"] > 0
    ]
    raw_stats = "\n".join(monitor_lines) if monitor_lines else "All monitors healthy (0 down events)"
    fields = [{"name": "Last 24 Hours Telemetry", "value": f"```{raw_stats[:900]}```", "inline": False}]
    if overlaps:
        ov_lines = "\n".join(f"{o['time']} — {o['count']} monitors: {', '.join(o['monitors'])}" for o in overlaps[:8])
        fields.append({"name": "🔗 Verified Simultaneous Outages", "value": f"```{ov_lines[:900]}```", "inline": False})
    if upcoming_text:
        fields.append({"name": "🔮 Upcoming 24h Forecast Windows", "value": f"```{upcoming_text[:700]}```", "inline": False})
    fields.append({"name": "🤖 AI Executive Summary", "value": ai_summary[:1000], "inline": False})
    return {"title": "📊 Daily SRE Monitoring Digest", "color": 0x3498DB,
            "timestamp": datetime.utcnow().isoformat(), "fields": fields,
            "footer": {"text": f"Kuma Foresight • {datetime.now().strftime('%Y-%m-%d %H:%M')}"}}


def weekly_embed(comp_block, dur_block, corr_block, lead_block, forecast_block, ai_summary):
    return {
        "title": "📅 Weekly SRE Reliability & Forecast Report",
        "color": 0x1ABC9C,
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [
            {"name": "Week-over-Week Comparison", "value": f"```{comp_block[:900]}```", "inline": False},
            {"name": "Outage Duration Trends", "value": f"```{dur_block[:900]}```", "inline": False},
            {"name": "Directional Correlations (Leader -> Follower)", "value": f"```{corr_block[:900]}```", "inline": False},
            {"name": "Lead-Time Precursor Reliability", "value": f"```{lead_block[:900]}```", "inline": False},
            {"name": "🔮 AI Forecast Accuracy Audit", "value": f"```{forecast_block[:700]}```", "inline": False},
            {"name": "🤖 AI Executive Summary", "value": ai_summary[:1000], "inline": False},
        ],
        "footer": {"text": f"Kuma Foresight Weekly • {datetime.now().strftime('%Y-%m-%d')}"}
    }


# ─── DAILY & WEEKLY RUNNERS ──────────────────────────────────────────────────

def run_daily_digest(forecasts):
    print(f"[{datetime.now()}] Running daily digest...")
    rt_results = prom_range(f'monitor_response_time{{job="{PROM_JOB_NAME}"}}', 24, step=300)
    st_results = prom_range(f'monitor_status{{job="{PROM_JOB_NAME}"}}',        24, step=300)

    rt_by_name = {r["metric"].get("monitor_name"): r["values"] for r in rt_results}
    st_by_name = {r["metric"].get("monitor_name"): r["values"] for r in st_results}

    stats, rt_trends = {}, {}
    for name in rt_by_name:
        rt_vals = [float(v[1]) for v in rt_by_name[name] if float(v[1]) > 0]
        st_vals = [float(v[1]) for v in st_by_name.get(name, [])]
        stats[name] = {
            "avg_rt": sum(rt_vals) / len(rt_vals) if rt_vals else 0,
            "max_rt": max(rt_vals) if rt_vals else 0,
            "down_events": sum(1 for v in st_vals if v == 0.0),
            "flaps": count_flaps(st_vals),
        }
        rt_trends[name] = rt_trend(rt_by_name[name])

    overlaps      = detect_overlaps(st_by_name)
    upcoming_text = upcoming_forecasts_text(forecasts, time.time())

    try:
        ai_summary = ai_daily_digest(stats, overlaps, rt_trends, upcoming_text)
    except Exception as e:
        ai_summary = f"AI summary unavailable: {e}"

    try:
        send_discord([digest_embed(ai_summary, stats, overlaps, upcoming_text)])
        print(f"[{datetime.now()}] Digest sent ({len(overlaps)} overlaps)")
    except Exception as e:
        print(f"[{datetime.now()}] Digest Discord error: {e}")


def run_weekly_report(events_store, forecasts):
    print(f"[{datetime.now()}] Running weekly report...")
    now = time.time()

    this_st = prom_range_window(f'monitor_status{{job="{PROM_JOB_NAME}"}}', now - 7*86400, now, step=300)
    last_st = prom_range_window(f'monitor_status{{job="{PROM_JOB_NAME}"}}', now - 14*86400, now - 7*86400, step=300)

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
        f"- {n}: this week {c['this_down']} down/{c['this_flap']} flaps | "
        f"last week {c['last_down']} down/{c['last_flap']} flaps"
        for n, c in sorted(comparison.items(), key=lambda x: -x[1]['this_down'])
    ) if comparison else "No significant week-over-week changes"

    dur_block = "\n".join(
        f"- {n}: {d['trend']} — older avg {d['older_avg']}m -> recent avg {d['recent_avg']}m (n={d['n']})"
        for n, d in duration_trends.items()
    ) if duration_trends else "Insufficient history"

    corr_block = "\n".join(
        f"- {c['a']} -> {c['b']}: co-occurs {c['pct']}%, Leader->Follower {c['lead_pct']}% (avg lag ~{c['avg_lag_m']}m, n={c['n']})"
        for c in correlations
    ) if correlations else "No strong directional correlations found"

    lead_block = "\n".join(
        f"- {n}: {v['pct']}% of outages preceded by RT/Jitter spike (n={v['n']}) — reliable early-warning indicator"
        for n, v in lead_times.items()
    ) if lead_times else "No reliable leading indicators verified yet"

    forecast_block = forecast_report_block(forecasts, now)

    try:
        ai_summary = ai_weekly_report(comp_block, dur_block, corr_block, lead_block, forecast_block)
    except Exception as e:
        ai_summary = f"AI summary unavailable: {e}"

    try:
        send_discord([weekly_embed(comp_block, dur_block, corr_block, lead_block, forecast_block, ai_summary)])
        print(f"[{datetime.now()}] Weekly report sent")
    except Exception as e:
        print(f"[{datetime.now()}] Weekly report Discord error: {e}")


# ─── TRIAGE FALLBACK ─────────────────────────────────────────────────────────

def triage_fallback(cands, state, now, embeds_to_send):
    for c in cands:
        name, flaps = c["name"], c["flaps"]
        down_events, dur_trend = c["down_events"], c["dur_trend"]
        cooldown_key = f"flap:{name}"

        last_alert = state["cooldowns"].get(cooldown_key, 0)
        if now - last_alert < ALERT_COOLDOWN:
            continue
        try:
            ai = ai_analyze_flap(name, flaps, down_events, c["rt_trend"], c["baseline"], c["learned"], dur_trend)
        except Exception as e:
            ai = {"severity": "MEDIUM", "type": "unknown", "normal": "no", "matches_history": "—",
                  "analysis": f"AI unavailable: {e}"}
        ai["severity"] = apply_severity_hard_veto(ai.get("severity"), down_events, flaps)
        if str(ai.get("normal")).lower() in ("yes", "true") and "LOW" in ai.get("severity", ""):
            print(f"[{datetime.now()}] Skipped {name} — AI: normal recovery, LOW severity")
        else:
            embeds_to_send.append(flap_embed(name, flaps, down_events, c["baseline"], c["learned"],
                                             c["current_status"], ai, dur_trend))
            state["cooldowns"][cooldown_key] = now
            print(f"[{datetime.now()}] Flap alert: {name} ({flaps} flaps, {ai['severity']})")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print(f"[{datetime.now()}] Run started" + (" (DRY RUN — nothing is sent or saved)" if DRY_RUN else ""))
    lock = acquire_lock()
    if lock is None:
        print(f"[{datetime.now()}] Previous run is still active — exiting")
        return

    state        = load_state()
    history      = load_history()
    events_store = load_events()
    forecasts    = load_forecasts()
    now          = time.time()

    today_str    = datetime.now().strftime("%Y-%m-%d")
    current_hour = datetime.now().hour
    weekday      = datetime.now().weekday()

    if current_hour == DIGEST_HOUR and state.get("last_digest_date") != today_str:
        run_daily_digest(forecasts)
        state["last_digest_date"] = today_str
        save_json(STATE_FILE, state)

    if (weekday == WEEKLY_DIGEST_DAY and current_hour == WEEKLY_DIGEST_HOUR
            and state.get("last_weekly_date") != today_str):
        run_weekly_report(events_store, forecasts)
        state["last_weekly_date"] = today_str
        save_json(STATE_FILE, state)

    st_results = prom_range(f'monitor_status{{job="{PROM_JOB_NAME}"}}',        LOOKBACK_HOURS)
    rt_results = prom_range(f'monitor_response_time{{job="{PROM_JOB_NAME}"}}', LOOKBACK_HOURS)

    st_by_name = {r["metric"].get("monitor_name"): r["values"] for r in st_results}
    rt_by_name = {r["metric"].get("monitor_name"): r["values"] for r in rt_results}

    for name, vals in st_by_name.items():
        for ts_str, val in vals:
            if float(val) == 0.0:
                record_downtime(history, name, float(ts_str))
    save_json(HISTORY_FILE, history)

    global_start   = recording_start(history, events_store, now)
    embeds_to_send = []

    # Evaluate all configured and auto-discovered co-location / infrastructure groups
    effective_groups = build_effective_infra_groups(st_results, INFRA_GROUPS, AUTO_GROUP_BY_LABEL)
    infra_issues     = detect_infra_group_issues(st_by_name, effective_groups)
    infra_affected_names = set()

    for issue in infra_issues:
        infra_affected_names.update(issue["monitors"])
        grp_key = f"infra_group:{issue['group_name']}"
        last_alert = state["cooldowns"].get(grp_key, 0)
        if now - last_alert >= ALERT_COOLDOWN:
            try:
                ai_comment = ai_infra_group_issue(issue)
            except Exception as e:
                ai_comment = f"AI summary unavailable: {e}"
            embeds_to_send.append(infra_group_issue_embed(issue, ai_comment))
            state["cooldowns"][grp_key] = now
            print(f"[{datetime.now()}] Shared Infra Issue [{issue['group_name']}]: {issue['monitors']} "
                  f"(persisted {issue['consecutive_windows']} windows)")

    candidates       = []
    live             = {}
    episodes_by_name = {}
    mon_starts       = {}

    for name, st_raw in st_by_name.items():
        st_vals        = [float(v[1]) for v in st_raw]
        flaps          = count_flaps(st_vals)
        current_status = st_vals[-1] if st_vals else 1.0
        rt_raw         = rt_by_name.get(name, [])
        rt_str         = rt_trend(rt_raw)
        rising, diff, rt_reason = short_term_rising(rt_raw)

        down_events = extract_down_events(st_raw, rt_raw)
        for e in down_events:
            if not e.get("ongoing") and not e.get("left_truncated"):
                record_completed_event(events_store, name, e)

        m_start  = monitor_data_start(name, state, history, events_store, global_start, now)
        episodes = build_episodes(history, events_store, name)
        mon_starts[name]       = m_start
        episodes_by_name[name] = episodes
        live[name] = {
            "status": current_status,
            "flaps": flaps,
            "rt_trend": rt_str,
            "rt_rising": rising,
            "rt_delta": diff,
            "rt_reason": rt_reason,
        }

        dur_trend = duration_trend(name, events_store)
        warn_key  = f"earlywarn:{name}"

        # Suppress individual alerts for monitors already covered by a Shared Infra Group alert
        if name in infra_affected_names:
            continue

        # ── SIGNAL 1: Early Warning (Relative %, Absolute ms, or RT Jitter) ──
        lt = lead_time_reliability(name, events_store)
        if lt and lt["reliable"] and rising and current_status != 0.0:
            last_alert = state["cooldowns"].get(warn_key, 0)
            if now - last_alert >= ALERT_COOLDOWN:
                embeds_to_send.append(early_warning_embed(name, lt, rt_reason, rt_str))
                state["cooldowns"][warn_key] = now
                print(f"[{datetime.now()}] Early warning: {name} ({rt_reason}, lead-time {lt['pct']}%)")

        if flaps >= FLAP_THRESHOLD:
            candidates.append(gather_triage_candidates(
                name, flaps, current_status,
                history_baseline(episodes, m_start, now),
                learned_pattern_text(forecasts, name),
                down_events, rt_str, dur_trend
            ))

    # ── SIGNAL 2: Live Directional Cascading Risk (Leader -> Follower) ──
    cascade_alerts = detect_cascading_risks(
        events_store, live, episodes_by_name, infra_affected_names, state, now
    )
    for cd_key, emb in cascade_alerts:
        embeds_to_send.append(emb)
        state["cooldowns"][cd_key] = now
        print(f"[{datetime.now()}] Cascading risk alert fired: {cd_key}")

    # ── SIGNAL 3: Batched AI Flap Triage ──
    if candidates:
        try:
            decisions = ai_batch_triage(candidates)
        except Exception as e:
            print(f"[{datetime.now()}] Batch triage call failed ({e}) — falling back to per-monitor calls")
            decisions = None

        if decisions is None:
            triage_fallback(candidates, state, now, embeds_to_send)
        else:
            decisions_by_name = {norm_name(d.get("monitor")): d for d in decisions}
            missing = []

            for c in candidates:
                name = c["name"]
                d = decisions_by_name.get(norm_name(name))
                if not d:
                    missing.append(c)
                    continue

                cooldown_key = f"flap:{name}"
                if d.get("flap_alert") is True:
                    last_alert = state["cooldowns"].get(cooldown_key, 0)
                    if now - last_alert >= ALERT_COOLDOWN:
                        severity = apply_severity_hard_veto(d.get("severity"), c["down_events"], c["flaps"])
                        is_normal = d.get("normal") is True
                        if is_normal and severity == "MEDIUM" and str(d.get("severity", "")).upper() == "LOW":
                            severity = "LOW"
                        if is_normal and severity == "LOW":
                            print(f"[{datetime.now()}] Skipped {name} — AI: normal recovery, LOW severity")
                        else:
                            mh = d.get("matches_history")
                            ai = {
                                "severity": severity,
                                "type": d.get("type", "unknown"),
                                "normal": "yes" if is_normal else "no",
                                "matches_history": "yes" if mh is True else "no" if mh is False else "—",
                                "analysis": d.get("reasoning", "—"),
                            }
                            embeds_to_send.append(
                                flap_embed(name, c["flaps"], c["down_events"], c["baseline"], c["learned"],
                                           c["current_status"], ai, c["dur_trend"])
                            )
                            state["cooldowns"][cooldown_key] = now
                            print(f"[{datetime.now()}] Flap alert: {name} ({c['flaps']} flaps, {severity}) [batch]")

            if missing:
                print(f"[{datetime.now()}] Batch triage skipped {len(missing)} monitor(s) — using per-monitor fallback")
                triage_fallback(missing, state, now, embeds_to_send)

    # ── SIGNAL 4: Upcoming Forecast & Hybrid Watch + Live RT Alerts ──
    fc_alerts = collect_forecast_alerts(forecasts, live, now)
    embeds_to_send.extend(emb for _, emb in fc_alerts)

    if embeds_to_send:
        send_discord(embeds_to_send)
        print(f"[{datetime.now()}] Sent {len(embeds_to_send)} alert(s)")
    else:
        print(f"[{datetime.now()}] No alerts")
    for name, _ in fc_alerts:
        forecasts["current"][name]["alerted_at"] = now

    save_json(EVENTS_FILE, events_store)
    save_json(STATE_FILE, state)
    save_json(FORECASTS_FILE, forecasts)

    # ── LEARNING LOOP — score past forecasts, re-learn from history, predict ──
    try:
        score_forecasts(forecasts, episodes_by_name, now)
        refresh_forecasts(forecasts, episodes_by_name, live, mon_starts, now)
        new_alerts = collect_forecast_alerts(forecasts, live, now)
        if new_alerts:
            send_discord([emb for _, emb in new_alerts])
            for name, _ in new_alerts:
                forecasts["current"][name]["alerted_at"] = now
            print(f"[{datetime.now()}] Sent {len(new_alerts)} forecast alert(s)")
    except Exception as e:
        print(f"[{datetime.now()}] Learning loop error: {e}")
    finally:
        save_json(FORECASTS_FILE, forecasts)

    print(f"[{datetime.now()}] Done")


if __name__ == "__main__":
    main()