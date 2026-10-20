"""Collect the APM spans of one app from Elasticsearch and compute per-trace end-to-end latency."""
import argparse
import base64
import csv
import json
import os
import urllib.request
from collections import defaultdict

ES_URL = "http://localhost:9200"
ES_AUTH = "elastic:elastic"
FIELDS = ["trace.id", "span.id", "span.name", "span.duration.us",
          "timestamp.us", "service.name", "labels"]


def es_search(body):
    """Send one search request to Elasticsearch and return the parsed response."""
    req = urllib.request.Request(
        f"{ES_URL}/traces-apm-default/_search",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json",
                 "Authorization": "Basic " + base64.b64encode(ES_AUTH.encode()).decode()},
    )
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)


def fetch_spans(app_id, start, end):
    """Fetch all span documents of the app in the time range, page by page."""
    body = {
        "size": 5000,
        "_source": FIELDS,
        "query": {"bool": {"filter": [
            {"term": {"labels.app_name": app_id}},
            {"term": {"processor.event": "span"}},  # same filter as Egon: spans only
            {"range": {"@timestamp": {"gte": start, "lte": end}}},
        ]}},
        "sort": [{"timestamp.us": "asc"}, {"span.id": "asc"}],
    }
    spans = []
    while True:
        hits = es_search(body)["hits"]["hits"]
        if not hits:
            return spans
        spans.extend(h["_source"] for h in hits)
        body["search_after"] = hits[-1]["sort"]


def build_traces(spans):
    """Group spans by trace; end-to-end latency = last span end - first span start."""
    by_trace = defaultdict(list)
    for s in spans:
        by_trace[s["trace"]["id"]].append(s)
    rows = []
    for trace_id, items in by_trace.items():
        start = min(s["timestamp"]["us"] for s in items)
        end = max(s["timestamp"]["us"] + s["span"]["duration"]["us"] for s in items)
        labels = [s.get("labels", {}) for s in items]
        row = {
            "trace_id": trace_id,
            "start_us": start,
            "end_us": end,
            "e2e_ms": round((end - start) / 1000, 3),
            "deployments": "|".join(sorted({s["service"]["name"] for s in items})),
            "n_spans": len(items),
            "has_start": any(l.get("trace_boundary_start") == "true" for l in labels),
            "has_end": any(l.get("trace_boundary_end") == "true" for l in labels),
        }
        # Per-part durations; they can overlap (forward_request waits for the next function)
        for s in items:
            key = s["span"]["name"] + "_ms"
            row[key] = round(row.get(key, 0) + s["span"]["duration"]["us"] / 1000, 3)
        rows.append(row)
    return sorted(rows, key=lambda r: r["start_us"])


def write_csv(path, rows):
    """Write a list of dicts to CSV, using the union of all keys as columns."""
    columns = []
    for r in rows:
        columns += [k for k in r if k not in columns]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--app", required=True, help="app id (labels.app_name)")
    parser.add_argument("--start", required=True, help="UTC start, e.g. 2026-10-19T12:05:00Z")
    parser.add_argument("--end", required=True, help="UTC end")
    parser.add_argument("--out", required=True, help="output directory of the run")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    spans = fetch_spans(args.app, args.start, args.end)
    traces = build_traces(spans)
    write_csv(os.path.join(args.out, "traces.csv"), traces)

    complete = [t for t in traces if t["has_start"] and t["has_end"]]
    print(f"spans: {len(spans)}, traces: {len(traces)}, complete: {len(complete)}")


if __name__ == "__main__":
    main()