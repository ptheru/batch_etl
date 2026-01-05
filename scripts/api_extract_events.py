import os
import json
import time
import uuid
import argparse
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import requests

def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def request_with_backoff(url: str, headers: Dict[str, str], params: Dict[str, Any], max_retries: int = 6) -> requests.Response:
    backoff = 1.0
    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code in (429, 500, 502, 503, 504):
            # rate limit 
            time.sleep(backoff)
            backoff *= 2
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()
    return resp 

def write_jsonl(path: str, rows) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")

def main(process_date: str):
    """
    Writes:
      data/raw/events/dt=YYYY-MM-DD.jsonl
    """
    api_base = os.getenv("EVENTS_API_BASE_URL", "").strip()
    api_key = os.getenv("EVENTS_API_KEY", "").strip()

    if not api_base:
        raise ValueError("Missing EVENTS_API_BASE_URL env var")
    if not api_key:
        raise ValueError("Missing EVENTS_API_KEY env var")

    out_base = os.getenv("RAW_EVENTS_PATH", "data/raw/events")
    out_path = os.path.join(out_base, f"dt={process_date}", ".jsonl")

    url = f"{api_base.rstrip('/')}/events"
    headers = {"Authorization": f"Bearer {api_key}"}

    request_id = str(uuid.uuid4())
    extracted = []
    page_token: Optional[str] = None

    while True:
        params = {"date": process_date, "limit": 1000}
        if page_token:
            params["page_token"] = page_token

        resp = request_with_backoff(url, headers, params)
        payload = resp.json()

        items = payload.get("items", [])
        next_token = payload.get("next_page_token")

        # Add extraction metadata to every record
        for it in items:
            it["_extract_ts"] = iso_utc_now()
            it["_request_id"] = request_id
            it["_source"] = url
            extracted.append(it)

        if not next_token:
            break
        page_token = next_token

    write_jsonl(out_path, extracted)
    print(f"Wrote {len(extracted)} events to {out_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--process-date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()
    main(args.process_date)
