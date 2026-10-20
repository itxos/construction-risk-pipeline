import argparse
import os
import sys
import time

import psycopg2
import requests
from psycopg2.extras import Json, execute_values

BASE = "https://apiprod.dol.gov/v4/get/osha"
KEY = os.environ["DOL_API_KEY"]
BACKOFF = [30, 60, 120, 240, 480]  # seconds
PACE = 6.0


class RateLimited(Exception):
    pass


def log(msg):
    print(msg, flush=True)


def get(dataset, params):
    """GET one page. Returns a list of records ([] for HTTP 204 = no rows)."""
    query = {"X-API-KEY": KEY, **params}
    for wait in BACKOFF:
        time.sleep(PACE)
        r = requests.get(f"{BASE}/{dataset}/json", params=query, timeout=60)
        if r.status_code == 204:
            return []
        if r.status_code == 200:
            return r.json().get("data", [])
        if r.status_code in (429, 500, 502, 503, 504):
            retry_after = r.headers.get("Retry-After", "")
            delay = int(retry_after) if retry_after.isdigit() else wait
            log(f"  HTTP {r.status_code} on {dataset}, waiting {delay}s")
            time.sleep(delay)
            continue
        # Never use raise_for_status(): its message would include the URL, which has the key.
        raise RuntimeError(f"{dataset}: HTTP {r.status_code}")
    raise RateLimited(dataset)


def fetch_violations(ids):
    """All violations for a batch of activity numbers, paging until exhausted."""
    flt = '{"field":"activity_nr","operator":"in","value":[%s]}' % ",".join(str(i) for i in ids)
    rows, offset, limit = [], 0, 500
    while True:
        page = get("violation", {"filter_object": flt, "limit": limit,
                                 "offset": offset, "sort_by": "activity_nr"})
        rows.extend(page)
        if len(page) < limit:
            return rows
        offset += limit


def main():
    global PACE
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2024-01-01")
    ap.add_argument("--page-size", type=int, default=200)
    ap.add_argument("--max-pages", type=int, default=0, help="0 = no limit")
    ap.add_argument("--sleep", type=float, default=6.0, help="seconds between API calls")
    ap.add_argument("--start-after", type=int, default=None,
                    help="activity_nr to resume after (default: highest already loaded)")
    ap.add_argument("--viol-batch", type=int, default=50,
                    help="inspections per violation request (try 200)")
    args = ap.parse_args()
    PACE = args.sleep

    conn = psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5433"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )

    if args.start_after is not None:
        last_id = args.start_after
    else:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT coalesce(max(activity_nr), 0) FROM raw.osha_inspections")
            last_id = cur.fetchone()[0]
    log(f"starting after activity_nr {last_id}")

    page_no, total_i, total_v = 0, 0, 0
    try:
        while True:
            insp_filter = (
                '{"and":[{"field":"site_state","operator":"eq","value":"TX"},'
                '{"field":"open_date","operator":"gt","value":"%s"},'
                '{"field":"naics_code","operator":"like","value":"23%%"},'
                '{"field":"activity_nr","operator":"gt","value":%d}]}' % (args.since, last_id)
            )
            inspections = get("inspection", {"filter_object": insp_filter,
                                             "limit": args.page_size, "sort_by": "activity_nr"})
            if not inspections:
                break
            ids = [i["activity_nr"] for i in inspections]
            # Safety check: keyset paging only works if results really are sorted ascending.
            if ids != sorted(ids) or ids[0] <= last_id:
                raise RuntimeError("results are not sorted by activity_nr as expected; stopping")
            page_no += 1

            violations = []
            for start in range(0, len(ids), args.viol_batch):
                violations.extend(fetch_violations(ids[start:start + args.viol_batch]))

            insp_rows = {i["activity_nr"]: (i["activity_nr"], Json(i)) for i in inspections}
            viol_rows = {(v["activity_nr"], str(v["citation_id"])):
                         (v["activity_nr"], str(v["citation_id"]), Json(v)) for v in violations}

            # One transaction per page: inspections and their violations land together or not at all.
            with conn, conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO raw.osha_inspections (activity_nr, payload) VALUES %s
                    ON CONFLICT (activity_nr)
                    DO UPDATE SET payload = EXCLUDED.payload, ingested_at = now()""",
                    list(insp_rows.values()))
                if viol_rows:
                    execute_values(cur, """
                        INSERT INTO raw.osha_violations (activity_nr, citation_id, payload) VALUES %s
                        ON CONFLICT (activity_nr, citation_id)
                        DO UPDATE SET payload = EXCLUDED.payload, ingested_at = now()""",
                        list(viol_rows.values()))

            last_id = ids[-1]
            total_i += len(insp_rows)
            total_v += len(viol_rows)
            log(f"page {page_no}: {len(insp_rows)} inspections, {len(viol_rows)} violations "
                f"(this run: {total_i} / {total_v}), last activity_nr {last_id}")

            if len(inspections) < args.page_size or (args.max_pages and page_no >= args.max_pages):
                break
    except RateLimited:
        log("Rate limited after all retries. Progress is saved; re-run later to resume.")
        sys.exit(2)

    log("done")


if __name__ == "__main__":
    main()
