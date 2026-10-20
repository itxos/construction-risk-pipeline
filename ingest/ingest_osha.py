import argparse
import os
import sys
import time

import psycopg2
import requests
from psycopg2.extras import Json, execute_values

BASE = "https://apiprod.dol.gov/v4/get/osha"
KEY = os.environ["DOL_API_KEY"]


def get(dataset, params, retries=5):
    """GET one page. Returns a list of records ([] for HTTP 204 = no rows)."""
    query = {"X-API-KEY": KEY, **params}
    for attempt in range(retries):
        r = requests.get(f"{BASE}/{dataset}/json", params=query, timeout=60)
        if r.status_code == 204:
            return []
        if r.status_code == 200:
            return r.json().get("data", [])
        if r.status_code in (429, 500, 502, 503, 504):
            wait = 2 ** attempt * 2
            print(f"  HTTP {r.status_code}, retrying in {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
        # Don't use raise_for_status(): its message would include the URL, and the URL has the key.
        raise RuntimeError(f"{dataset}: HTTP {r.status_code}")
    raise RuntimeError(f"{dataset}: gave up after {retries} attempts")


def fetch_violations(ids, sleep):
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
        time.sleep(sleep)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2024-01-01")
    ap.add_argument("--page-size", type=int, default=200)
    ap.add_argument("--max-pages", type=int, default=0, help="0 = no limit")
    ap.add_argument("--sleep", type=float, default=1.0, help="seconds between API calls")
    args = ap.parse_args()

    conn = psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5433"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )

    insp_filter = (
        '{"and":[{"field":"site_state","operator":"eq","value":"TX"},'
        '{"field":"open_date","operator":"gt","value":"%s"},'
        '{"field":"naics_code","operator":"like","value":"23%%"}]}' % args.since
    )

    offset, page_no, total_i, total_v = 0, 0, 0, 0
    while True:
        inspections = get("inspection", {"filter_object": insp_filter, "limit": args.page_size,
                                         "offset": offset, "sort_by": "activity_nr"})
        if not inspections:
            break
        page_no += 1

        violations = []
        ids = [i["activity_nr"] for i in inspections]
        for start in range(0, len(ids), 50):
            time.sleep(args.sleep)
            violations.extend(fetch_violations(ids[start:start + 50], args.sleep))

        # Dedupe on the primary key so one batch can't hit the same row twice.
        insp_rows = {i["activity_nr"]: (i["activity_nr"], Json(i)) for i in inspections}
        viol_rows = {(v["activity_nr"], str(v["citation_id"])):
                     (v["activity_nr"], str(v["citation_id"]), Json(v)) for v in violations}

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

        total_i += len(insp_rows)
        total_v += len(viol_rows)
        print(f"page {page_no}: {len(insp_rows)} inspections, {len(viol_rows)} violations "
              f"(running total {total_i} / {total_v})")

        if len(inspections) < args.page_size or (args.max_pages and page_no >= args.max_pages):
            break
        offset += args.page_size
        time.sleep(args.sleep)

    print("done")


if __name__ == "__main__":
    main()
