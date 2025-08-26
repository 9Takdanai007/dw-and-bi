import glob
import json
import os
from typing import List, Iterable, Dict, Any

import psycopg2
from psycopg2.extras import execute_batch


def get_files(filepath: str) -> List[str]:
    """
    List all *.json files under a directory (recursive).
    """
    all_files = []
    for root, _dirs, _files in os.walk(filepath):
        for f in glob.glob(os.path.join(root, "*.json")):
            all_files.append(os.path.abspath(f))
    print(f"{len(all_files)} files found in {filepath}")
    return all_files


def read_json_file(path: str) -> Any:
    """
    Read a JSON file with robust UTF-8 handling (Windows-safe).
    """
    # Most GitHub event dumps are UTF-8; utf-8-sig also handles BOM if present.
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except UnicodeDecodeError:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)


def ensure_schema(cur) -> None:
    """
    Create minimal tables if they do not exist.
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS actors (
            id BIGINT PRIMARY KEY,
            login TEXT NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            actor_id BIGINT NOT NULL REFERENCES actors(id)
        );
        """
    )


def iter_records(data: Any) -> Iterable[Dict[str, Any]]:
    """
    Normalize input to an iterable of event dicts.
    Accept either a list[dict] or a dict with 'items' etc.
    """
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                yield item
    elif isinstance(data, dict):
        # some JSON dumps wrap items
        items = data.get("items") or data.get("events") or []
        for item in items:
            if isinstance(item, dict):
                yield item


def process(cur, conn, filepath: str) -> None:
    """
    Read JSON files and upsert into Postgres.
    Commits once per file for performance.
    """
    all_files = get_files(filepath)

    for datafile in all_files:
        data = read_json_file(datafile)

        # buffers for batch insert
        actor_rows = []
        event_rows = []

        for each in iter_records(data):
            # safe access with defaults
            ev_id = str(each.get("id", ""))
            ev_type = each.get("type", "")
            actor = each.get("actor") or {}
            actor_id = actor.get("id")
            actor_login = actor.get("login", "")

            # print sample (and avoid KeyError if fields missing)
            if ev_type == "IssueCommentEvent":
                payload = each.get("payload") or {}
                issue = payload.get("issue") or {}
                issue_url = issue.get("url", "")
                print(ev_id, ev_type, actor_id, actor_login,
                      (each.get("repo") or {}).get("id"),
                      (each.get("repo") or {}).get("name"),
                      each.get("created_at"), issue_url)
            else:
                print(ev_id, ev_type, actor_id, actor_login,
                      (each.get("repo") or {}).get("id"),
                      (each.get("repo") or {}).get("name"),
                      each.get("created_at"))

            # skip records without essential keys
            if actor_id is None or not ev_id:
                continue

            actor_rows.append((int(actor_id), actor_login))
            event_rows.append((ev_id, ev_type, int(actor_id)))

        # upsert in batches (faster + SQL injection-safe)
        if actor_rows:
            execute_batch(
                cur,
                """
                INSERT INTO actors (id, login)
                VALUES (%s, %s)
                ON CONFLICT (id) DO UPDATE SET login = EXCLUDED.login;
                """,
                actor_rows,
                page_size=1000,
            )

        if event_rows:
            execute_batch(
                cur,
                """
                INSERT INTO events (id, type, actor_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
                """,
                event_rows,
                page_size=1000,
            )

        conn.commit()
        print(f"Committed {len(actor_rows)} actors and {len(event_rows)} events from {os.path.basename(datafile)}")


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    ensure_schema(cur)
    conn.commit()

    process(cur, conn, filepath="../data")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
