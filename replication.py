# ============================================================
#  replication.py — Micro-Batch Replication Script
#  Author  : Sudhakar
#  Project : Pagila Cloud Dashboard on Azure
#
#  WHAT THIS DOES:
#  Every 30 seconds:
#    1. Connects to LOCAL PostgreSQL (WSL2) — source
#    2. Finds NEW rentals added since last sync
#    3. Inserts them into AZURE PostgreSQL — target
#    4. Logs the sync result with timestamp
#
#  HOW TO RUN:
#    Open Terminal 3: python replication.py
#
#  HOW TO STOP:
#    Ctrl+C
#
#  TO SIMULATE LAG FOR DEMO:
#    Stop this script (Ctrl+C)
#    Insert new records in local DB (commands below)
#    Check Replication tab — shows LAG ⚠
#    Restart script — lag goes back to 0 ✅
#
#  INSERT TEST DATA COMMAND (run in psql):
#    INSERT INTO rental (rental_date, inventory_id,
#      customer_id, staff_id, last_update)
#    VALUES (NOW(), 1, 1, 1, NOW());
# ============================================================

import os
import time
import datetime
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ── Load .env file ───────────────────────────────────────────
load_dotenv()

# ── How often to sync (seconds) ─────────────────────────────
SYNC_INTERVAL = 30   # change to 10 for faster demo

# ── High watermark — tracks last synced rental_id ───────────
# Same concept as Oracle GoldenGate SCN watermark!
last_synced_rental_id = 0


# ════════════════════════════════════════════════════════════
#  CONNECTION HELPERS
# ════════════════════════════════════════════════════════════
def get_source_conn():
    """Connect to LOCAL PostgreSQL (WSL2) — source/on-prem DB"""
    return psycopg2.connect(
        host            = os.getenv("LOCAL_DB_HOST",     "localhost"),
        port            = os.getenv("LOCAL_DB_PORT",     5432),
        dbname          = os.getenv("LOCAL_DB_NAME",     "pagila"),
        user            = os.getenv("LOCAL_DB_USER",     "dbadmin"),
        password        = os.getenv("LOCAL_DB_PASSWORD", "admin123"),
        sslmode         = "disable",
        connect_timeout = 5
    )


def get_target_conn():
    """Connect to AZURE PostgreSQL — target/cloud DB"""
    return psycopg2.connect(
        host            = os.getenv("DB_HOST"),
        port            = os.getenv("DB_PORT",     5432),
        dbname          = os.getenv("DB_NAME",     "pagila"),
        user            = os.getenv("DB_USER",     "dbadmin"),
        password        = os.getenv("DB_PASSWORD", "Admin###123"),
        sslmode         = os.getenv("DB_SSLMODE",  "require"),
        connect_timeout = 10
    )


# ════════════════════════════════════════════════════════════
#  INITIALIZE — Find starting watermark
#  Read MAX rental_id from Azure so we don't re-sync
#  records that are already there
# ════════════════════════════════════════════════════════════
def get_last_synced_id():
    """
    Reads MAX rental_id from Azure PostgreSQL.
    This is our HIGH WATERMARK starting point.
    Any rental with ID higher than this = new record.
    """
    conn = None
    try:
        conn = get_target_conn()
        cur  = conn.cursor()
        cur.execute("SELECT COALESCE(MAX(rental_id), 0) FROM rental")
        max_id = cur.fetchone()[0]
        cur.close()
        return max_id
    except Exception as e:
        log(f"WARNING: Could not get watermark from Azure: {e}")
        return 0
    finally:
        if conn:
            conn.close()


# ════════════════════════════════════════════════════════════
#  FETCH — Get new records from source
# ════════════════════════════════════════════════════════════
def fetch_new_rentals(since_id):
    """
    Gets rental records from LOCAL DB where rental_id > since_id.
    LIMIT 100 = process max 100 rows per cycle (prevents overload).
    """
    conn = None
    try:
        conn = get_source_conn()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                rental_id,
                rental_date,
                inventory_id,
                customer_id,
                return_date,
                staff_id,
                last_update
            FROM rental
            WHERE rental_id > %s
            ORDER BY rental_id ASC
            LIMIT 100
        """, (since_id,))
        rows = cur.fetchall()
        cur.close()
        return [dict(r) for r in rows]
    except Exception as e:
        log(f"WARNING: Could not fetch from source: {e}")
        return []
    finally:
        if conn:
            conn.close()


# ════════════════════════════════════════════════════════════
#  INSERT — Push records into Azure
# ════════════════════════════════════════════════════════════
def insert_into_azure(rentals):
    """
    Inserts rental records into Azure PostgreSQL.
    ON CONFLICT DO NOTHING = skip if already exists.
    This is IDEMPOTENT — safe to run multiple times.
    Same concept as GoldenGate HANDLECOLLISIONS!
    """
    if not rentals:
        return 0

    conn = None
    try:
        conn = get_target_conn()
        cur  = conn.cursor()
        inserted_count = 0

        for rental in rentals:
            cur.execute("""
                INSERT INTO rental (
                    rental_id, rental_date, inventory_id,
                    customer_id, return_date, staff_id, last_update
                ) VALUES (
                    %(rental_id)s, %(rental_date)s, %(inventory_id)s,
                    %(customer_id)s, %(return_date)s,
                    %(staff_id)s, %(last_update)s
                )
                ON CONFLICT (rental_id) DO NOTHING
            """, rental)
            inserted_count += cur.rowcount

        conn.commit()
        cur.close()
        return inserted_count

    except Exception as e:
        if conn:
            conn.rollback()
        log(f"WARNING: Could not insert into Azure: {e}")
        return 0
    finally:
        if conn:
            conn.close()


# ════════════════════════════════════════════════════════════
#  GET COUNTS — For logging before/after
# ════════════════════════════════════════════════════════════
def get_counts():
    """Returns rental counts from both source and Azure."""
    counts = {"source": "?", "azure": "?"}
    try:
        conn = get_source_conn()
        cur  = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM rental")
        counts["source"] = cur.fetchone()[0]
        cur.close()
        conn.close()
    except:
        counts["source"] = "OFFLINE"

    try:
        conn = get_target_conn()
        cur  = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM rental")
        counts["azure"] = cur.fetchone()[0]
        cur.close()
        conn.close()
    except:
        counts["azure"] = "OFFLINE"

    return counts


# ════════════════════════════════════════════════════════════
#  LOGGER
# ════════════════════════════════════════════════════════════
def log(message):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}")


# ════════════════════════════════════════════════════════════
#  MAIN SYNC LOOP
# ════════════════════════════════════════════════════════════
def run_replication():
    global last_synced_rental_id

    print("=" * 60)
    print("  Pagila Replication Script — Starting")
    print(f"  Source   : LOCAL PostgreSQL (WSL2 localhost)")
    print(f"  Target   : Azure PostgreSQL")
    print(f"  Interval : every {SYNC_INTERVAL} seconds")
    print(f"  Table    : rental (watermark = rental_id)")
    print("=" * 60)

    # Initialize watermark from Azure
    log("Initializing — reading current Azure watermark...")
    last_synced_rental_id = get_last_synced_id()
    log(f"Starting watermark : rental_id > {last_synced_rental_id}")
    log("Replication loop started! Press Ctrl+C to stop.\n")

    cycle = 0

    while True:
        cycle += 1
        print(f"{'─' * 60}")
        log(f"Sync cycle #{cycle} starting...")

        try:
            # Fetch new records from source
            new_rentals = fetch_new_rentals(last_synced_rental_id)

            if not new_rentals:
                counts = get_counts()
                log(f"IN SYNC — No new records found")
                log(f"   Source rentals : {counts['source']}")
                log(f"   Azure rentals  : {counts['azure']}")
                log(f"   Lag            : 0 rows")

            else:
                log(f"Found {len(new_rentals)} new rental(s) to sync")
                log(f"   First ID : {new_rentals[0]['rental_id']}")
                log(f"   Last ID  : {new_rentals[-1]['rental_id']}")

                # Insert into Azure
                inserted = insert_into_azure(new_rentals)

                # Move watermark forward
                last_synced_rental_id = new_rentals[-1]["rental_id"]

                counts = get_counts()
                log(f"SYNCED {inserted} record(s) to Azure successfully!")
                log(f"   New watermark  : rental_id > {last_synced_rental_id}")
                log(f"   Source rentals : {counts['source']}")
                log(f"   Azure rentals  : {counts['azure']}")

                if isinstance(counts["source"], int) and \
                   isinstance(counts["azure"], int):
                    lag = counts["source"] - counts["azure"]
                    log(f"   Remaining lag  : {lag} rows")

        except KeyboardInterrupt:
            raise
        except Exception as e:
            log(f"ERROR in cycle #{cycle}: {e}")
            log("  Will retry next cycle...")

        log(f"Sleeping {SYNC_INTERVAL}s until next sync...\n")
        time.sleep(SYNC_INTERVAL)


# ════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════
if __name__ == "__main__":
    try:
        run_replication()
    except KeyboardInterrupt:
        print("\n")
        log("Replication stopped by user (Ctrl+C)")
        log("To restart: python replication.py")
        print("=" * 60)
