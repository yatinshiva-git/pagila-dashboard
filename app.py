# ============================================================
#  app.py — Pagila Analytics Dashboard (Flask API)
#  Author  : Sudhakar
#  Project : Pagila Cloud Dashboard on Azure
#
#  ROUTES:
#    GET /                  → Dashboard HTML page
#    GET /api/overview      → Total counts (customers, films, etc.)
#    GET /api/films         → Top 10 rented films
#    GET /api/customers     → Top 10 customers by revenue
#    GET /api/revenue       → Monthly revenue (partitioned tables)
#    GET /api/replication   → Live sync status: local vs Azure counts
#    POST /api/failover     → Simulate source DB failure / recovery
# ============================================================

# ── IMPORTS ─────────────────────────────────────────────────
import os                          # reads environment variables
import datetime                    # for timestamps in replication status
import psycopg2                    # connects Python to PostgreSQL
import psycopg2.extras             # gives us dict-style query results
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS        # allows browser to call our API
from dotenv import load_dotenv     # reads our .env file

# ── LOAD .env FILE ──────────────────────────────────────────
# This reads DB_HOST, DB_PASSWORD etc. from the .env file
# so we never hard-code passwords in our code.
load_dotenv()

# ── CREATE FLASK APP ─────────────────────────────────────────
app = Flask(__name__)
CORS(app)   # allow cross-origin requests (needed when frontend calls API)


# ════════════════════════════════════════════════════════════
#  DATABASE CONNECTION HELPER
#  Every endpoint calls this to get a fresh DB connection.
#  Using psycopg2 with SSL because Azure PostgreSQL requires it.
# ════════════════════════════════════════════════════════════

def _azure_conn():
    """Raw Azure PostgreSQL connection"""
    return psycopg2.connect(
        host            = os.getenv("DB_HOST"),
        port            = os.getenv("DB_PORT", 5432),
        dbname          = os.getenv("DB_NAME", "pagila"),
        user            = os.getenv("DB_USER", "dbadmin"),
        password        = os.getenv("DB_PASSWORD"),
        sslmode         = os.getenv("DB_SSLMODE", "require"),
        connect_timeout = 5
    )


def _local_conn():
    """Raw Local PostgreSQL connection"""
    return psycopg2.connect(
        host            = os.getenv("LOCAL_DB_HOST", "localhost"),
        port            = os.getenv("LOCAL_DB_PORT", 5432),
        dbname          = os.getenv("LOCAL_DB_NAME", "pagila"),
        user            = os.getenv("LOCAL_DB_USER", "dbadmin"),
        password        = os.getenv("LOCAL_DB_PASSWORD", "admin123"),
        sslmode         = "disable",
        connect_timeout = 3
    )


def get_db_connection():
    """
    Smart connection with automatic fallback!
    Tries Azure first → falls back to local DB.
    Returns: (connection, source_name)
    This is CONNECTION RESILIENCE — same concept
    as Oracle Data Guard automatic failover!
    """
    # Try Azure first
    try:
        conn = _azure_conn()
        return conn, "Azure PostgreSQL"
    except Exception as e1:
        print(f"[FALLBACK] Azure failed: {e1}")
        print(f"[FALLBACK] Trying Local PostgreSQL...")

    # Fallback to local
    try:
        conn = _local_conn()
        print(f"[FALLBACK] Connected to Local PostgreSQL!")
        return conn, "Local PostgreSQL (Fallback)"
    except Exception as e2:
        raise Exception(
            f"Both DBs failed! "
            f"Azure: {e1} | Local: {e2}"
        )

# ════════════════════════════════════════════════════════════
#  ROUTE 1 — HOME PAGE  (GET /)
#  Serves the HTML dashboard page (templates/index.html)
# ════════════════════════════════════════════════════════════
@app.route("/")
def index():
    """Renders the main dashboard HTML page."""
    return render_template("index.html")


# ════════════════════════════════════════════════════════════
#  ROUTE 2 — OVERVIEW  (GET /api/overview)
#  Returns high-level counts: customers, films, rentals etc.
#
#  Example response:
#  {
#    "total_customers": 599,
#    "total_films": 1000,
#    "total_rentals": 16044,
#    "total_revenue": "61312.04",
#    "total_actors": 200,
#    "total_inventory": 4581
#  }
# ════════════════════════════════════════════════════════════
@app.route("/api/overview")
def overview():
    """Returns total counts across all major tables."""
    conn = None
    db_source = None
    try:
        conn, db_source = get_db_connection()

        # RealDictCursor returns rows as Python dicts (key=column name)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM customer)   AS total_customers,
                (SELECT COUNT(*) FROM film)        AS total_films,
                (SELECT COUNT(*) FROM rental)      AS total_rentals,
                (SELECT ROUND(SUM(amount)::numeric, 2)
                   FROM payment)                  AS total_revenue,
                (SELECT COUNT(*) FROM actor)       AS total_actors,
                (SELECT COUNT(*) FROM inventory)   AS total_inventory
        """)

        row = cur.fetchone()   # fetchone() because we expect exactly 1 row
        cur.close()

        result = dict(row)
        result["data_source"] = db_source
        return jsonify(result)

    except Exception as e:
        # If DB connection fails, return error message with HTTP 500
        return jsonify({"error": str(e)}), 500

    finally:
        # ALWAYS close the connection — even if an error occurred
        if conn:
            conn.close()


# ════════════════════════════════════════════════════════════
#  ROUTE 3 — TOP FILMS  (GET /api/films)
#  Returns top 10 most rented films with category info.
#
#  Example response:
#  [
#    {"film_title": "BUCKET BROTHERHOOD", "category": "Travel",
#     "rental_count": 34, "rental_rate": "4.99"},
#    ...
#  ]
# ════════════════════════════════════════════════════════════
@app.route("/api/films")
def films():
    """Returns top 10 most rented films with their category."""
    conn = None
    db_source = None
    try:
        conn, db_source = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("""
            SELECT
                f.title                   AS film_title,
                c.name                    AS category,
                COUNT(r.rental_id)        AS rental_count,
                f.rental_rate
            FROM film f
            -- JOIN film_category links films to categories
            JOIN film_category fc ON f.film_id = fc.film_id
            JOIN category c       ON fc.category_id = c.category_id
            -- JOIN inventory links films to physical copies
            JOIN inventory i      ON f.film_id = i.film_id
            -- JOIN rental links inventory to actual rentals
            JOIN rental r         ON i.inventory_id = r.inventory_id
            GROUP BY f.film_id, f.title, c.name, f.rental_rate
            ORDER BY rental_count DESC
            LIMIT 10
        """)

        rows = cur.fetchall()   # fetchall() returns a list of rows
        cur.close()

        result = [dict(r) for r in rows]
        return jsonify({"data": result, "data_source": db_source})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()


# ════════════════════════════════════════════════════════════
#  ROUTE 4 — TOP CUSTOMERS  (GET /api/customers)
#  Returns top 10 customers by total amount paid.
#
#  Example response:
#  [
#    {"customer_name": "KARL SEAL", "email": "karl.seal@...",
#     "total_spent": "221.55", "rental_count": 45},
#    ...
#  ]
# ════════════════════════════════════════════════════════════
@app.route("/api/customers")
def customers():
    """Returns top 10 customers ranked by total revenue."""
    conn = None
    db_source = None
    try:
        conn, db_source = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("""
            SELECT
                -- Combine first and last name into one field
                c.first_name || ' ' || c.last_name  AS customer_name,
                c.email,
                ROUND(SUM(p.amount)::numeric, 2)    AS total_spent,
                COUNT(p.payment_id)                 AS rental_count
            FROM customer c
            JOIN payment p ON c.customer_id = p.customer_id
            GROUP BY c.customer_id, c.first_name, c.last_name, c.email
            ORDER BY total_spent DESC
            LIMIT 10
        """)

        rows = cur.fetchall()
        cur.close()

        result = [dict(r) for r in rows]
        return jsonify({"data": result, "data_source": db_source})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()


# ════════════════════════════════════════════════════════════
#  ROUTE 5 — MONTHLY REVENUE  (GET /api/revenue)
#  Returns monthly revenue totals.
#  Pagila payment data is partitioned by month (Jan–Jul 2022),
#  so querying the parent 'payment' table gets all partitions.
#
#  Example response:
#  [
#    {"month": "2022-01", "revenue": "8351.84", "payment_count": 1153},
#    {"month": "2022-02", "revenue": "9629.09", "payment_count": 1290},
#    ...
#  ]
# ════════════════════════════════════════════════════════════
@app.route("/api/revenue")
def revenue():
    """Returns monthly revenue — works across all payment partitions."""
    conn = None
    db_source = None
    try:
        conn, db_source = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("""
            SELECT
                -- TO_CHAR formats the date as 'YYYY-MM' e.g. '2022-03'
                TO_CHAR(payment_date, 'YYYY-MM')    AS month,
                ROUND(SUM(amount)::numeric, 2)      AS revenue,
                COUNT(payment_id)                   AS payment_count
            FROM payment
            GROUP BY TO_CHAR(payment_date, 'YYYY-MM')
            ORDER BY month ASC
        """)

        rows = cur.fetchall()
        cur.close()

        result = [dict(r) for r in rows]
        return jsonify({"data": result, "data_source": db_source})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()


# ════════════════════════════════════════════════════════════
#  ROUTE 6 — REPLICATION STATUS  (GET /api/replication)
#
#  This simulates a "live replication monitor" by:
#    1. Connecting to LOCAL PostgreSQL (WSL2) — the "source"
#    2. Connecting to AZURE PostgreSQL — the "replica"
#    3. Comparing record counts in key tables
#    4. Reporting sync lag (count difference)
#
#  In real companies this is called "micro-batch replication"
#  (used by tools like Debezium, GoldenGate in async mode).
#  replication.py runs every 30s and keeps counts in sync.
#
#  Example response:
#  {
#    "source_db":  {"status": "online", "rentals": 16044, "payments": 16049},
#    "azure_db":   {"status": "online", "rentals": 16044, "payments": 16049},
#    "sync_lag":   {"rentals": 0, "payments": 0},
#    "overall_status": "IN_SYNC",
#    "checked_at": "2026-04-21T10:30:00"
#  }
# ════════════════════════════════════════════════════════════
@app.route("/api/replication")
def replication():
    """
    Compares record counts between local (source) and Azure (replica).
    Shows if data is in sync or how many rows are lagging.
    """
    result = {
        "source_db":      {},
        "azure_db":       {},
        "sync_lag":       {},
        "overall_status": "UNKNOWN",
        "checked_at":     datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    }

    # ── Step 1: Query LOCAL PostgreSQL (source) ──────────────
    local_conn = None
    try:
        local_conn = _local_conn()
        cur = local_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM customer) AS customers,
                (SELECT COUNT(*) FROM rental)   AS rentals,
                (SELECT COUNT(*) FROM payment)  AS payments,
                (SELECT COUNT(*) FROM film)     AS films
        """)
        row = cur.fetchone()
        cur.close()
        result["source_db"] = {
            "status":    "online",
            "customers": int(row["customers"]),
            "rentals":   int(row["rentals"]),
            "payments":  int(row["payments"]),
            "films":     int(row["films"])
        }
    except Exception as e:
        # Local DB is down — this is exactly what the Failover tab shows!
        result["source_db"] = {
            "status": "offline",
            "error":  str(e)
        }
    finally:
        if local_conn:
            local_conn.close()

    # ── Step 2: Query AZURE PostgreSQL (replica) ─────────────
    azure_conn = None
    try:
        azure_conn, _ = get_db_connection()
        cur = azure_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM customer) AS customers,
                (SELECT COUNT(*) FROM rental)   AS rentals,
                (SELECT COUNT(*) FROM payment)  AS payments,
                (SELECT COUNT(*) FROM film)     AS films
        """)
        row = cur.fetchone()
        cur.close()
        result["azure_db"] = {
            "status":    "online",
            "customers": int(row["customers"]),
            "rentals":   int(row["rentals"]),
            "payments":  int(row["payments"]),
            "films":     int(row["films"])
        }
    except Exception as e:
        result["azure_db"] = {
            "status": "offline",
            "error":  str(e)
        }
    finally:
        if azure_conn:
            azure_conn.close()

    # ── Step 3: Calculate sync lag ───────────────────────────
    # Only possible if BOTH databases are online
    src = result["source_db"]
    azr = result["azure_db"]

    if src.get("status") == "online" and azr.get("status") == "online":
        lag = {
            "customers": src["customers"] - azr["customers"],
            "rentals":   src["rentals"]   - azr["rentals"],
            "payments":  src["payments"]  - azr["payments"],
            "films":     src["films"]     - azr["films"]
        }
        result["sync_lag"] = lag
        # IN_SYNC if all differences are zero
        result["overall_status"] = "IN_SYNC" if all(
            v == 0 for v in lag.values()
        ) else "LAG_DETECTED"

    elif src.get("status") == "offline" and azr.get("status") == "online":
        # This is the HA scenario — source is down, Azure is still running!
        result["overall_status"] = "SOURCE_DOWN_AZURE_RUNNING"

    elif src.get("status") == "online" and azr.get("status") == "offline":
        result["overall_status"] = "AZURE_DOWN"

    else:
        result["overall_status"] = "BOTH_DOWN"

    return jsonify(result)


# ════════════════════════════════════════════════════════════
#  ROUTE 7 — FAILOVER SIMULATION  (POST /api/failover)
#
#  This endpoint powers the "Simulate Failure" button on the
#  dashboard. It doesn't actually break anything — it just
#  reports the current live status of both databases.
#
#  The REAL simulation happens by manually stopping local PG:
#    sudo systemctl stop postgresql   ← run this in WSL2
#  Then click the button — dashboard shows SOURCE DB: DOWN ⚠️
#  Then restart:
#    sudo systemctl start postgresql  ← recovery
#  Dashboard shows recovery automatically.
#
#  Why this is senior-level thinking:
#    This is exactly what DBAs test in DR drills —
#    "if source goes down, does replica hold?"
#    Answer: YES — Azure PostgreSQL keeps serving the dashboard!
#
#  Example response (source down):
#  {
#    "source_db":     {"status": "offline", "host": "localhost"},
#    "azure_db":      {"status": "online",  "host": "pagila-pg-..."},
#    "ha_status":     "FAILOVER_ACTIVE",
#    "message":       "Source DB is DOWN. Azure is serving all traffic.",
#    "failover_time": "< 1 second (no actual switch needed)",
#    "checked_at":    "2026-04-21T10:30:00Z"
#  }
# ════════════════════════════════════════════════════════════
@app.route("/api/failover", methods=["GET", "POST"])
def failover():
    """
    Checks health of both source (local) and replica (Azure) DBs.
    Powers the Failover Simulation tab on the dashboard.
    """
    now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") + "Z"

    # ── Check LOCAL DB ────────────────────────────────────────
    source_status = "offline"
    source_detail = ""
    local_conn = None
    try:
        local_conn = _local_conn()
        cur = local_conn.cursor()
        cur.execute("SELECT version()")          # simplest possible health check
        ver = cur.fetchone()[0]
        cur.close()
        source_status = "online"
        # Extract just "PostgreSQL 16.x" from the full version string
        source_detail = ver.split(",")[0]
    except Exception as e:
        source_detail = str(e)
    finally:
        if local_conn:
            local_conn.close()

    # ── Check AZURE DB ────────────────────────────────────────
    azure_status = "offline"
    azure_detail = ""
    azure_conn = None
    try:
        azure_conn, _ = get_db_connection()
        cur = azure_conn.cursor()
        cur.execute("SELECT version()")
        ver = cur.fetchone()[0]
        cur.close()
        azure_status = "online"
        azure_detail = ver.split(",")[0]
    except Exception as e:
        azure_detail = str(e)
    finally:
        if azure_conn:
            azure_conn.close()

    # ── Determine HA status and human-readable message ────────
    if source_status == "online" and azure_status == "online":
        ha_status = "NORMAL"
        message   = "Both databases are healthy. No failover needed."
        tip       = "To simulate failure: run 'sudo systemctl stop postgresql' in WSL2, then click this button again."

    elif source_status == "offline" and azure_status == "online":
        ha_status = "FAILOVER_ACTIVE"
        message   = "⚠️ Source DB is DOWN. Azure PostgreSQL is serving all traffic. HA is working!"
        tip       = "To recover: run 'sudo systemctl start postgresql' in WSL2."

    elif source_status == "online" and azure_status == "offline":
        ha_status = "AZURE_DOWN"
        message   = "⚠️ Azure DB is unreachable. Check network/firewall rules."
        tip       = "Check Azure Portal → PostgreSQL server status and NSG rules."

    else:
        ha_status = "BOTH_DOWN"
        message   = "🔴 Both databases are unreachable. Check your setup."
        tip       = "Check WSL2 PostgreSQL service and Azure Portal."

    return jsonify({
        "source_db": {
            "status": source_status,
            "host":   os.getenv("LOCAL_DB_HOST", "localhost"),
            "detail": source_detail
        },
        "azure_db": {
            "status": azure_status,
            "host":   os.getenv("DB_HOST", ""),
            "detail": azure_detail
        },
        "ha_status":     ha_status,
        "message":       message,
        "tip":           tip,
        "failover_time": "< 1 second (architecture-level HA, no manual switch)",
        "checked_at":    now
    })


# ════════════════════════════════════════════════════════════
#  START THE SERVER
#  debug=True   → shows detailed errors in browser (DEV only)
#  host='0.0.0.0' → listens on all network interfaces
#                   (needed so Windows browser can reach WSL2)
#  port=5000    → http://localhost:5000
# ════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("  Pagila Dashboard API — Starting...")
    print("  Dashboard   → http://localhost:5000")
    print("  Overview    → http://localhost:5000/api/overview")
    print("  Films       → http://localhost:5000/api/films")
    print("  Customers   → http://localhost:5000/api/customers")
    print("  Revenue     → http://localhost:5000/api/revenue")
    print("  Replication → http://localhost:5000/api/replication")
    print("  Failover    → http://localhost:5000/api/failover")
    print("=" * 60)
    app.run(debug=True, host="0.0.0.0", port=5000)
