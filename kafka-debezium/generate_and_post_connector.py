import os
import json
import requests
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")

# -----------------------------
# Build connector config
# -----------------------------
connector_config = {
    "name": "banking_server_clean",
    "config": {
        # ── Core connector ──────────────────────────────────────────────
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

        # ── Postgres connection ──────────────────────────────────────────
        "database.hostname": os.getenv("POSTGRES_HOST"),
        "database.port":     os.getenv("POSTGRES_PORT", "5432"),
        "database.user":     os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname":   os.getenv("POSTGRES_DB"),

        # ── Topic / table routing ────────────────────────────────────────
        # Must match the prefix used in kafka_consumer.py:
        #   banking_server_v3.public.customers
        #   banking_server_v3.public.accounts
        #   banking_server_v3.public.transactions
        "topic.prefix":        "banking_server_clean",
        "table.include.list":  "public.customers,public.accounts,public.transactions",

        # ── Replication / snapshot ───────────────────────────────────────
        "plugin.name":                  "pgoutput",
        "slot.name":                    "banking_server_clean",
        "publication.name":             "banking_pub_v4",
        "publication.autocreate.mode":  "filtered",
        "snapshot.mode":                "initial",

        # ── Serialisation ────────────────────────────────────────────────
        "decimal.handling.mode": "string",   # → native float; change to
                                             #   "string" if you need exact
                                             #   precision for large decimals
        "tombstones.on.delete":  "false",

        # ── Heartbeat (keeps the replication slot alive under low load) ──
        "heartbeat.interval.ms": "30000",
    },
}

# -----------------------------
# Helpers
# -----------------------------
HEADERS = {"Content-Type": "application/json"}
NAME    = connector_config["name"]


def connector_exists() -> bool:
    r = requests.get(f"{CONNECT_URL}/connectors/{NAME}", timeout=10)
    return r.status_code == 200


def delete_connector() -> None:
    r = requests.delete(f"{CONNECT_URL}/connectors/{NAME}", timeout=10)
    r.raise_for_status()
    print(f"🗑️  Deleted existing connector '{NAME}'.")


def create_connector() -> None:
    r = requests.post(
        f"{CONNECT_URL}/connectors",
        headers=HEADERS,
        data=json.dumps(connector_config),
        timeout=10,
    )
    if r.status_code == 201:
        print(f"✅ Connector '{NAME}' created successfully!")
        print(json.dumps(r.json(), indent=2))
    elif r.status_code == 409:
        # Race condition – shouldn't happen after delete, but handle gracefully
        print(f"⚠️  Connector '{NAME}' already exists (409).")
    else:
        print(f"❌ Failed to create connector ({r.status_code}): {r.text}")
        r.raise_for_status()


def check_status() -> None:
    r = requests.get(f"{CONNECT_URL}/connectors/{NAME}/status", timeout=10)
    if r.status_code == 200:
        status = r.json()
        state  = status.get("connector", {}).get("state", "UNKNOWN")
        print(f"\n📊 Connector state : {state}")
        for task in status.get("tasks", []):
            print(f"   Task {task['id']} → {task['state']}")
    else:
        print(f"⚠️  Could not retrieve status ({r.status_code}).")


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    print(f"🔌 Targeting Kafka Connect at: {CONNECT_URL}\n")

    if connector_exists():
        print(f"ℹ️  Connector '{NAME}' already exists — recreating...")
        delete_connector()

    create_connector()
    check_status()