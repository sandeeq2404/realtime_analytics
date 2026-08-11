#!/bin/sh
# ═══════════════════════════════════════════════════════════════
#   FoodStream Analytics — Pinot Initialization Script
#   Runs once after Pinot Controller + Broker are healthy.
#   Creates the food_orders schema and REALTIME table.
# ═══════════════════════════════════════════════════════════════

set -e

PINOT_CONTROLLER="http://pinot-controller:9000"
SCHEMA_FILE="/pinot/schemas/food_orders_schema.json"
TABLE_FILE="/pinot/tables/food_orders_table.json"

# ── Wait for Pinot Controller ────────────────────────────────
echo ""
echo "╔════════════════════════════════════════╗"
echo "║   Pinot Initialization Starting...     ║"
echo "╚════════════════════════════════════════╝"
echo ""
echo "⏳ Waiting for Pinot Controller at ${PINOT_CONTROLLER}..."

MAX_WAIT=120
ELAPSED=0
until curl -sf "${PINOT_CONTROLLER}/health" > /dev/null 2>&1; do
  if [ $ELAPSED -ge $MAX_WAIT ]; then
    echo "❌ Timed out waiting for Pinot Controller!"
    exit 1
  fi
  echo "   → Not ready yet (${ELAPSED}s elapsed), retrying in 10s..."
  sleep 10
  ELAPSED=$((ELAPSED + 10))
done

echo "✅ Pinot Controller is ready!"
sleep 5  # Extra buffer for cluster stabilization

# ── Create Schema ────────────────────────────────────────────
echo ""
echo "📋 Creating food_orders schema..."
SCHEMA_RESP=$(curl -sf -X POST \
  "${PINOT_CONTROLLER}/schemas" \
  -H "Content-Type: application/json" \
  --data-binary @"${SCHEMA_FILE}" 2>&1 || echo "SCHEMA_ERROR")

if echo "${SCHEMA_RESP}" | grep -qi "error\|exception"; then
  echo "⚠️  Schema may already exist or had an issue: ${SCHEMA_RESP}"
else
  echo "✅ Schema created: ${SCHEMA_RESP}"
fi

# ── Create REALTIME Table ────────────────────────────────────
echo ""
echo "📦 Creating food_orders REALTIME table..."
TABLE_RESP=$(curl -sf -X POST \
  "${PINOT_CONTROLLER}/tables" \
  -H "Content-Type: application/json" \
  --data-binary @"${TABLE_FILE}" 2>&1 || echo "TABLE_ERROR")

if echo "${TABLE_RESP}" | grep -qi "error\|exception"; then
  echo "⚠️  Table may already exist or had an issue: ${TABLE_RESP}"
else
  echo "✅ Table created: ${TABLE_RESP}"
fi

# ── Done ─────────────────────────────────────────────────────
echo ""
echo "╔════════════════════════════════════════════════════╗"
echo "║   🎉  Pinot Initialization Complete!               ║"
echo "║                                                    ║"
echo "║   🌐 Pinot UI  → http://localhost:9000             ║"
echo "║   📡 Query API → http://localhost:8099/query/sql   ║"
echo "║   🔥 Kafka UI  → http://localhost:8080             ║"
echo "╚════════════════════════════════════════════════════╝"
echo ""
