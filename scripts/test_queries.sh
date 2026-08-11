#!/bin/sh
# ═══════════════════════════════════════════════════════════════
#   FoodStream Analytics — Example Pinot Queries
#   Run from your host machine after services are up.
#   Usage: bash scripts/test_queries.sh
# ═══════════════════════════════════════════════════════════════

PINOT_BROKER="http://localhost:8099"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

print_header() {
  echo ""
  echo "${YELLOW}══════════════════════════════════════════════════════${NC}"
  echo "${YELLOW}  $1${NC}"
  echo "${YELLOW}══════════════════════════════════════════════════════${NC}"
}

run_query() {
  local title="$1"
  local sql="$2"
  print_header "$title"
  echo "${CYAN}SQL:${NC} $sql"
  echo ""
  curl -s -X POST "${PINOT_BROKER}/query/sql" \
    -H "Content-Type: application/json" \
    -d "{\"sql\": \"${sql}\", \"queryOptions\": \"groupByMode=sql;responseFormat=sql\"}" \
    | python3 -m json.tool 2>/dev/null \
    | grep -A 1000 '"resultTable"' \
    | head -60 \
    || echo "(install python3 for formatted output)"
  echo ""
}

echo ""
echo "╔═══════════════════════════════════════════════════════╗"
echo "║    🍔  FoodStream Analytics — Live Query Dashboard    ║"
echo "╚═══════════════════════════════════════════════════════╝"
echo "Broker: ${PINOT_BROKER}"

run_query "1️⃣  Total Orders Ingested" \
  "SELECT COUNT(*) AS total_orders, ROUND(SUM(price), 2) AS total_revenue FROM food_orders LIMIT 1"

run_query "2️⃣  Revenue & Orders by City" \
  "SELECT city, COUNT(*) AS orders, ROUND(SUM(price), 2) AS revenue, ROUND(AVG(price), 2) AS avg_order_value FROM food_orders GROUP BY city ORDER BY revenue DESC LIMIT 10"

run_query "3️⃣  Top Cuisines by Popularity" \
  "SELECT cuisine, COUNT(*) AS orders, ROUND(AVG(price), 2) AS avg_price FROM food_orders GROUP BY cuisine ORDER BY orders DESC"

run_query "4️⃣  Order Status Breakdown" \
  "SELECT status, COUNT(*) AS count, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct FROM food_orders GROUP BY status ORDER BY count DESC"

run_query "5️⃣  Payment Method Distribution" \
  "SELECT payment_method, COUNT(*) AS orders, ROUND(SUM(price), 2) AS total_revenue FROM food_orders GROUP BY payment_method ORDER BY orders DESC"

run_query "6️⃣  Peak vs Off-Peak Orders" \
  "SELECT is_peak_hour, COUNT(*) AS orders, ROUND(AVG(price), 2) AS avg_price, ROUND(AVG(delivery_time_mins), 1) AS avg_delivery_mins FROM food_orders GROUP BY is_peak_hour"

run_query "7️⃣  Top 10 Restaurants by Revenue" \
  "SELECT restaurant_name, COUNT(*) AS orders, ROUND(SUM(price), 2) AS total_revenue, ROUND(AVG(rating), 2) AS avg_rating FROM food_orders WHERE status = 'delivered' GROUP BY restaurant_name ORDER BY total_revenue DESC LIMIT 10"

run_query "8️⃣  Order Value Category Breakdown" \
  "SELECT order_value_category, COUNT(*) AS orders, ROUND(AVG(price), 2) AS avg_price FROM food_orders GROUP BY order_value_category ORDER BY avg_price DESC"

run_query "9️⃣  Hourly Order Volume (Last 24h)" \
  "SELECT hour_of_day, COUNT(*) AS orders FROM food_orders GROUP BY hour_of_day ORDER BY hour_of_day"

run_query "🔟  High-Value Orders with Ratings" \
  "SELECT restaurant_name, city, cuisine, price, rating FROM food_orders WHERE is_high_value = 1 AND status = 'delivered' ORDER BY price DESC LIMIT 10"

echo "${GREEN}✅  All queries complete!${NC}"
echo "💡  Tip: Open Pinot UI at http://localhost:9000 for interactive queries"
echo ""
