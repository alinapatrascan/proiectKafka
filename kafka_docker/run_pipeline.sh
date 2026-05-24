#!/usr/bin/env bash
# run_pipeline.sh
# Porneste pipeline-ul complet pentru demo live.
# Toti consumerii pornesc in background, apoi toti producerii ruleaza secvential.

set -e
cd "$(dirname "$0")/code"

SINK_DIR="../sink"
LOG_DIR="../logs"
mkdir -p "$SINK_DIR" "$LOG_DIR"

echo "======================================================"
echo "  KAFKA FAST FOOD HEALTH IMPACT — DEMO PIPELINE"
echo "======================================================"

# ── Curata sink-urile si log-urile anterioare ──────────────
echo "[1/4] Cleaning previous sink, logs and Kafka topics..."
rm -f "$SINK_DIR"/*.csv
rm -f "$LOG_DIR"/*.log

docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --delete --topic demographics_events 2>/dev/null || true
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --delete --topic consumption_events 2>/dev/null || true
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --delete --topic health_events 2>/dev/null || true

sleep 3

docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --create \
  --topic demographics_events --partitions 3 --replication-factor 3
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --create \
  --topic consumption_events --partitions 3 --replication-factor 3
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --create \
  --topic health_events --partitions 3 --replication-factor 3

sleep 3
echo "Topics recreated clean."
# ── Porneste toti consumerii in background ─────────────────
echo ""
echo "[2/4] Starting consumers in background..."

python consumer_demographics.py   > "$LOG_DIR/consumer_demographics_g1.log"   2>&1 &
PID_D1=$!
python consumer_demographics_group2.py > "$LOG_DIR/consumer_demographics_g2.log" 2>&1 &
PID_D2=$!

python consumer_consumption.py    > "$LOG_DIR/consumer_consumption_g1.log"    2>&1 &
PID_C1=$!
python consumer_consumption_group2.py  > "$LOG_DIR/consumer_consumption_g2.log"  2>&1 &
PID_C2=$!

python consumer_health.py         > "$LOG_DIR/consumer_health_g1.log"         2>&1 &
PID_H1=$!
python consumer_health_group2.py  > "$LOG_DIR/consumer_health_g2.log"         2>&1 &
PID_H2=$!

echo "  demographics-group-1 PID: $PID_D1"
echo "  demographics-group-2 PID: $PID_D2"
echo "  consumption-group-1  PID: $PID_C1"
echo "  consumption-group-2  PID: $PID_C2"
echo "  health-group-1       PID: $PID_H1"
echo "  health-group-2       PID: $PID_H2"

echo ""
echo "Waiting 5s for consumers to connect..."
sleep 5

# ── Ruleaza toti producerii secvential ────────────────────
echo ""
echo "[3/4] Running producers..."
echo ""

echo "----- Producer: demographics -----"
python producer_demographics.py

echo ""
echo "----- Producer: consumption -----"
python producer_consumption.py

echo ""
echo "----- Producer: health -----"
python producer_health.py

echo ""
echo "Waiting 10s for consumers to finish processing..."
sleep 30

# ── Opreste toti consumerii ───────────────────────────────
echo ""
echo "[4/4] Stopping consumers..."
kill $PID_D1 $PID_D2 $PID_C1 $PID_C2 $PID_H1 $PID_H2 2>/dev/null
wait 2>/dev/null

# ── Rezultate finale ──────────────────────────────────────
echo ""
echo "======================================================"
echo "  SINK RESULTS"
echo "======================================================"
wc -l "$SINK_DIR"/*.csv

echo ""
echo "======================================================"
echo "  SAMPLE — demographics_sink.csv"
echo "======================================================"
head -3 "$SINK_DIR/demographics_sink.csv"

echo ""
echo "======================================================"
echo "  SAMPLE — consumption_sink.csv"
echo "======================================================"
head -3 "$SINK_DIR/consumption_sink.csv"

echo ""
echo "======================================================"
echo "  SAMPLE — health_sink.csv"
echo "======================================================"
head -3 "$SINK_DIR/health_sink.csv"

echo ""
echo "Pipeline finished."
