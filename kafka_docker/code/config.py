# config.py — shared intre toti membrii echipei

BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094"

TOPICS = {
    "demographics": "demographics_events",
    "consumption":  "consumption_events",
    "health":       "health_events",
}

# Durability config:
# acks=all — producerul asteapta confirmarea de la toti brokerii in-sync
# rf=3 + min.insync.replicas=2 — cel putin 2 din 3 brokeri trebuie sa confirme
# Daca un broker cade, scrierile continua
# Daca 2 brokeri cad simultan, scrierile se blocheaza — durabilitate maxima
PRODUCER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all",
}

DATA_DIR = "../data"
SINK_DIR = "../sink"
