import time
import subprocess
import redis

# ======================================================
# FP – Scenario 1 (CLEAN OUTPUT + FIXED PIPE FOR WINDOWS)
# Redis Replication & Eventual Consistency Observation
# - Observe replication via: docker exec redis-cli INFO replication
# - Bulk write via: redis-cli --pipe (binary payload, safe on Windows)
# - Add missed_count from 1 replica read
# ======================================================

MASTER_CONTAINER = "redis-master"
MASTER_PORT_IN_CONTAINER = 6379

# Docker published ports on Windows host
MASTER_HOST = "localhost"
MASTER_PUBLISHED_PORT = 6379

# Choose one replica published port (from your docker ps output)
REPLICA_HOST = "localhost"
REPLICA_PUBLISHED_PORT = 6380  # redis-replica-1 -> 0.0.0.0:6380->6379

TOTAL_OPERATIONS = 50_000
KEY_NAMESPACE = "fp_s1_"
STABILIZATION_WAIT = 1  # seconds

PAYLOAD_BYTES = 2048
PIPE_BATCH = 5000


# -----------------------------
# Subprocess helpers
# -----------------------------
def sh_text(cmd):
    return subprocess.check_output(cmd, text=True).strip()


def sh_quiet_bytes(cmd, payload_bytes: bytes):
    """
    Run command with binary stdin to avoid newline translation on Windows.
    Suppress stdout/stderr to remove redis-cli --pipe spam.
    """
    subprocess.run(
        cmd,
        input=payload_bytes,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=True
    )


def docker_redis_cli(*args):
    cmd = ["docker", "exec", "-i", MASTER_CONTAINER, "redis-cli"] + list(args)
    return sh_text(cmd)


def fetch_replication_info_raw():
    return docker_redis_cli("INFO", "replication")


def get_master_repl_offset(raw):
    for line in raw.splitlines():
        if line.startswith("master_repl_offset:"):
            return int(line.split(":", 1)[1])
    return None


def get_connected_slaves(raw):
    for line in raw.splitlines():
        if line.startswith("connected_slaves:"):
            return int(line.split(":", 1)[1])
    return 0


def parse_slave_offsets_lags(raw):
    offsets, lags = [], []
    for line in raw.splitlines():
        if line.startswith("slave") and "offset=" in line:
            parts = line.split(",")
            for p in parts:
                if p.startswith("offset="):
                    offsets.append(int(p.split("=")[1]))
                elif p.startswith("lag="):
                    lags.append(int(p.split("=")[1]))
    return offsets, lags


def docker_dbsize(container_name):
    out = subprocess.check_output(
        ["docker", "exec", "-i", container_name, "redis-cli", "-p", "6379", "DBSIZE"],
        text=True
    ).strip()
    return int(out.split()[-1])


# -----------------------------
# RESP builders (binary-safe)
# -----------------------------
def resp_set_bytes(key: str, val: str) -> bytes:
    # *3\r\n$3\r\nSET\r\n$<len>\r\n<key>\r\n$<len>\r\n<val>\r\n
    key_b = key.encode("utf-8")
    val_b = val.encode("utf-8")
    return (
        b"*3\r\n$3\r\nSET\r\n$" + str(len(key_b)).encode() + b"\r\n" + key_b + b"\r\n"
        b"$" + str(len(val_b)).encode() + b"\r\n" + val_b + b"\r\n"
    )


def resp_del_bytes(key: str) -> bytes:
    key_b = key.encode("utf-8")
    return (
        b"*2\r\n$3\r\nDEL\r\n$" + str(len(key_b)).encode() + b"\r\n" + key_b + b"\r\n"
    )


def pipe_send(payload: bytes):
    cmd = ["docker", "exec", "-i", MASTER_CONTAINER, "redis-cli", "-p", str(MASTER_PORT_IN_CONTAINER), "--pipe"]
    sh_quiet_bytes(cmd, payload)


def missed_count(replica_client, total_ops, key_ns):
    missed = 0
    for i in range(total_ops):
        if replica_client.get(f"{key_ns}{i}") is None:
            missed += 1
    return missed


# -----------------------------
# Main scenario
# -----------------------------
def main():
    print("\n=== FP Scenario 1: Replication & Eventual Consistency Test (CLEAN) ===")
    print("-" * 83)

    # connect to master (host -> published port)
    master = redis.Redis(host=MASTER_HOST, port=MASTER_PUBLISHED_PORT, decode_responses=True)
    master.ping()

    # connect to one replica (host -> published port)
    replica = redis.Redis(host=REPLICA_HOST, port=REPLICA_PUBLISHED_PORT, decode_responses=True)
    replica.ping()

    # Phase 1: baseline
    print("\n[Phase 1] Baseline Replication State (Before Write)")
    raw_before = fetch_replication_info_raw()
    connected_slaves = get_connected_slaves(raw_before)
    master_offset_before = get_master_repl_offset(raw_before)
    db_before = docker_dbsize(MASTER_CONTAINER)
    print(f"connected_slaves={connected_slaves}, master_repl_offset={master_offset_before}, dbsize={db_before}")

    # Phase 2: bulk write (pipe)
    print(f"\n[Phase 2] Bulk write {TOTAL_OPERATIONS} keys via --pipe (payload={PAYLOAD_BYTES}B)")
    pad = "X" * PAYLOAD_BYTES
    t0 = time.time()

    sent = 0
    while sent < TOTAL_OPERATIONS:
        chunk_n = min(PIPE_BATCH, TOTAL_OPERATIONS - sent)
        now = time.time()

        buf = bytearray()
        for j in range(chunk_n):
            i = sent + j
            k = f"{KEY_NAMESPACE}{i}"
            v = f"{now}_{i}_{pad}"
            buf += resp_set_bytes(k, v)

        pipe_send(bytes(buf))
        sent += chunk_n

    dur = time.time() - t0
    db_after = docker_dbsize(MASTER_CONTAINER)
    print(f"✔ Write completed in {dur:.3f}s (~{int(TOTAL_OPERATIONS / dur)} ops/sec)")
    print(f"[After write] dbsize={db_after} (must be > 0)")

    # Phase 3: immediate observation
    print("\n[Phase 3] Immediate Replication Observation (RIGHT NOW)")
    raw_now = fetch_replication_info_raw()
    mo_now = get_master_repl_offset(raw_now)
    so_now, lag_now = parse_slave_offsets_lags(raw_now)

    missed_now = missed_count(replica, TOTAL_OPERATIONS, KEY_NAMESPACE)
    print(f"master_repl_offset: {master_offset_before} -> {mo_now}")
    print(f"replica_offsets   : {so_now}")
    print(f"replica_lag(sec)  : {lag_now}")
    print(f"missed_keys       : {missed_now}/{TOTAL_OPERATIONS}")

    # Phase 4: stabilization
    print(f"\n[Phase 4] Waiting {STABILIZATION_WAIT}s for stabilization window")
    time.sleep(STABILIZATION_WAIT)

    raw_late = fetch_replication_info_raw()
    so_late, lag_late = parse_slave_offsets_lags(raw_late)

    missed_late = missed_count(replica, TOTAL_OPERATIONS, KEY_NAMESPACE)
    print("\n[Phase 4 Result] After Stabilization")
    print(f"replica_offsets   : {so_late}")
    print(f"replica_lag(sec)  : {lag_late}")
    print(f"missed_keys       : {missed_late}/{TOTAL_OPERATIONS}")

    # Summary
    print("\n=== ANALYSIS SUMMARY ===")
    print(f"Write Throughput     : {int(TOTAL_OPERATIONS / dur)} ops/sec")
    print(f"Replica Lag late     : {lag_late}")
    if missed_late == 0:
        print("✔ Eventual consistency achieved")
    else:
        print("⚠ Replication lag still observed (async behavior visible)")
    print("CAP Perspective      : AP (Availability + Partition Tolerance)")
    print("-" * 83)

    # Cleanup
    print("\n[Cleanup] Deleting keys via --pipe ...")
    sent = 0
    while sent < TOTAL_OPERATIONS:
        chunk_n = min(PIPE_BATCH, TOTAL_OPERATIONS - sent)

        buf = bytearray()
        for j in range(chunk_n):
            i = sent + j
            k = f"{KEY_NAMESPACE}{i}"
            buf += resp_del_bytes(k)

        pipe_send(bytes(buf))
        sent += chunk_n

    print(f"✔ Cleanup completed. dbsize={docker_dbsize(MASTER_CONTAINER)}")


if __name__ == "__main__":
    main()
