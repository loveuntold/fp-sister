import time
import sys
import redis
from redis.sentinel import Sentinel, MasterNotFoundError

# ==============================
# FP Scenario 2 (CLEAN)
# Redis Sentinel Failover Test
# ==============================

# Sentinel ports mapped to host (docker-compose)
SENTINELS = [
    ("localhost", 26379),
    ("localhost", 26380),
    ("localhost", 26381),
]

SERVICE_NAME = "init_master"  # must match sentinel monitor <name> ...
# Published ports on host for your redis containers (docker-compose)
PUBLISHED_PORTS = {
    "redis-master": 6379,
    "redis-replica-1": 6380,
    "redis-replica-2": 6381,
    "redis-replica-3": 6382,
    "redis-replica-4": 6383,
}
# We don't know which replica becomes new master, so we try these ports
CANDIDATE_WRITE_PORTS = [6379, 6380, 6381, 6382, 6383]

WRITE_BEFORE = 2000
WRITE_AFTER = 2000
KEY_PREFIX = "fp_s2_"

SENTINEL_SOCKET_TIMEOUT = 0.2
REDIS_SOCKET_TIMEOUT = 1.0


def connect_sentinel():
    return Sentinel(SENTINELS, socket_timeout=SENTINEL_SOCKET_TIMEOUT)


def discover_master_ip_port(sentinel_conn):
    return sentinel_conn.discover_master(SERVICE_NAME)


def connect_writable_master_via_host_ports():
    """
    Docker Sentinel returns INTERNAL IP (172.19.x.x). Host (Windows) can't connect to that.
    So for writes we probe published ports on localhost and pick the one that is role=master.
    """
    last_err = None
    for port in CANDIDATE_WRITE_PORTS:
        try:
            r = redis.Redis(
                host="localhost",
                port=port,
                decode_responses=True,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
            )
            info = r.info("replication")
            if info.get("role") == "master":
                return r, port
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Could not find writable master via host ports. Last error: {last_err}")


def write_keys(port, start_idx, count):
    r = redis.Redis(
        host="localhost",
        port=port,
        decode_responses=True,
        socket_timeout=REDIS_SOCKET_TIMEOUT,
    )
    t0 = time.time()
    pipe = r.pipeline(transaction=False)
    now = time.time()
    for i in range(start_idx, start_idx + count):
        pipe.set(f"{KEY_PREFIX}{i}", f"{now}_{i}")
    pipe.execute()
    return time.time() - t0


def missed_count_on_replicas(sample_start, sample_count):
    """
    Optional: read-after-write style check to show eventual consistency.
    We'll sample reads from replicas (published ports 6380-6383).
    """
    missed_total = 0
    checked = 0

    replica_ports = [6380, 6381, 6382, 6383]
    for rp in replica_ports:
        try:
            rr = redis.Redis(
                host="localhost",
                port=rp,
                decode_responses=True,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
            )
            # quick check: only count for replicas that are actually reachable
            _ = rr.ping()
        except Exception:
            continue

        local_missed = 0
        for i in range(sample_start, sample_start + sample_count):
            if rr.get(f"{KEY_PREFIX}{i}") is None:
                local_missed += 1
        missed_total += local_missed
        checked += 1

    return missed_total, checked


def cleanup_all_keys(total_keys):
    # delete from whichever is master now
    r, port = connect_writable_master_via_host_ports()
    keys = [f"{KEY_PREFIX}{i}" for i in range(1, total_keys + 1)]
    CHUNK = 5000
    for i in range(0, len(keys), CHUNK):
        r.delete(*keys[i : i + CHUNK])
    return port


def main():
    print("\n=== FP Scenario 2: Redis Sentinel Failover Test (CLEAN) ===")
    print("-----------------------------------------------------------")

    # 1) connect sentinel + get current master (for monitoring info)
    sentinel_conn = connect_sentinel()

    try:
        m_ip, m_port = discover_master_ip_port(sentinel_conn)
        print(f"✔ master(before)={m_ip}:{m_port}")
    except MasterNotFoundError:
        print("❌ Master not found from Sentinel (monitoring not ready).")
        return
    except Exception as e:
        print(f"❌ Sentinel connection error: {e}")
        return

    # 2) find actual writable master port on HOST and write BEFORE failover
    r_master, host_master_port = connect_writable_master_via_host_ports()
    print(f"[Info] writable master (host) = localhost:{host_master_port}")

    print(f"[Phase 1] write {WRITE_BEFORE} keys (before failover)")
    d1 = write_keys(host_master_port, 1, WRITE_BEFORE)
    print(f"✔ write(before) done in {d1:.3f}s")

    # quick consistency sample (optional but requested earlier)
    missed, checked = missed_count_on_replicas(1, min(200, WRITE_BEFORE))
    if checked > 0:
        print(f"[Phase 1] missed_count(sample@replicas)={missed} (checked_replicas={checked})")
    else:
        print("[Phase 1] missed_count(sample@replicas)=N/A (no replicas reachable on host ports)")

    # 3) trigger failover manually (stop current master container)
    # NOTE: we DO NOT do docker stop here to keep logic minimal; user can stop manually.
    print("\n[Phase 2] ACTION: stop current master container to trigger failover")
    print("Example:")
    print("  docker stop redis-master")
    print("Monitoring... (Ctrl+C to stop)\n")

    last_master = f"{m_ip}:{m_port}"
    t_failover_start = time.time()
    failover_detected_at = None
    new_master = None

    try:
        while True:
            try:
                cur_ip, cur_port = discover_master_ip_port(sentinel_conn)
                cur = f"{cur_ip}:{cur_port}"

                if cur != last_master:
                    failover_detected_at = time.time()
                    new_master = cur
                    dt = failover_detected_at - t_failover_start
                    print(f"[{time.strftime('%H:%M:%S')}] 🚨 FAILOVER FOUND! new_master={cur} (t={dt:.2f}s)")
                    break

                # print first master once
                if last_master and cur == last_master:
                    # keep output clean: dots
                    sys.stdout.write("-")
                    sys.stdout.flush()

                time.sleep(0.5)

            except MasterNotFoundError:
                print(f"\n[{time.strftime('%H:%M:%S')}] Master not found! (voting in progress?)")
                time.sleep(0.5)

            except Exception:
                sys.stdout.write("-")
                sys.stdout.flush()
                time.sleep(1)

    except KeyboardInterrupt:
        print("\nMonitoring stopped.")
        return

    # 4) after failover, write AGAIN to the new master (via host ports scan)
    r_master2, host_master_port2 = connect_writable_master_via_host_ports()
    print(f"\n[Phase 3] writable master(after failover) = localhost:{host_master_port2}")
    print(f"[Phase 3] write {WRITE_AFTER} keys (after failover)")
    d2 = write_keys(host_master_port2, WRITE_BEFORE + 1, WRITE_AFTER)
    print(f"✔ write(after) done in {d2:.3f}s")

    missed2, checked2 = missed_count_on_replicas(WRITE_BEFORE + 1, min(200, WRITE_AFTER))
    if checked2 > 0:
        print(f"[Phase 3] missed_count(sample@replicas)={missed2} (checked_replicas={checked2})")
    else:
        print("[Phase 3] missed_count(sample@replicas)=N/A (no replicas reachable on host ports)")

    # 5) summary
    print("\n=== SUMMARY ===")
    print(f"- master(before)           : {last_master}")
    print(f"- master(after)            : {new_master}")
    print(f"- failover_detection_time  : {(failover_detected_at - t_failover_start):.2f}s")
    print(f"- write(before/after)      : {d1:.3f}s / {d2:.3f}s")

    # 6) cleanup
    total_keys = WRITE_BEFORE + WRITE_AFTER
    cleaned_on = cleanup_all_keys(total_keys)
    print(f"✔ Cleanup completed (issued via localhost:{cleaned_on})")


if __name__ == "__main__":
    main()
