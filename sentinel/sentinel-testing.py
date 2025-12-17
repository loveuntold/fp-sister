import time
import sys
import subprocess
import threading
import logging
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

# If True the script will attempt to `docker stop redis-master` to trigger failover.
# Keep False by default to avoid surprises; set to True only when running in controlled env.
AUTO_STOP_MASTER = False

# Delay (seconds) before issuing docker stop when AUTO_STOP_MASTER is True
AUTO_STOP_DELAY_SECONDS = 10

# Log file path for capturing events when AUTO_STOP_MASTER runs
LOG_FILE = "sentinel-failover.log"


def setup_logging():
    # file handler (detailed DEBUG) + stdout handler (concise INFO)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # detailed file handler
    fh = logging.FileHandler(LOG_FILE, mode="w")
    fh.setLevel(logging.DEBUG)
    fh_fmt = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    fh.setFormatter(fh_fmt)
    logger.addHandler(fh)

    # concise stdout handler for neat terminal output
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh_fmt = logging.Formatter("%(message)s")
    sh.setFormatter(sh_fmt)
    logger.addHandler(sh)


def delayed_stop_master(delay_seconds):
    logging.info(f"Auto-stop scheduled in {delay_seconds}s: will run 'docker stop redis-master'")
    time.sleep(delay_seconds)
    logging.info("Issuing: docker stop redis-master")
    try:
        p = subprocess.run(["docker", "stop", "redis-master"], capture_output=True, text=True, check=False)
        logging.info(f"docker stop exit={p.returncode}; stdout={p.stdout.strip()} stderr={p.stderr.strip()}")
    except Exception as e:
        logging.exception(f"Failed to run docker stop: {e}")


def capture_sentinel_snapshots(sentinels):
    """Capture a minimal INFO snapshot from each sentinel for timeline analysis."""
    snaps = []
    for h, p in sentinels:
        try:
            r = redis.Redis(host=h, port=p, socket_timeout=0.5)
            info = r.info()
            snaps.append({"host": h, "port": p, "ts": time.time(), "info_keys": {k: info.get(k) for k in ("role", "sentinel_masters", "connected_slaves") if k in info}})
        except Exception as e:
            snaps.append({"host": h, "port": p, "ts": time.time(), "error": str(e)})
    logging.debug("Captured sentinel snapshots (raw): %s", snaps)
    # concise summary for terminal/log at INFO
    try:
        summary = [{"host": s["host"], "port": s["port"], "sentinel_masters": s.get("info_keys", {}).get("sentinel_masters")} for s in snaps]
    except Exception:
        summary = snaps
    logging.info("Captured sentinel snapshots summary: %s", summary)
    return snaps


def log_sentinels_during_election(sentinels):
    """Query each sentinel for 'SENTINEL masters' and 'SENTINEL sentinels <name>' and log results."""
    for h, p in sentinels:
        try:
            r = redis.Redis(host=h, port=p, socket_timeout=0.5)
            try:
                masters = r.execute_command("SENTINEL", "masters")
            except Exception as e:
                masters = f"ERR: {e}"
            try:
                sent_list = r.execute_command("SENTINEL", "sentinels", SERVICE_NAME)
            except Exception as e:
                sent_list = f"ERR: {e}"

            # detailed raw output to file (DEBUG)
            logging.debug("Sentinel %s:%s raw masters=%s sentinels=%s", h, p, masters, sent_list)

            # concise terminal-friendly summary
            try:
                # try to extract flags and voted-leader if present
                flags = None
                voted = None
                if isinstance(masters, list) and len(masters) > 0:
                    m = masters[0]
                    # find flags
                    for i in range(0, len(m) - 1, 2):
                        if m[i] == b'flags' and i + 1 < len(m):
                            flags = m[i + 1].decode() if isinstance(m[i + 1], bytes) else str(m[i + 1])
                            break
                if isinstance(sent_list, list) and len(sent_list) > 0:
                    s0 = sent_list[0]
                    for i in range(0, len(s0) - 1, 2):
                        if s0[i] == b'voted-leader' and i + 1 < len(s0):
                            voted = s0[i + 1].decode() if isinstance(s0[i + 1], bytes) else str(s0[i + 1])
                            break
            except Exception:
                flags = None
                voted = None

            logging.info("Sentinel %s:%s flags=%s voted=%s", h, p, flags, voted)
        except Exception as e:
            logging.debug("Sentinel %s:%s unreachable: %s", h, p, e)
            logging.info("Sentinel %s:%s unreachable", h, p)


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
    setup_logging()
    logging.info("=== FP Scenario 2: Redis Sentinel Failover Test (CLEAN) ===")
    logging.info("-----------------------------------------------------------")

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

    # 3) trigger failover manually (stop current master container) or optionally automated
    logging.info("\n[Phase 2] ACTION: stop current master container to trigger failover")
    if AUTO_STOP_MASTER:
        # start a background thread to stop master after configured delay
        t = threading.Thread(target=delayed_stop_master, args=(AUTO_STOP_DELAY_SECONDS,), daemon=True)
        t.start()
    else:
        logging.info("Example: docker stop redis-master")

    logging.info("Monitoring... (Ctrl+C to stop)\n")

    # capture sentinel snapshots pre-monitoring
    pre_snaps = capture_sentinel_snapshots(SENTINELS)
    logging.info("Captured pre-failover sentinel snapshots: %s", pre_snaps)

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
                            logging.info("🚨 FAILOVER FOUND! new_master=%s (t=%.2f s)", cur, dt)
                            # capture sentinel state immediately when change observed
                            log_sentinels_during_election(SENTINELS)
                            break

                        # keep output clean; log occasional heartbeat
                        if last_master and cur == last_master:
                            sys.stdout.write("-")
                            sys.stdout.flush()
                            # occasionally log a heartbeat
                            if int(time.time() - t_failover_start) % 5 == 0:
                                logging.info("Monitoring: master still=%s", cur)

                        time.sleep(0.5)

            except MasterNotFoundError:
                logging.info("Master not found! (voting in progress?)")
                # capture per-sentinel detailed view during election
                log_sentinels_during_election(SENTINELS)
                time.sleep(0.5)

            except Exception as e:
                logging.exception("Unexpected error while polling sentinel: %s", e)
                sys.stdout.write("-")
                sys.stdout.flush()
                # also query sentinels to capture context
                log_sentinels_during_election(SENTINELS)
                time.sleep(1)

    except KeyboardInterrupt:
        print("\nMonitoring stopped.")
        return

    # 4) after failover, write AGAIN to the new master (via host ports scan)
    # capture sentinel snapshots right after failover detection
    post_snaps = capture_sentinel_snapshots(SENTINELS)
    print("[Info] Captured post-failover sentinel snapshots")

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
