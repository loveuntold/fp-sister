import time
import sys
import subprocess
import threading
import logging
import os
import redis
from redis.sentinel import Sentinel, MasterNotFoundError

# Sentinel ports mapped to host (docker-compose)
SENTINELS = [
    ("localhost", 26379),
    ("localhost", 26380),
    ("localhost", 26381),
]

SERVICE_NAME = "init_master" 
PUBLISHED_PORTS = {
    "redis-master": 6379,
    "redis-replica-1": 6380,
    "redis-replica-2": 6381,
    "redis-replica-3": 6382,
    "redis-replica-4": 6383,
}

CANDIDATE_WRITE_PORTS = [6379, 6380, 6381, 6382, 6383]

WRITE_BEFORE = 2000
WRITE_AFTER = 2000
KEY_PREFIX = "fp_s2_"

SENTINEL_SOCKET_TIMEOUT = 0.2
REDIS_SOCKET_TIMEOUT = 1.0

AUTO_STOP_MASTER = False
AUTO_STOP_DELAY_SECONDS = 10

LOG_FILE = "sentinel-failover.log"

DOCKER_SENTINEL_CONTAINERS = [
    "redis-sentinel-1",
    "redis-sentinel-2",
    "redis-sentinel-3",
]

DOCKER_REPLICA_CONTAINERS = [
    "redis-replica-1",
    "redis-replica-2",
    "redis-replica-3",
    "redis-replica-4",
]

DOCKER_MASTER_CONTAINER = "redis-master"

LOGDIR = "logs"

def ensure_logdir():
    os.makedirs(LOGDIR, exist_ok=True)

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(LOG_FILE, mode="w")
    fh.setLevel(logging.DEBUG)
    fh_fmt = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    fh.setFormatter(fh_fmt)
    logger.addHandler(fh)

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


def capture_docker_logs_for_sentinels():
    """Capture `docker logs` for sentinel, master and replica containers and write to separate files (overwrite each run)."""
    ensure_logdir()
    all_names = []
    all_names.extend(DOCKER_SENTINEL_CONTAINERS)
    all_names.append(DOCKER_MASTER_CONTAINER)
    all_names.extend(DOCKER_REPLICA_CONTAINERS)

    for name in all_names:
        outpath = os.path.join(LOGDIR, f"{name}.log")
        try:
            p = subprocess.run(["docker", "logs", name], capture_output=True, text=True, check=False)
            with open(outpath, "w") as f:
                f.write(f"# docker logs for {name} captured at {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n")
                f.write(p.stdout or "")
                if p.stderr:
                    f.write("\n# STDERR:\n")
                    f.write(p.stderr)

        except Exception as e:
            logging.debug("Failed to capture docker logs for %s: %s", name, e)


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

            logging.debug("Sentinel %s:%s raw masters=%s sentinels=%s", h, p, masters, sent_list)

            try:
                flags = None
                voted = None
                if isinstance(masters, list) and len(masters) > 0:
                    m = masters[0]
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


def background_writer(start_idx, total_count, interval, stop_event, result_dict):
    """Continuously attempt to write keys to whichever published host-port is the current master.
    This will retry on connection errors so writes continue across failover.
    Writes keys [start_idx .. start_idx+total_count-1].
    """
    i = start_idx
    written = 0
    t0 = time.time()
    while written < total_count and not stop_event.is_set():
        try:
            r, port = connect_writable_master_via_host_ports()
            r.set(f"{KEY_PREFIX}{i}", f"{time.time()}_{i}")
            i += 1
            written += 1
        except Exception:
            time.sleep(0.05)
            continue
        if interval:
            time.sleep(interval)

    result_dict["written"] = written
    result_dict["duration"] = time.time() - t0
    return


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
    r, port = connect_writable_master_via_host_ports()
    keys = [f"{KEY_PREFIX}{i}" for i in range(1, total_keys + 1)]
    CHUNK = 5000
    for i in range(0, len(keys), CHUNK):
        r.delete(*keys[i : i + CHUNK])
    return port


def main():
    ensure_logdir()
    capture_docker_logs_for_sentinels()

    print("\n=== FP Scenario 2: Redis Sentinel Failover Test ===")
    print("-----------------------------------------------------------")

    sentinel_conn = connect_sentinel()

    try:
        m_ip, m_port = discover_master_ip_port(sentinel_conn)
        print(f"✔ master(before)={m_ip}:{m_port}")
    except MasterNotFoundError:
        logging.info("❌ Master not found from Sentinel (monitoring not ready).")
        return
    except Exception as e:
        logging.info(f"❌ Sentinel connection error: {e}")
        return

    r_master, host_master_port = connect_writable_master_via_host_ports()
    print(f"[Info] writable master (host) = localhost:{host_master_port}")

    print(f"[Phase 1] write {WRITE_BEFORE} keys (before failover)")
    d1 = write_keys(host_master_port, 1, WRITE_BEFORE)
    print(f"✔ write(before) done in {d1:.3f}s")

    missed, checked = missed_count_on_replicas(1, min(200, WRITE_BEFORE))
    if checked > 0:
        print(f"[Phase 1] missed_count(sample@replicas)={missed} (checked_replicas={checked})")
    else:
        print("[Phase 1] missed_count(sample@replicas)=N/A (no replicas reachable on host ports)")

    print("\n[Phase 2] ACTION: stop current master container to trigger failover")
    print("Example:")
    print("  docker stop redis-master")
    if AUTO_STOP_MASTER:
        t = threading.Thread(target=delayed_stop_master, args=(AUTO_STOP_DELAY_SECONDS,), daemon=True)
        t.start()

    writer_stop = threading.Event()
    writer_result = {}
    writer_thread = threading.Thread(
        target=background_writer,
        args=(WRITE_BEFORE + 1, WRITE_AFTER, 0.0, writer_stop, writer_result),
        daemon=True,
    )
    writer_thread.start()
    print(f"[Phase 2] background writer started: writing {WRITE_AFTER} keys starting at {WRITE_BEFORE+1}")

    print("Monitoring... (Ctrl+C to stop)\n")

    pre_snaps = capture_sentinel_snapshots(SENTINELS)

    last_master = f"{m_ip}:{m_port}"
    t_failover_start = time.time()
    failover_detected_at = None
    new_master = None
    last_heartbeat = 0.0

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
                    log_sentinels_during_election(SENTINELS)
                    capture_docker_logs_for_sentinels()
                    break

                if last_master and cur == last_master:
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

    post_snaps = capture_sentinel_snapshots(SENTINELS)

    print("\n[Phase 3] waiting for background writer to finish (it continues across failover)")
    writer_thread.join(timeout=300)
    if writer_thread.is_alive():
        print("⚠ background writer still running after timeout; stopping it now.")
        writer_stop.set()
        writer_thread.join()

    written_after = writer_result.get("written", 0)
    duration_after = writer_result.get("duration", 0.0)
    print(f"✔ background write completed: written={written_after} in {duration_after:.3f}s")

    missed2, checked2 = missed_count_on_replicas(WRITE_BEFORE + 1, min(200, WRITE_AFTER))
    if checked2 > 0:
        print(f"[Phase 3] missed_count(sample@replicas)={missed2} (checked_replicas={checked2})")
    else:
        print("[Phase 3] missed_count(sample@replicas)=N/A (no replicas reachable on host ports)")

    print("\n=== SUMMARY ===")
    print(f"- master(before)           : {last_master}")
    print(f"- master(after)            : {new_master}")
    print(f"- failover_detection_time  : {(failover_detected_at - t_failover_start):.2f}s")
    print(f"- write(before)            : {d1:.3f}s")
    print(f"- write(after/background)  : {duration_after:.3f}s (written={written_after})")

    total_keys = WRITE_BEFORE + WRITE_AFTER
    cleaned_on = cleanup_all_keys(total_keys)
    print(f"✔ Cleanup completed (issued via localhost:{cleaned_on})")


if __name__ == "__main__":
    main()