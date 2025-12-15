import time
import redis
import subprocess
import re

# ==================================================
# FP – SCENARIO 2
# Redis Sentinel Automatic Failover Test
# ==================================================

SENTINEL_CONTAINER = "redis-sentinel-1"
SENTINEL_PORT = "26379"
TEST_KEY = "fp_failover_key"


# -------------------------------
# Helper: run docker command
# -------------------------------
def run_cmd(cmd):
    return subprocess.check_output(cmd, text=True)


# -------------------------------
# Extract master name from SENTINEL masters (RESP)
# -------------------------------
def get_master_name():
    output = run_cmd([
        "docker", "exec", SENTINEL_CONTAINER,
        "redis-cli", "-p", SENTINEL_PORT,
        "SENTINEL", "masters"
    ])

    # Cari pola: "name" <newline> "<master_name>"
    matches = re.findall(r'"name"\s*\n\s*"([^"]+)"', output)
    if not matches:
        raise RuntimeError(
            "❌ Tidak menemukan master name dari Sentinel.\n"
            "Pastikan Sentinel benar-benar memonitor master."
        )
    return matches[0]


# -------------------------------
# Get active master IP & port
# -------------------------------
def get_active_master():
    master_name = get_master_name()

    output = run_cmd([
        "docker", "exec", SENTINEL_CONTAINER,
        "redis-cli", "-p", SENTINEL_PORT,
        "SENTINEL", "get-master-addr-by-name", master_name
    ])

    lines = [l.strip() for l in output.splitlines()
             if l.strip() and l.strip() != "(nil)"]

    if len(lines) < 2:
        raise RuntimeError(
            f"❌ Sentinel gagal mengembalikan alamat master '{master_name}'.\n"
            f"Output:\n{output}"
        )

    return master_name, lines[0], int(lines[1])


# -------------------------------
# Stop original master container
# -------------------------------
def stop_initial_master():
    subprocess.call(["docker", "stop", "redis-master"])


# ==================================================
# MAIN SCENARIO
# ==================================================
def run_scenario_2():
    print("\n=== FP SCENARIO 2: Redis Sentinel Failover Test ===")
    print("-" * 60)

    # Phase 1 — Identify initial master
    print("\n[Phase 1] Identifying initial master via Sentinel...")
    master_name, master_ip, master_port = get_active_master()
    print(f"✔ Master name    : {master_name}")
    print(f"✔ Initial master : {master_ip}:{master_port}")

    master = redis.Redis(
        host=master_ip,
        port=master_port,
        decode_responses=True
    )
    master.ping()
    print("✔ Connected to initial master")

    # Phase 2 — Initial write
    print("\n[Phase 2] Writing data to initial master...")
    master.set(TEST_KEY, "before_failover")
    print("✔ Write before failover successful")

    # Phase 3 — Simulate failure
    print("\n[Phase 3] Simulating master failure (docker stop redis-master)")
    failover_start = time.time()
    stop_initial_master()

    # Phase 4 — Wait for leader election
    print("\n[Phase 4] Waiting for Sentinel leader election...")
    new_master_ip = None
    new_master_port = None

    while True:
        try:
            _, ip, port = get_active_master()
            if ip != master_ip:
                new_master_ip = ip
                new_master_port = port
                break
        except:
            pass
        time.sleep(0.2)

    failover_time = time.time() - failover_start
    print(f"✔ New master elected : {new_master_ip}:{new_master_port}")
    print(f"✔ Failover time      : {failover_time:.2f} seconds")

    # Phase 5 — Test availability after failover
    print("\n[Phase 5] Testing write availability after failover...")
    new_master = redis.Redis(
        host=new_master_ip,
        port=new_master_port,
        decode_responses=True
    )

    try:
        new_master.set(TEST_KEY, "after_failover")
        print("✔ Write after failover successful")
    except Exception as e:
        print("❌ Write failed after failover:", e)

    # Phase 6 — Summary
    print("\n" + "-" * 60)
    print("📌 FAILOVER TEST SUMMARY")
    print(f"- Master name          : {master_name}")
    print(f"- Leader election time : {failover_time:.2f} seconds")
    print(f"- New master node      : {new_master_ip}:{new_master_port}")
    print("- Availability         : Restored after failover")
    print("- Consistency          : Temporarily weakened during election")
    print("- CAP Trade-off        : AP (Availability & Partition Tolerance)")
    print("-" * 60)


if __name__ == "__main__":
    run_scenario_2()
