import subprocess
import time
from collections import defaultdict

BOOTSTRAP_CONTAINER = "redis-node-1"
BOOTSTRAP_PORT = 7000

KEY_PREFIX = "key"
KEY_START = 0
KEY_END = 10000  # inclusive
CLEANUP = True


def sh(cmd, inp=None):
    if inp is None:
        return subprocess.check_output(cmd, text=True)
    p = subprocess.run(cmd, input=inp, text=True, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed ({p.returncode}).\nSTDERR:\n{p.stderr}\nSTDOUT:\n{p.stdout}")
    return p.stdout


def rcli(container, port, *args):
    base = ["docker", "exec", "-i", container, "redis-cli", "-p", str(port)]
    return sh(base + list(args)).strip()


def verify_cluster_ok():
    info = rcli(BOOTSTRAP_CONTAINER, BOOTSTRAP_PORT, "CLUSTER", "INFO")
    if "cluster_state:ok" not in info:
        print("❌ Cluster not ready:\n", info)
        raise SystemExit(1)
    print("✔ cluster_state:ok")


def port_to_container(port: int) -> str:
    if 7000 <= port <= 7005:
        return f"redis-node-{(port - 6999)}"
    return BOOTSTRAP_CONTAINER


def parse_master_slot_ranges(raw_nodes: str):
    ranges = []
    for line in raw_nodes.splitlines():
        parts = line.strip().split()
        if len(parts) < 9:
            continue
        flags = parts[2]
        if "master" not in flags:
            continue
        if "fail" in flags or "handshake" in flags or "noaddr" in flags:
            continue

        addr = parts[1].split("@")[0]
        ip, port_str = addr.split(":")
        port = int(port_str)

        slot_token = None
        for tok in parts[8:]:
            if "-" in tok and tok[0].isdigit():
                slot_token = tok
                break
        if not slot_token:
            continue

        s, e = slot_token.split("-", 1)
        ranges.append((int(s), int(e), ip, port))

    if not ranges:
        raise RuntimeError("Failed to parse master slot ranges from CLUSTER NODES.")
    return sorted(ranges, key=lambda x: x[0])


def slot_owner(slot: int, slot_ranges):
    for s, e, ip, port in slot_ranges:
        if s <= slot <= e:
            return ip, port
    return "UNKNOWN", -1


def build_slot_table(slot_ranges):
    """
    Precompute slot->(ip,port) for all 16384 slots once.
    Then key routing becomes O(1).
    """
    table = [("UNKNOWN", -1)] * 16384
    for s, e, ip, port in slot_ranges:
        for slot in range(s, e + 1):
            table[slot] = (ip, port)
    return table


def bulk_write_fast(slot_table):
    total = KEY_END - KEY_START + 1
    print(f"\n[Phase 1] Bulk write {total} keys (GROUPED + --pipe, fast)")

    # group payload per master_port
    payload_by_port = defaultdict(list)

    # compute slot locally (CRC16 used by Redis Cluster)
    # We'll ask redis for KEYSLOT in batch using one redis-cli --pipe to avoid reimplementing CRC16.
    # Trick: send many "CLUSTER KEYSLOT key" through one pipe, then parse outputs.
    keys = [f"{KEY_PREFIX}{i}" for i in range(KEY_START, KEY_END + 1)]
    cmd_lines = "".join([f"CLUSTER KEYSLOT {k}\n" for k in keys])
    out = sh(["docker", "exec", "-i", BOOTSTRAP_CONTAINER, "redis-cli", "-p", str(BOOTSTRAP_PORT), "--raw"], inp=cmd_lines)
    slots = [int(x) for x in out.splitlines() if x.strip().isdigit()]

    if len(slots) != len(keys):
        raise RuntimeError(f"Slot count mismatch. keys={len(keys)} slots={len(slots)}")

    for k, slot in zip(keys, slots):
        ip, master_port = slot_table[slot]
        if master_port == -1:
            raise RuntimeError(f"Cannot find owner for slot {slot}")
        val = f"v{k[len(KEY_PREFIX):]}"
        payload_by_port[master_port].append(f"SET {k} {val}\n")

    t0 = time.time()
    for master_port, lines in sorted(payload_by_port.items()):
        container = port_to_container(master_port)
        payload = "".join(lines)
        sh(["docker", "exec", "-i", container, "redis-cli", "-p", str(master_port), "--pipe"], inp=payload)
    dur = time.time() - t0
    print(f"✔ Write finished in {dur:.3f}s (~{int(total/dur)} ops/sec)")


def analyze_distribution(slot_table):
    total = KEY_END - KEY_START + 1
    counts = defaultdict(int)
    samples = defaultdict(list)

    print("\n[Phase 2] Distribution analysis (key -> slot -> master)")

    # reuse the same batch slot computation
    keys = [f"{KEY_PREFIX}{i}" for i in range(KEY_START, KEY_END + 1)]
    cmd_lines = "".join([f"CLUSTER KEYSLOT {k}\n" for k in keys])
    out = sh(["docker", "exec", "-i", BOOTSTRAP_CONTAINER, "redis-cli", "-p", str(BOOTSTRAP_PORT), "--raw"], inp=cmd_lines)
    slots = [int(x) for x in out.splitlines() if x.strip().isdigit()]

    for k, slot in zip(keys, slots):
        ip, port = slot_table[slot]
        master = f"{ip}:{port}"
        counts[master] += 1
        if len(samples[master]) < 5:
            samples[master].append(f"{k}(slot={slot})")

    print("\n=== SHARDING RESULT SUMMARY ===")
    for master, c in sorted(counts.items()):
        pct = (c / total) * 100
        print(f"- Master {master} -> {c} keys ({pct:.2f}%)")
        print(f"  sample: {', '.join(samples[master])}")

    print("\n[Conclusion]")
    print("- Redis Cluster shards data by hash slots (0-16383).")
    print("- Slot ranges are owned by different masters (shards).")
    print("- Distribution proves partitioning & horizontal scaling.")


def cleanup_fast(slot_table):
    if not CLEANUP:
        return
    total = KEY_END - KEY_START + 1
    print(f"\n[Phase 3] Cleanup {total} keys (GROUPED + --pipe)")

    payload_by_port = defaultdict(list)

    keys = [f"{KEY_PREFIX}{i}" for i in range(KEY_START, KEY_END + 1)]
    cmd_lines = "".join([f"CLUSTER KEYSLOT {k}\n" for k in keys])
    out = sh(["docker", "exec", "-i", BOOTSTRAP_CONTAINER, "redis-cli", "-p", str(BOOTSTRAP_PORT), "--raw"], inp=cmd_lines)
    slots = [int(x) for x in out.splitlines() if x.strip().isdigit()]

    for k, slot in zip(keys, slots):
        ip, master_port = slot_table[slot]
        payload_by_port[master_port].append(f"DEL {k}\n")

    for master_port, lines in sorted(payload_by_port.items()):
        container = port_to_container(master_port)
        payload = "".join(lines)
        sh(["docker", "exec", "-i", container, "redis-cli", "-p", str(master_port), "--pipe"], inp=payload)

    print("✔ Cleanup done")


def main():
    print("\n=== FP SCENARIO 3: Redis Cluster Sharding Test (FAST FINAL) ===")
    print("------------------------------------------------------------")

    verify_cluster_ok()

    raw_nodes = rcli(BOOTSTRAP_CONTAINER, BOOTSTRAP_PORT, "CLUSTER", "NODES")
    slot_ranges = parse_master_slot_ranges(raw_nodes)

    print("\n[Phase 0] Master slot ranges detected (from CLUSTER NODES):")
    for s, e, ip, port in slot_ranges:
        print(f"  - {s}-{e} -> {ip}:{port}")

    slot_table = build_slot_table(slot_ranges)

    bulk_write_fast(slot_table)
    analyze_distribution(slot_table)
    cleanup_fast(slot_table)


if __name__ == "__main__":
    main()
