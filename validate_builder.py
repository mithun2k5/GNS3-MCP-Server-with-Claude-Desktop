#!/usr/bin/env python3
"""
validate_builder.py — prove gns3_builder.py works, without Claude Desktop.

Runs staged checks against the live GNS3 server and prints PASS/FAIL per stage.
Creates a throwaway project and deletes it at the end unless --keep is given.

Usage:
    python validate_builder.py                # stages 1-6 (fast, ~20s)
    python validate_builder.py --full         # + boot, configure, verify OSPF
    python validate_builder.py --full --keep  # leave the project in place
    python validate_builder.py --template "Cisco IOU L3"

Reads the same env vars as gns3_builder.py.
"""

import argparse
import asyncio
import sys
import time

import gns3_builder as b

TEST_PROJECT = f"ZZ-Validate-{int(time.time())}"

def ospf_config(hostname: str, interface: str, ip: str, router_id: str) -> str:
    """
    Build an OSPF config for whatever interface the link actually landed on.

    Interface naming differs by image — IOU uses Ethernet0/0, IOSv uses
    GigabitEthernet0/0 — so this is derived at runtime, never hardcoded.
    """
    return f"""
hostname {hostname}
no ip domain-lookup
interface {interface}
 ip address {ip} 255.255.255.252
 no shutdown
interface Loopback0
 ip address {router_id} 255.255.255.255
router ospf 1
 router-id {router_id}
 network 10.99.99.0 0.0.0.3 area 0
 network {router_id} 0.0.0.0 area 0
"""

results: list[tuple[str, bool, str]] = []


def record(stage: str, ok: bool, detail: str = "") -> bool:
    results.append((stage, ok, detail))
    mark = "PASS" if ok else "FAIL"
    print(f"[{mark}] {stage}" + (f" — {detail}" if detail else ""), flush=True)
    return ok


async def linked_interface(pid: str, node_name: str) -> str:
    """Return the full interface name that this node's link is cabled to."""
    n = await b._find_node(pid, node_name)
    used = await b._used_ports(pid)
    for p in n.get("ports", []):
        if (n["node_id"], p["adapter_number"], p["port_number"]) in used:
            return p.get("name") or p.get("short_name")
    raise RuntimeError(f"{node_name} has no cabled interface")


async def console_cmd(pid: str, node_name: str, cmds: list[str]) -> str:
    """Minimal console reader, so validation doesn't depend on the other server."""
    import telnetlib3

    n = await b._find_node(pid, node_name)
    reader, writer = await telnetlib3.open_connection(
        b._console_host(n), n["console"], encoding="utf-8"
    )
    out = ""
    try:
        out += await b._reach_priv_exec(reader, writer, timeout=120)
        await b._send(writer, "terminal length 0")
        await b._read_until(reader, [b.P_PRIV], 8)
        for c in cmds:
            await b._send(writer, c)
            chunk, _ = await b._read_until(reader, [b.P_PRIV], 15)
            out += chunk
    finally:
        writer.close()
    return out


async def main(args) -> int:
    print(f"\nTarget : {b.GNS3_URL} ({b.API_VER})")
    print(f"Project: {TEST_PROJECT}\n")

    pid = None

    # ---- Stage 1: API reachable ----------------------------------------
    try:
        ver = await b._get("/version")
        record("1. API reachable", True, f"GNS3 {ver.get('version')}")
    except Exception as e:
        record("1. API reachable", False, str(e))
        print("\nCannot continue without API access. Check GNS3_URL and the port.")
        return 1

    # ---- Stage 2: templates --------------------------------------------
    template = args.template
    try:
        tpls = await b.list_templates()
        names = [t["name"] for t in tpls]
        record("2. Templates listed", bool(tpls), f"{len(tpls)} found")

        if template:
            if template not in names:
                record("2b. Requested template exists", False,
                       f"'{template}' not in: {', '.join(names[:8])}")
                return 1
        else:
            routerish = [
                t["name"] for t in tpls
                if t.get("category") == "router"
                or any(k in t["name"].lower() for k in ("l3", "router", "ios", "vios"))
            ]
            template = (routerish or names)[0]
        record("2c. Template selected", True, template)
    except Exception as e:
        record("2. Templates listed", False, str(e))
        return 1

    try:
        # ---- Stage 3: create project ------------------------------------
        proj = await b.create_project(TEST_PROJECT)
        pid = proj["id"]
        record("3. Project created", True, pid)

        # ---- Stage 4: add nodes -----------------------------------------
        n1 = await b.add_node(template=template, name="VR1", project=pid, x=-200, y=0)
        n2 = await b.add_node(template=template, name="VR2", project=pid, x=200, y=0)
        ok = n1["name"] == "VR1" and n2["name"] == "VR2"
        record("4. Two nodes added", ok, f"{n1['name']}, {n2['name']}")
        if not n1["ports"]:
            record("4b. Node reports ports", False,
                   "no ports — QEMU nodes may need a moment; rerun if this persists")
        else:
            record("4b. Node reports ports", True, f"{len(n1['ports'])} on VR1")

        # ---- Stage 5: link ----------------------------------------------
        link = await b.connect("VR1", "VR2", project=pid)
        record("5. Auto-port link created", True, link)

        # ---- Stage 6: verify via API ------------------------------------
        links = await b._get(f"/projects/{pid}/links")
        nodes = await b._nodes(pid)
        ok = len(links) == 1 and len(nodes) == 2
        record("6. Topology verified", ok, f"{len(nodes)} nodes, {len(links)} link")

        ports = await b.list_ports("VR1", project=pid)
        in_use = [p for p in ports if p["in_use"]]
        record("6b. Port marked in use", len(in_use) == 1,
               f"{in_use[0]['short_name'] if in_use else 'none'}")

        if not args.full:
            print("\nFast checks done. Add --full to test boot + config + OSPF.")
            return 0

        # ---- Stage 7: boot ----------------------------------------------
        print("\nStarting nodes — this takes 30-90s per device...\n")
        for name in ("VR1", "VR2"):
            n = await b._find_node(pid, name)
            await b._post(f"/projects/{pid}/nodes/{n['node_id']}/start")
        await asyncio.sleep(15)

        for name in ("VR1", "VR2"):
            try:
                await b.wait_for_boot(name, project=pid, timeout=args.boot_timeout)
                record(f"7. {name} booted", True)
            except Exception as e:
                record(f"7. {name} booted", False, str(e)[:160])
                return 1

        # ---- Stage 8: configure -----------------------------------------
        if1 = await linked_interface(pid, "VR1")
        if2 = await linked_interface(pid, "VR2")
        record("8a. Interfaces detected", True, f"VR1 {if1}, VR2 {if2}")

        cfgs = (
            ("VR1", ospf_config("VR1", if1, "10.99.99.1", "1.1.1.1")),
            ("VR2", ospf_config("VR2", if2, "10.99.99.2", "2.2.2.2")),
        )
        for name, cfg in cfgs:
            t = await b.configure_node(name, cfg, project=pid, save=True)
            bad = t.startswith("CONFIG APPLIED WITH ERRORS")
            record(f"8. {name} configured", not bad,
                   t.splitlines()[1] if bad else "no rejected commands")

        # ---- Stage 9: OSPF adjacency ------------------------------------
        print("\nWaiting 45s for OSPF to converge...\n")
        await asyncio.sleep(45)

        out = await console_cmd(pid, "VR1", ["show ip ospf neighbor"])
        record("9. OSPF neighbor FULL", "FULL" in out,
               "adjacency up" if "FULL" in out else "no FULL state seen")

        out = await console_cmd(pid, "VR1", ["show ip route ospf"])
        record("9b. OSPF route learned", "2.2.2.2" in out,
               "2.2.2.2/32 present" if "2.2.2.2" in out else "loopback route missing")

        out = await console_cmd(pid, "VR1", ["ping 2.2.2.2 source 1.1.1.1"])
        good = "!!!!" in out or "Success rate is 100" in out
        record("9c. End-to-end ping", good, "reachable" if good else "ping failed")

        return 0

    finally:
        if pid and not args.keep:
            try:
                await b._delete(f"/projects/{pid}")
                print(f"\nCleaned up {TEST_PROJECT}")
            except Exception as e:
                print(f"\nCleanup failed, delete manually: {TEST_PROJECT} ({e})")
        elif pid:
            print(f"\nLeft project in place: {TEST_PROJECT}")

        passed = sum(1 for _, ok, _ in results if ok)
        print(f"\n{'=' * 52}")
        print(f"  {passed}/{len(results)} checks passed")
        for stage, ok, detail in results:
            if not ok:
                print(f"  FAILED: {stage} — {detail}")
        print(f"{'=' * 52}\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true",
                    help="also boot, configure and verify OSPF (several minutes)")
    ap.add_argument("--keep", action="store_true",
                    help="do not delete the test project afterwards")
    ap.add_argument("--template", help="exact template name to use")
    ap.add_argument("--boot-timeout", type=float, default=240.0)
    args = ap.parse_args()
    sys.exit(asyncio.run(main(args)) or 0)
