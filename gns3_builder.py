#!/usr/bin/env python3
"""
gns3_builder.py — MCP server for BUILDING and CONFIGURING GNS3 labs.

Runs alongside gns3_mcp_server.py as a second, independent MCP server.
Deliberately contains NO tools that overlap with gns3_mcp_server.py:

  this file (gns3-builder):  list_templates, list_ports, create_project,
                             delete_project, add_node, connect,
                             build_topology, wait_for_boot, configure_node
  gns3_mcp_server.py:        list_projects, list_nodes, topology,
                             open_project, node_power, run_cli

Env vars:
  GNS3_URL           e.g. http://192.168.31.136      (no port = 80)
  GNS3_API_VERSION   v2 | v3                          (default v2)
  GNS3_USER / GNS3_PASSWORD
  GNS3_PROJECT       default project name or UUID     (optional)
  GNS3_COMPUTE       compute id for new nodes         (default "local")
  GNS3_ENABLE_PW     enable password if your images ask for one

Requires: mcp[cli] httpx telnetlib3 pyyaml
"""

import asyncio
import os
import re
import yaml
import telnetlib3
import httpx
from mcp.server.fastmcp import FastMCP

GNS3_URL = os.getenv("GNS3_URL", "http://127.0.0.1:3080").rstrip("/")
API_VER = os.getenv("GNS3_API_VERSION", "v2")
USER = os.getenv("GNS3_USER", "admin")
PASSWORD = os.getenv("GNS3_PASSWORD", "")
DEFAULT_PROJECT = os.getenv("GNS3_PROJECT")
COMPUTE = os.getenv("GNS3_COMPUTE", "local")
ENABLE_PW = os.getenv("GNS3_ENABLE_PW", "")

BASE = f"{GNS3_URL}/{API_VER}"
mcp = FastMCP("gns3-builder")

_token: str | None = None

P_SETUP = r"initial configuration dialog\? ?\[yes/no\]:"
P_RETURN = r"Press RETURN to get started"
P_USER = r"[\r\n][\w\-\.]+>\s*$"
P_PRIV = r"[\r\n][\w\-\.]+#\s*$"
P_CONFIG = r"[\r\n][\w\-\.]+\(config[^)]*\)#\s*$"
P_PASSWD = r"[Pp]assword:\s*$"


# ---------------------------------------------------------------- plumbing

async def _client() -> httpx.AsyncClient:
    global _token
    if API_VER == "v3":
        if _token is None:
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.post(
                    f"{BASE}/access/users/login",
                    data={"username": USER, "password": PASSWORD},
                )
                r.raise_for_status()
                _token = r.json()["access_token"]
        return httpx.AsyncClient(timeout=60, headers={"Authorization": f"Bearer {_token}"})
    return httpx.AsyncClient(timeout=60, auth=(USER, PASSWORD))


async def _get(path: str):
    async with await _client() as c:
        r = await c.get(f"{BASE}{path}")
        r.raise_for_status()
        return r.json()


async def _post(path: str, payload: dict | None = None):
    async with await _client() as c:
        r = await c.post(f"{BASE}{path}", json=payload if payload is not None else {})
        if r.status_code >= 400:
            raise RuntimeError(f"POST {path} -> {r.status_code}: {r.text[:400]}")
        return r.json() if r.text else {"ok": True}


async def _put(path: str, payload: dict):
    async with await _client() as c:
        r = await c.put(f"{BASE}{path}", json=payload)
        if r.status_code >= 400:
            raise RuntimeError(f"PUT {path} -> {r.status_code}: {r.text[:400]}")
        return r.json() if r.text else {"ok": True}


async def _delete(path: str):
    async with await _client() as c:
        r = await c.delete(f"{BASE}{path}")
        if r.status_code >= 400:
            raise RuntimeError(f"DELETE {path} -> {r.status_code}: {r.text[:400]}")
        return {"ok": True}


async def _resolve_project(project: str | None) -> str:
    project = project or DEFAULT_PROJECT
    if not project:
        raise ValueError("No project given and GNS3_PROJECT is unset.")
    if len(project) == 36 and project.count("-") == 4:
        return project
    for p in await _get("/projects"):
        if p["name"].lower() == project.lower():
            return p["project_id"]
    names = ", ".join(p["name"] for p in await _get("/projects"))
    raise ValueError(f"Project '{project}' not found. Available: {names}")


async def _nodes(pid: str) -> list[dict]:
    return await _get(f"/projects/{pid}/nodes")


async def _find_node(pid: str, name: str) -> dict:
    nodes = await _nodes(pid)
    for n in nodes:
        if n["name"].lower() == name.lower():
            return n
    raise ValueError(
        f"Node '{name}' not found. Available: {', '.join(n['name'] for n in nodes)}"
    )


def _console_host(node: dict) -> str:
    host = node.get("console_host")
    if host in (None, "", "0.0.0.0", "127.0.0.1", "::"):
        host = httpx.URL(GNS3_URL).host
    return host


async def _used_ports(pid: str) -> set:
    used = set()
    for link in await _get(f"/projects/{pid}/links"):
        for e in link["nodes"]:
            used.add((e["node_id"], e["adapter_number"], e["port_number"]))
    return used


def _match_port(node: dict, wanted: str | None, used: set) -> dict:
    ports = node.get("ports", [])
    if not ports:
        raise ValueError(f"Node '{node['name']}' reports no ports yet.")
    if wanted:
        w = wanted.lower().replace(" ", "")
        for p in ports:
            if w in (p.get("name", "").lower(), p.get("short_name", "").lower()):
                return p
        avail = ", ".join(f"{p.get('name')} ({p.get('short_name')})" for p in ports)
        raise ValueError(f"Port '{wanted}' not on {node['name']}. Available: {avail}")
    for p in ports:
        if (node["node_id"], p["adapter_number"], p["port_number"]) not in used:
            return p
    raise ValueError(f"No free ports left on '{node['name']}'.")


# ---------------------------------------------------------------- console

async def _read_until(reader, patterns: list[str], timeout: float):
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    buf = ""
    while loop.time() < deadline:
        remaining = deadline - loop.time()
        try:
            chunk = await asyncio.wait_for(reader.read(4096), min(remaining, 2.0))
        except asyncio.TimeoutError:
            chunk = ""
        if chunk:
            buf += chunk
            for p in patterns:
                if re.search(p, buf):
                    return buf, p
    return buf, None


async def _send(writer, line: str) -> None:
    writer.write(line + "\r\n")
    await writer.drain()


async def _reach_priv_exec(reader, writer, timeout: float = 60.0) -> str:
    log = ""
    await _send(writer, "")
    for _ in range(8):
        out, hit = await _read_until(
            reader, [P_SETUP, P_RETURN, P_PRIV, P_USER, P_PASSWD], timeout
        )
        log += out
        if hit is None:
            await _send(writer, "")
            continue
        if hit == P_PRIV:
            return log
        if hit == P_SETUP:
            await _send(writer, "no")
        elif hit == P_RETURN:
            await _send(writer, "")
        elif hit == P_USER:
            await _send(writer, "enable")
        elif hit == P_PASSWD:
            await _send(writer, ENABLE_PW)
    raise RuntimeError(
        "Could not reach a '#' prompt. Device may still be booting, or it is not a "
        f"Cisco-style CLI. Last output:\n{log[-600:]}"
    )


# ---------------------------------------------------------------- tools

@mcp.tool()
async def list_templates() -> list[dict]:
    """
    List device templates available on this GNS3 server.

    ALWAYS call this before building a topology, so node templates match what
    actually exists. Use the exact 'name' value when adding nodes.
    """
    return [
        {"name": t["name"], "type": t.get("template_type"), "category": t.get("category")}
        for t in await _get("/templates")
    ]


@mcp.tool()
async def list_ports(node: str, project: str | None = None) -> list[dict]:
    """Show a node's interfaces and whether each one is already cabled."""
    pid = await _resolve_project(project)
    n = await _find_node(pid, node)
    used = await _used_ports(pid)
    return [
        {
            "name": p.get("name"),
            "short_name": p.get("short_name"),
            "in_use": (n["node_id"], p["adapter_number"], p["port_number"]) in used,
        }
        for p in n.get("ports", [])
    ]


@mcp.tool()
async def create_project(name: str) -> dict:
    """Create a new empty GNS3 project. Use the user's requested name verbatim."""
    for p in await _get("/projects"):
        if p["name"].lower() == name.lower():
            raise ValueError(f"A project named '{p['name']}' already exists.")
    proj = await _post("/projects", {"name": name})
    return {"name": proj["name"], "id": proj["project_id"], "status": proj["status"]}


@mcp.tool()
async def delete_project(name: str, confirm: bool = False) -> dict:
    """
    Permanently delete a project. Requires confirm=True.

    Destructive and irreversible. Always ask the user before calling this.
    """
    if not confirm:
        raise ValueError("Refusing to delete without confirm=True.")
    pid = await _resolve_project(name)
    await _delete(f"/projects/{pid}")
    return {"deleted": name}


@mcp.tool()
async def add_node(
    template: str,
    name: str | None = None,
    project: str | None = None,
    x: int = 0,
    y: int = 0,
) -> dict:
    """Add one node from a device template. 'template' must match list_templates."""
    pid = await _resolve_project(project)
    tpl = None
    for t in await _get("/templates"):
        if t["name"].lower() == template.lower():
            tpl = t
            break
    if tpl is None:
        avail = ", ".join(t["name"] for t in await _get("/templates"))
        raise ValueError(f"Template '{template}' not found. Available: {avail}")
    node = await _post(
        f"/projects/{pid}/templates/{tpl['template_id']}",
        {"x": x, "y": y, "compute_id": tpl.get("compute_id") or COMPUTE},
    )
    if name and node.get("name") != name:
        node = await _put(f"/projects/{pid}/nodes/{node['node_id']}", {"name": name})
    return {
        "name": node["name"],
        "id": node["node_id"],
        "type": node["node_type"],
        "ports": [p.get("short_name") or p.get("name") for p in node.get("ports", [])],
    }


@mcp.tool()
async def connect(
    node_a: str,
    node_b: str,
    port_a: str | None = None,
    port_b: str | None = None,
    project: str | None = None,
) -> str:
    """Cable two nodes. Omit ports to auto-pick the first free interface on each."""
    pid = await _resolve_project(project)
    a = await _find_node(pid, node_a)
    b = await _find_node(pid, node_b)
    used = await _used_ports(pid)
    pa = _match_port(a, port_a, used)
    used.add((a["node_id"], pa["adapter_number"], pa["port_number"]))
    pb = _match_port(b, port_b, used)
    await _post(
        f"/projects/{pid}/links",
        {
            "nodes": [
                {
                    "node_id": a["node_id"],
                    "adapter_number": pa["adapter_number"],
                    "port_number": pa["port_number"],
                },
                {
                    "node_id": b["node_id"],
                    "adapter_number": pb["adapter_number"],
                    "port_number": pb["port_number"],
                },
            ]
        },
    )
    return (
        f"{a['name']} {pa.get('short_name') or pa.get('name')} <-> "
        f"{b['name']} {pb.get('short_name') or pb.get('name')}"
    )


@mcp.tool()
async def wait_for_boot(
    node: str, project: str | None = None, timeout: float = 180.0
) -> str:
    """
    Wait until a node's console reaches a privileged-exec prompt.

    Cisco images need 30-90 seconds after starting before they accept commands.
    Call this before configure_node if the node was only just powered on.
    """
    pid = await _resolve_project(project)
    n = await _find_node(pid, node)
    if n["status"] != "started":
        raise ValueError(f"Node '{node}' is {n['status']}; start it first.")
    reader, writer = await telnetlib3.open_connection(
        _console_host(n), n["console"], encoding="utf-8"
    )
    try:
        await _reach_priv_exec(reader, writer, timeout=timeout)
        return f"{node} is booted and at a privileged-exec prompt."
    finally:
        writer.close()


@mcp.tool()
async def configure_node(
    node: str,
    config: str,
    project: str | None = None,
    save: bool = True,
    mode: str = "cisco",
    boot_timeout: float = 180.0,
) -> str:
    """
    Push configuration lines to a node through its console.

    'config' is the config body as it appears in a config file. Do NOT include
    'configure terminal' or 'end' — those are added automatically.

    mode='cisco' handles the setup dialog, enable, conf t and end.
    mode='raw' sends lines verbatim, for VyOS, Linux hosts or other CLIs.

    Returns the console transcript. Any line starting with '%' is surfaced at
    the top — always show those to the user, they indicate rejected commands.
    """
    pid = await _resolve_project(project)
    n = await _find_node(pid, node)
    if n["status"] != "started":
        raise ValueError(f"Node '{node}' is {n['status']}; start it first.")

    lines = [ln.rstrip() for ln in config.splitlines() if ln.strip()]
    if not lines:
        raise ValueError("No configuration lines given.")

    reader, writer = await telnetlib3.open_connection(
        _console_host(n), n["console"], encoding="utf-8"
    )
    transcript = ""
    try:
        if mode == "cisco":
            transcript += await _reach_priv_exec(reader, writer, timeout=boot_timeout)
            await _send(writer, "terminal length 0")
            out, _ = await _read_until(reader, [P_PRIV], 10)
            transcript += out
            await _send(writer, "configure terminal")
            out, _ = await _read_until(reader, [P_CONFIG], 15)
            transcript += out
            if not re.search(P_CONFIG, out):
                raise RuntimeError(
                    f"Device did not enter config mode. Transcript:\n{transcript[-600:]}"
                )
        for ln in lines:
            await _send(writer, ln)
            out, _ = await _read_until(reader, [P_CONFIG, P_PRIV], 6)
            transcript += out
        if mode == "cisco":
            await _send(writer, "end")
            out, _ = await _read_until(reader, [P_PRIV], 10)
            transcript += out
            if save:
                await _send(writer, "write memory")
                out, _ = await _read_until(reader, [P_PRIV], 30)
                transcript += out
    finally:
        writer.close()

    errors = [l.strip() for l in transcript.splitlines() if l.strip().startswith("%")]
    if errors:
        return "CONFIG APPLIED WITH ERRORS:\n" + "\n".join(errors) + "\n\n" + transcript
    return transcript


@mcp.tool()
async def build_topology(spec: str, start: bool = False, configure: bool = False) -> dict:
    """
    Build a complete project from a YAML spec in one call.

    Preferred way to create a lab from a description: call list_templates first,
    translate the user's request into this YAML, then pass it here.

    project: OSPF-Lab
    nodes:
      - name: R1
        template: Cisco IOU L3
        x: -200
        config: |
          hostname R1
          interface Ethernet0/0
           ip address 10.0.0.1 255.255.255.252
           no shutdown
      - name: R2
        template: Cisco IOU L3
        x: 200
        config: |
          hostname R2
          interface Ethernet0/0
           ip address 10.0.0.2 255.255.255.252
           no shutdown
    links:
      - [R1, R2]

    Omit the port after ':' in links to auto-pick a free interface.
    start=True powers nodes on. configure=True also waits for boot and pushes
    each node's 'config' block — allow several minutes for that.
    """
    try:
        data = yaml.safe_load(spec)
    except yaml.YAMLError as e:
        raise ValueError(f"Spec is not valid YAML: {e}")
    if not isinstance(data, dict):
        raise ValueError("Spec must be a YAML mapping with 'project' and 'nodes'.")

    pname = data.get("project")
    if not pname:
        raise ValueError("Spec needs a 'project' name.")
    node_specs = data.get("nodes") or []
    if not node_specs:
        raise ValueError("Spec needs at least one node under 'nodes'.")

    created = await create_project(pname)
    pid = created["id"]
    report: dict = {
        "project": pname,
        "project_id": pid,
        "nodes": [],
        "links": [],
        "configured": [],
        "errors": [],
    }

    for i, ns in enumerate(node_specs):
        try:
            n = await add_node(
                template=ns["template"],
                name=ns.get("name"),
                project=pid,
                x=int(ns.get("x", -300 + i * 200)),
                y=int(ns.get("y", 0)),
            )
            report["nodes"].append(n["name"])
        except Exception as e:
            report["errors"].append(f"node {ns.get('name', i)}: {e}")

    def _split(ep):
        ep = str(ep)
        return (ep.split(":", 1)[0], ep.split(":", 1)[1]) if ":" in ep else (ep, None)

    for ls in data.get("links") or []:
        try:
            ends = ls.get("endpoints", []) if isinstance(ls, dict) else list(ls)
            if len(ends) != 2:
                raise ValueError("a link needs exactly two endpoints")
            (an, ap), (bn, bp) = _split(ends[0]), _split(ends[1])
            report["links"].append(await connect(an, bn, ap, bp, project=pid))
        except Exception as e:
            report["errors"].append(f"link {ls}: {e}")

    if start or configure:
        for name in report["nodes"]:
            try:
                n = await _find_node(pid, name)
                await _post(f"/projects/{pid}/nodes/{n['node_id']}/start")
            except Exception as e:
                report["errors"].append(f"start {name}: {e}")

    if configure:
        await asyncio.sleep(20)
        for ns in node_specs:
            cfg, name = ns.get("config"), ns.get("name")
            if not cfg or name not in report["nodes"]:
                continue
            try:
                await configure_node(name, cfg, project=pid, save=True)
                report["configured"].append(name)
            except Exception as e:
                report["errors"].append(f"config {name}: {e}")

    return report


if __name__ == "__main__":
    mcp.run(transport="stdio")
