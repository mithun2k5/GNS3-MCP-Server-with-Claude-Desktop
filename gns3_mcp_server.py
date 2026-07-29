#!/usr/bin/env python3
"""
gns3_mcp_server.py — an MCP server that exposes a GNS3 lab as tools.

Works with GNS3 v2.2 (HTTP basic auth, /v2 API) and GNS3 v3 (JWT bearer, /v3 API).
Set GNS3_API_VERSION to "v2" or "v3".

Env vars:
  GNS3_URL           default http://127.0.0.1:3080
  GNS3_API_VERSION   v2 | v3          (default v3)
  GNS3_USER          username
  GNS3_PASSWORD      password
  GNS3_PROJECT       project name or UUID (optional default)
"""

import asyncio
import os
import telnetlib3
import httpx
from mcp.server.fastmcp import FastMCP

GNS3_URL = os.getenv("GNS3_URL", "http://127.0.0.1:3080").rstrip("/")
API_VER = os.getenv("GNS3_API_VERSION", "v3")
USER = os.getenv("GNS3_USER", "admin")
PASSWORD = os.getenv("GNS3_PASSWORD", "")
DEFAULT_PROJECT = os.getenv("GNS3_PROJECT")

BASE = f"{GNS3_URL}/{API_VER}"
mcp = FastMCP("gns3-lab")

_token: str | None = None


async def _client() -> httpx.AsyncClient:
    """Return an authenticated httpx client for either API version."""
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
        return httpx.AsyncClient(
            timeout=30, headers={"Authorization": f"Bearer {_token}"}
        )
    return httpx.AsyncClient(timeout=30, auth=(USER, PASSWORD))


async def _get(path: str):
    async with await _client() as c:
        r = await c.get(f"{BASE}{path}")
        r.raise_for_status()
        return r.json()


async def _post(path: str, payload: dict | None = None):
    async with await _client() as c:
        r = await c.post(f"{BASE}{path}", json=payload or {})
        r.raise_for_status()
        return r.json() if r.text else {"ok": True}


async def _resolve_project(project: str | None) -> str:
    """Accept a project name or UUID, return the UUID."""
    project = project or DEFAULT_PROJECT
    if not project:
        raise ValueError("No project given and GNS3_PROJECT is unset.")
    if len(project) == 36 and project.count("-") == 4:
        return project
    for p in await _get("/projects"):
        if p["name"] == project:
            return p["project_id"]
    raise ValueError(f"Project '{project}' not found.")


# ---------------------------------------------------------------- tools

@mcp.tool()
async def list_projects() -> list[dict]:
    """List GNS3 projects with their status."""
    return [
        {"name": p["name"], "id": p["project_id"], "status": p["status"]}
        for p in await _get("/projects")
    ]


@mcp.tool()
async def open_project(project: str) -> dict:
    """Open a GNS3 project so its nodes can be started. Accepts name or UUID."""
    pid = await _resolve_project(project)
    return await _post(f"/projects/{pid}/open")


@mcp.tool()
async def list_nodes(project: str | None = None) -> list[dict]:
    """List every node in the lab: name, type, status, and console port."""
    pid = await _resolve_project(project)
    return [
        {
            "name": n["name"],
            "id": n["node_id"],
            "type": n["node_type"],
            "status": n["status"],
            "console": n.get("console"),
            "console_host": n.get("console_host"),
        }
        for n in await _get(f"/projects/{pid}/nodes")
    ]


@mcp.tool()
async def topology(project: str | None = None) -> list[str]:
    """Describe the cabling as human-readable 'A port <-> B port' strings."""
    pid = await _resolve_project(project)
    nodes = {n["node_id"]: n["name"] for n in await _get(f"/projects/{pid}/nodes")}
    out = []
    for link in await _get(f"/projects/{pid}/links"):
        ends = [
            f"{nodes.get(p['node_id'], p['node_id'])} "
            f"e{p['adapter_number']}/{p['port_number']}"
            for p in link["nodes"]
        ]
        out.append(" <-> ".join(ends))
    return out


@mcp.tool()
async def node_power(node: str, action: str, project: str | None = None) -> dict:
    """Start, stop, suspend, or reload a node by name. action: start|stop|suspend|reload."""
    if action not in {"start", "stop", "suspend", "reload"}:
        raise ValueError("action must be start, stop, suspend, or reload")
    pid = await _resolve_project(project)
    for n in await _get(f"/projects/{pid}/nodes"):
        if n["name"] == node:
            return await _post(f"/projects/{pid}/nodes/{n['node_id']}/{action}")
    raise ValueError(f"Node '{node}' not found.")


@mcp.tool()
async def run_cli(
    node: str,
    commands: list[str],
    project: str | None = None,
    read_timeout: float = 4.0,
) -> str:
    """
    Run CLI commands on a node over its telnet console and return the output.

    Read-only by design guidance: prefer show commands. The node must be started.
    Send 'terminal length 0' first on Cisco-style devices to avoid pagination.
    """
    pid = await _resolve_project(project)
    target = next(
        (n for n in await _get(f"/projects/{pid}/nodes") if n["name"] == node), None
    )
    if target is None:
        raise ValueError(f"Node '{node}' not found.")
    if target["status"] != "started":
        raise ValueError(f"Node '{node}' is {target['status']}; start it first.")

    # GNS3 often reports 0.0.0.0 or 127.0.0.1 as console_host, which is
    # meaningless from a remote client. Fall back to the controller's host.
    host = target.get("console_host")
    if host in (None, "", "0.0.0.0", "127.0.0.1", "::"):
        host = httpx.URL(GNS3_URL).host
    port = target["console"]

    reader, writer = await telnetlib3.open_connection(host, port, encoding="utf-8")
    buf = []
    try:
        writer.write("\r\n")
        await asyncio.sleep(0.5)
        for cmd in commands:
            writer.write(cmd + "\r\n")
            await writer.drain()
            try:
                chunk = await asyncio.wait_for(reader.read(65536), read_timeout)
                buf.append(chunk)
            except asyncio.TimeoutError:
                pass
    finally:
        writer.close()
    return "".join(buf)


if __name__ == "__main__":
    mcp.run(transport="stdio")
