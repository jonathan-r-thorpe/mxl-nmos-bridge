#!/usr/bin/env python3

import argparse
import json
import subprocess
import time
from datetime import datetime
from pathlib import Path

import requests

BASE_URL = "http://localhost:7000/x-nmos/connection/v1.2"
RESOURCE_TYPES = ["senders", "receivers"]
MXL_TRANSPORT = "urn:x-nmos:transport:mxl"

sink_processes: dict[str, subprocess.Popen] = {}
source_processes: dict[str, subprocess.Popen] = {}
sender_flow_ids: dict[str, str] = {}
MXL_DOMAIN = Path.home() / "mxl_domain"


def _timestamp() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def fetch_json(url: str, timeout: float = 2.0) -> list | dict | None:
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        print(f"  [{_timestamp()}] ERROR polling {url}: {exc}")
        return None


def discover_resources(base_url: str) -> dict[str, list[str]]:
    """Query the senders and receivers list endpoints, filtered to MXL transport."""
    resources: dict[str, list[str]] = {}
    for rtype in RESOURCE_TYPES:
        data = fetch_json(f"{base_url}/single/{rtype}")
        if data is None:
            resources[rtype] = []
            continue
        ids = []
        for entry in data:
            rid = entry.rstrip("/")
            ttype = fetch_json(f"{base_url}/single/{rtype}/{rid}/transporttype")
            if ttype == MXL_TRANSPORT:
                ids.append(rid)
        resources[rtype] = ids
    return resources


def monitor(
    base_url: str,
    poll_interval: float = 1.0,
    rediscover_every: int = 30,
) -> None:
    active_state: dict[str, bool | None] = {}
    node_url = base_url.replace("/x-nmos/connection/v1.2", "/x-nmos/node/v1.3")
    polls_since_discovery = rediscover_every  # force initial discovery

    print(f"[{_timestamp()}] Monitoring NMOS IS-05 MXL active endpoints at {base_url}")
    print(f"  Poll interval: {poll_interval}s | Re-discovery every {rediscover_every} polls")
    print()

    while True:
        if polls_since_discovery >= rediscover_every:
            resources = discover_resources(base_url)
            for rtype, ids in resources.items():
                print(f"  [{_timestamp()}] Discovered {len(ids)} {rtype}")
            active_state = {
                f"{rtype}/{rid}": active_state.get(f"{rtype}/{rid}")
                for rtype, ids in resources.items()
                for rid in ids
            }
            polls_since_discovery = 0

        for key in list(active_state.keys()):
            url = f"{base_url}/single/{key}/active"
            data = fetch_json(url)
            if data is None:
                continue

            master_enable = data.get("master_enable", False)
            prev = active_state[key]

            if prev is None:
                state = "ACTIVE" if master_enable else "INACTIVE"
                print(f"  [{_timestamp()}] {key:<60} initial state: {state}")
            elif master_enable and not prev:
                print(f"  [{_timestamp()}] {key:<60} became ACTIVE")
            elif not master_enable and prev:
                print(f"  [{_timestamp()}] {key:<60} became INACTIVE")

            if master_enable and not prev:
                flow_id = data.get("transport_params", [{}])[0].get("flow_id")
                print(f"  [{_timestamp()}] {key:<60} flow_id: {flow_id}")

                if key.startswith("senders/") and flow_id:
                    sender_flow_ids[key] = flow_id
                    flow_data = fetch_json(f"{node_url}/flows/{flow_id}")
                    if flow_data:
                        resource_data = fetch_json(f"{node_url}/{key}")
                        if resource_data:
                            flow_data["tags"] = resource_data.get("tags", {})
                        if flow_data.get("format") == "urn:x-nmos:format:audio":
                            source_id = flow_data.get("source_id")
                            if source_id:
                                source_data = fetch_json(f"{node_url}/sources/{source_id}")
                                if source_data:
                                    flow_data["channel_count"] = len(source_data.get("channels", []))
                        flow_path = MXL_DOMAIN / f"{flow_id}.json"
                        flow_path.write_text(json.dumps(flow_data, indent=2))
                        print(f"  [{_timestamp()}] {key:<60} wrote {flow_path}")
                        flow_flag = "-a" if flow_data.get("format") == "urn:x-nmos:format:audio" else "-v"
                        proc = subprocess.Popen(
                            [str(Path.home() / "projects/mxl/build/Linux-GCC-Debug/tools/mxl-gst/mxl-gst-testsrc"),
                             "-d", str(MXL_DOMAIN), flow_flag, str(flow_path)],
                        )
                        source_processes[key] = proc
                        print(f"  [{_timestamp()}] {key:<60} launched mxl-gst-testsrc (pid {proc.pid})")

                if key.startswith("receivers/") and flow_id:
                    proc = subprocess.Popen(
                        [str(Path.home() / "projects/mxl/build/Linux-GCC-Debug/tools/mxl-gst/mxl-gst-sink"),
                         "-d", str(MXL_DOMAIN), "-v", flow_id],
                    )
                    sink_processes[key] = proc
                    print(f"  [{_timestamp()}] {key:<60} launched mxl-gst-sink (pid {proc.pid})")

            if not master_enable and prev:
                if key.startswith("senders/") and key in sender_flow_ids:
                    if key in source_processes:
                        source_processes[key].terminate()
                        print(f"  [{_timestamp()}] {key:<60} terminated mxl-gst-testsrc")
                        del source_processes[key]
                    flow_path = MXL_DOMAIN / f"{sender_flow_ids[key]}.json"
                    flow_path.unlink(missing_ok=True)
                    print(f"  [{_timestamp()}] {key:<60} removed {flow_path}")
                    del sender_flow_ids[key]

                if key.startswith("receivers/") and key in sink_processes:
                    sink_processes[key].terminate()
                    print(f"  [{_timestamp()}] {key:<60} terminated mxl-gst-sink")
                    del sink_processes[key]

            active_state[key] = master_enable

        polls_since_discovery += 1
        time.sleep(poll_interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Poll NMOS IS-05 active endpoints for state changes",
    )
    parser.add_argument(
        "--base-url",
        default=BASE_URL,
        help=f"Base Connection API URL (default: {BASE_URL})",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Polling interval in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--rediscover",
        type=int,
        default=30,
        help="Re-discover resources every N polls (default: 30)",
    )
    args = parser.parse_args()

    try:
        monitor(
            base_url=args.base_url.rstrip("/"),
            poll_interval=args.interval,
            rediscover_every=args.rediscover,
        )
    except KeyboardInterrupt:
        for key, proc in source_processes.items():
            proc.terminate()
            print(f"  [{_timestamp()}] {key:<60} terminated mxl-gst-testsrc")
        for key, proc in sink_processes.items():
            proc.terminate()
            print(f"  [{_timestamp()}] {key:<60} terminated mxl-gst-sink")
        for key, flow_id in sender_flow_ids.items():
            flow_path = MXL_DOMAIN / f"{flow_id}.json"
            flow_path.unlink(missing_ok=True)
            print(f"  [{_timestamp()}] {key:<60} removed {flow_path}")
        print(f"\n[{_timestamp()}] Stopped.")
