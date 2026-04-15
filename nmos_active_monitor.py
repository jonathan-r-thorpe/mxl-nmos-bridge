#!/usr/bin/env python3

import argparse
import json
import os
import subprocess
import tempfile
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
receiver_activation_times: dict[str, str] = {}
sender_patterns: dict[str, str] = {}
sender_looping_ts: dict[str, bool] = {}
MXL_DOMAIN = Path.home() / "mxl_domain"
# Flow descriptor JSON for mxl-gst-testsrc (not part of the MXL domain tree).
DEFAULT_MXL_FLOW_JSON_DIR = Path(tempfile.gettempdir()) / "nmos-active-monitor-mxl-flows"
MXL_GST_DIR = Path.home() / "projects/mxl/build/Linux-GCC-Debug/tools/mxl-gst"
# Built by utils/gst-looping-filesrc; required for mxl-gst-looping-filesrc (see mxl docs/Tools.md).
MXL_LOOPING_FILESRC_PLUGIN_DIR = MXL_GST_DIR.parent.parent / "utils" / "gst-looping-filesrc"
# Continuous-flow on-disk layout (mxl-internal PathUtils.hpp).
_MXL_FLOW_DIR_SUFFIX = ".mxl-flow"
_MXL_FLOW_DATA_NAME = "data"


def _mxl_flow_data_path(domain: Path, flow_id: str) -> Path:
    """Path to the MXL ``data`` file for a flow id (uuid string normalized to lowercase)."""
    return domain / f"{flow_id.lower().strip()}{_MXL_FLOW_DIR_SUFFIX}" / _MXL_FLOW_DATA_NAME


def _wait_for_mxl_flow_ready(domain: Path, flow_id: str, timeout_s: float) -> tuple[bool, Path]:
    """
    Block until ``mxl-gst-looping-filesrc`` has published the flow (GStreamer negotiates before
    ``mxlCreateFlowWriter``). Without this, ``mxl-gst-sink`` often starts first and fails with
    MXL_ERR_FLOW_NOT_FOUND (reported as \"status code 2\").
    """
    path = _mxl_flow_data_path(domain, flow_id)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.is_file():
            return True, path
        time.sleep(0.05)
    return False, path


TEST_PATTERNS = [
#    "smpte", "snow", "black", "white", "red", "green", "blue",
#    "checkers-1", "checkers-2", "checkers-4", "checkers-8",
#    "circular", "blink", "smpte75", "zone-plate", "gamut",
#    "chroma-zone-plate", "ball", "smpte100",
#    "bar", "pinwheel", "spokes", "gradient", "colors",
    "colors", "red",
]


def _subprocess_env_for_looping_filesrc() -> dict[str, str]:
    """Prepend MXL gst-looping-filesrc build dir so GStreamer loads ``looping_filesrc``."""
    env = os.environ.copy()
    pdir = MXL_LOOPING_FILESRC_PLUGIN_DIR
    if pdir.is_dir():
        prev = env.get("GST_PLUGIN_PATH", "")
        env["GST_PLUGIN_PATH"] = f"{pdir}:{prev}" if prev else str(pdir)
    return env


def _pattern_for_sender(sender_id: str) -> str:
    if sender_id not in sender_patterns:
        sender_patterns[sender_id] = TEST_PATTERNS[len(sender_patterns) % len(TEST_PATTERNS)]
    return sender_patterns[sender_id]


def _normalize_sender_resource_id(arg: str) -> str:
    """Normalize CLI sender id to ``senders/<id>`` for comparison (case-insensitive id)."""
    a = arg.strip().rstrip("/")
    if a.startswith("senders/"):
        rid = a[len("senders/") :]
    else:
        rid = a.split("/")[-1] if "/" in a else a
    return f"senders/{rid.lower()}"


def _launch_sink(key: str, flow_id: str, activation_time: str, node_url: str) -> None:
    receiver_data = fetch_json(f"{node_url}/{key}")
    format = receiver_data.get("format") if receiver_data else None
    flow_flag = "-a" if format == "urn:x-nmos:format:audio" else "-v"
    proc = subprocess.Popen(
        [str(MXL_GST_DIR / "mxl-gst-sink"),
         "-d", str(MXL_DOMAIN), flow_flag, flow_id],
    )
    sink_processes[key] = proc
    receiver_activation_times[key] = activation_time
    print(f"  [{_timestamp()}] {key:<60} launched mxl-gst-sink {flow_flag} (pid {proc.pid})")


def _terminate_sink(key: str) -> None:
    if key in sink_processes:
        sink_processes[key].terminate()
        print(f"  [{_timestamp()}] {key:<60} terminated mxl-gst-sink")
        del sink_processes[key]
    receiver_activation_times.pop(key, None)


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


def _pick_auto_looping_sender_key(node_url: str, sender_rids: list[str]) -> str | None:
    """Choose one sender for MPEG-TS looping: lowest id among video flows, else lowest sender id."""
    if not sender_rids:
        return None
    sorted_rids = sorted(sender_rids, key=str.casefold)
    video_rids: list[str] = []
    for rid in sorted_rids:
        sender = fetch_json(f"{node_url}/senders/{rid}")
        if not sender or not isinstance(sender, dict):
            continue
        flow_id = sender.get("flow_id")
        if not flow_id:
            continue
        flow = fetch_json(f"{node_url}/flows/{flow_id}")
        if flow and flow.get("format") == "urn:x-nmos:format:video":
            video_rids.append(rid)
    if video_rids:
        pick = min(video_rids, key=str.casefold)
    else:
        pick = min(sorted_rids, key=str.casefold)
    return _normalize_sender_resource_id(f"senders/{pick}")


def monitor(
    base_url: str,
    poll_interval: float = 1.0,
    rediscover_every: int = 30,
    flow_json_dir: Path | None = None,
    mpegts_file: Path | None = None,
    looping_sender_keys: frozenset[str] | None = None,
    testsrc_pattern: str | None = None,
    mxl_flow_ready_timeout_s: float = 90.0,
) -> None:
    flow_dir = flow_json_dir or DEFAULT_MXL_FLOW_JSON_DIR
    flow_dir.mkdir(parents=True, exist_ok=True)
    active_state: dict[str, bool | None] = {}
    node_url = base_url.replace("/x-nmos/connection/v1.2", "/x-nmos/node/v1.3")
    polls_since_discovery = rediscover_every  # force initial discovery
    effective_looping: frozenset[str] = frozenset()

    print(f"[{_timestamp()}] Monitoring NMOS IS-05 MXL active endpoints at {base_url}")
    print(f"  Poll interval: {poll_interval}s | Re-discovery every {rediscover_every} polls")
    if mpegts_file and looping_sender_keys:
        listed = ", ".join(sorted(looping_sender_keys))
        print(f"  MPEG-TS loop: {mpegts_file} → mxl-gst-looping-filesrc for [{listed}] (explicit)")
        print("  Other senders: mxl-gst-testsrc")
    elif mpegts_file:
        print(
            f"  MPEG-TS loop: {mpegts_file} → one video sender (auto: lowest id among video flows, "
            "else lowest sender id); others use mxl-gst-testsrc"
        )
    else:
        print("  Sender playback: mxl-gst-testsrc (patterns)")
    if testsrc_pattern:
        print(f"  Test pattern override: -p {testsrc_pattern}")
    if mpegts_file:
        print(f"  MPEG-TS: wait up to {mxl_flow_ready_timeout_s}s for MXL flow data before starting video sinks")
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
            if mpegts_file:
                if looping_sender_keys is not None:
                    effective_looping = looping_sender_keys
                else:
                    auto_key = _pick_auto_looping_sender_key(node_url, resources.get("senders", []))
                    effective_looping = frozenset({auto_key}) if auto_key else frozenset()
                    if auto_key:
                        print(f"  [{_timestamp()}] MPEG-TS looping sender (auto): {auto_key}")
            else:
                effective_looping = frozenset()
            polls_since_discovery = 0

        polled: list[tuple[str, dict, bool, bool | None]] = []
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

            polled.append((key, data, master_enable, prev))

        for key, data, master_enable, prev in polled:
            if master_enable and not prev:
                tp = data.get("transport_params") or []
                flow_id = tp[0].get("flow_id") if tp else None
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
                        if flow_data.get("format") == "urn:x-nmos:format:video":
                            flow_data.setdefault("interlace_mode", "progressive")
                        fmt = flow_data.get("format")
                        key_norm = _normalize_sender_resource_id(key)
                        if mpegts_file and fmt == "urn:x-nmos:format:video":
                            use_looping_ts = bool(effective_looping) and key_norm in effective_looping
                        else:
                            use_looping_ts = False
                        if mpegts_file and fmt == "urn:x-nmos:format:audio":
                            print(
                                f"  [{_timestamp()}] {key:<60} "
                                "mxl-gst-looping-filesrc is video-only; using mxl-gst-testsrc for audio"
                            )
                        if use_looping_ts:
                            plugin_so = MXL_LOOPING_FILESRC_PLUGIN_DIR / "liblooping_filesrc.so"
                            if not plugin_so.is_file():
                                print(
                                    f"  [{_timestamp()}] {key:<60} "
                                    f"WARNING: {plugin_so} missing; build mxl utils/gst-looping-filesrc "
                                    "(see mxl docs/Tools.md)"
                                )
                            proc = subprocess.Popen(
                                [
                                    str(MXL_GST_DIR / "mxl-gst-looping-filesrc"),
                                    "-d",
                                    str(MXL_DOMAIN),
                                    "-i",
                                    str(mpegts_file),
                                    "-f",
                                    flow_id,
                                ],
                                env=_subprocess_env_for_looping_filesrc(),
                            )
                            source_processes[key] = proc
                            sender_looping_ts[key] = True
                            print(
                                f"  [{_timestamp()}] {key:<60} "
                                f"launched mxl-gst-looping-filesrc -i {mpegts_file} -f {flow_id} (pid {proc.pid})"
                            )
                            ready, data_path = _wait_for_mxl_flow_ready(
                                MXL_DOMAIN, flow_id, mxl_flow_ready_timeout_s
                            )
                            if ready:
                                print(
                                    f"  [{_timestamp()}] {key:<60} "
                                    f"MXL flow ready ({data_path})"
                                )
                            else:
                                print(
                                    f"  [{_timestamp()}] {key:<60} "
                                    f"WARNING: timed out waiting for {data_path}; "
                                    "video sinks may fail with flow reader status 2 (flow not found)"
                                )
                        else:
                            flow_path = flow_dir / f"{flow_id}.json"
                            flow_path.write_text(json.dumps(flow_data, indent=2))
                            print(f"  [{_timestamp()}] {key:<60} wrote {flow_path}")
                            flow_flag = "-a" if fmt == "urn:x-nmos:format:audio" else "-v"
                            sender_id = key.removeprefix("senders/")
                            pattern = (
                                testsrc_pattern
                                if testsrc_pattern is not None
                                else _pattern_for_sender(sender_id)
                            )
                            proc = subprocess.Popen(
                                [
                                    str(MXL_GST_DIR / "mxl-gst-testsrc"),
                                    "-d",
                                    str(MXL_DOMAIN),
                                    flow_flag,
                                    str(flow_path),
                                    "-p",
                                    pattern,
                                ],
                            )
                            source_processes[key] = proc
                            sender_looping_ts[key] = False
                            print(
                                f"  [{_timestamp()}] {key:<60} "
                                f"launched mxl-gst-testsrc -p {pattern} (pid {proc.pid})"
                            )

        for key, data, master_enable, prev in polled:
            if master_enable and not prev and key.startswith("receivers/"):
                tp = data.get("transport_params") or []
                flow_id = tp[0].get("flow_id") if tp else None
                if flow_id:
                    activation_time = data.get("activation", {}).get("activation_time")
                    _launch_sink(key, flow_id, activation_time, node_url)

        for key, data, master_enable, prev in polled:
            if not master_enable and prev and key.startswith("receivers/"):
                _terminate_sink(key)

        for key, data, master_enable, prev in polled:
            if not master_enable and prev and key.startswith("senders/") and key in sender_flow_ids:
                if key in source_processes:
                    source_processes[key].terminate()
                    label = (
                        "mxl-gst-looping-filesrc"
                        if sender_looping_ts.get(key)
                        else "mxl-gst-testsrc"
                    )
                    print(f"  [{_timestamp()}] {key:<60} terminated {label}")
                    del source_processes[key]
                    sender_looping_ts.pop(key, None)
                flow_path = flow_dir / f"{sender_flow_ids[key]}.json"
                flow_path.unlink(missing_ok=True)
                print(f"  [{_timestamp()}] {key:<60} removed {flow_path}")
                del sender_flow_ids[key]

        for key, data, master_enable, prev in polled:
            if master_enable and prev and key.startswith("receivers/"):
                activation_time = data.get("activation", {}).get("activation_time")
                prev_activation_time = receiver_activation_times.get(key)
                if activation_time and activation_time != prev_activation_time:
                    tp = data.get("transport_params") or []
                    flow_id = tp[0].get("flow_id") if tp else None
                    print(f"  [{_timestamp()}] {key:<60} re-activated: {prev_activation_time} -> {activation_time}")
                    _terminate_sink(key)
                    if flow_id:
                        _launch_sink(key, flow_id, activation_time, node_url)

        for key, data, master_enable, prev in polled:
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
    parser.add_argument(
        "--flow-json-dir",
        type=Path,
        default=None,
        help=(
            "Directory for temporary flow JSON files passed to mxl-gst-testsrc "
            f"(default: {DEFAULT_MXL_FLOW_JSON_DIR})"
        ),
    )
    parser.add_argument(
        "--mpegts-file",
        type=Path,
        default=None,
        help=(
            "MPEG-TS file for mxl-gst-looping-filesrc. By default exactly one video sender "
            "is chosen automatically (see startup logs after each discovery). Use "
            "--looping-sender to pin the file to specific senders instead. "
            "GST_PLUGIN_PATH is prepended with MXL_LOOPING_FILESRC_PLUGIN_DIR (same CMake build "
            "tree as MXL_GST_DIR) so GStreamer can load looping_filesrc."
        ),
    )
    parser.add_argument(
        "--mxl-flow-ready-timeout",
        type=float,
        default=90.0,
        metavar="SEC",
        help=(
            "With --mpegts-file, seconds to wait for <domain>/<flow-uuid>.mxl-flow/data after "
            "starting mxl-gst-looping-filesrc before launching video sinks (default: 90). "
            "Avoids MXL_ERR_FLOW_NOT_FOUND when the sink starts before GStreamer negotiation finishes."
        ),
    )
    parser.add_argument(
        "--looping-sender",
        action="append",
        default=None,
        metavar="SENDER",
        help=(
            "Optional override: sender id (UUID or senders/<uuid>) that should play "
            "--mpegts-file; repeat for multiple. Requires --mpegts-file. If omitted, the "
            "monitor picks one video sender automatically."
        ),
    )
    parser.add_argument(
        "--testsrc-pattern",
        default=None,
        metavar="PATTERN",
        help=(
            "If set, passed as -p to every mxl-gst-testsrc (e.g. smpte for color bars). "
            "If omitted, patterns rotate via the built-in list."
        ),
    )
    args = parser.parse_args()
    if args.mpegts_file and not args.mpegts_file.is_file():
        parser.error(f"--mpegts-file is not a readable file: {args.mpegts_file}")
    if args.looping_sender and not args.mpegts_file:
        parser.error("--looping-sender requires --mpegts-file")
    looping_keys: frozenset[str] | None = None
    if args.looping_sender:
        looping_keys = frozenset(
            _normalize_sender_resource_id(s)
            for s in args.looping_sender
            if s.strip()
        )
        if not looping_keys:
            parser.error("--looping-sender requires a non-empty sender id")

    try:
        monitor(
            base_url=args.base_url.rstrip("/"),
            poll_interval=args.interval,
            rediscover_every=args.rediscover,
            flow_json_dir=args.flow_json_dir,
            mpegts_file=args.mpegts_file,
            looping_sender_keys=looping_keys,
            testsrc_pattern=args.testsrc_pattern,
            mxl_flow_ready_timeout_s=args.mxl_flow_ready_timeout,
        )
    except KeyboardInterrupt:
        for key, proc in source_processes.items():
            proc.terminate()
            label = (
                "mxl-gst-looping-filesrc"
                if sender_looping_ts.get(key)
                else "mxl-gst-testsrc"
            )
            print(f"  [{_timestamp()}] {key:<60} terminated {label}")
        for key, proc in sink_processes.items():
            proc.terminate()
            print(f"  [{_timestamp()}] {key:<60} terminated mxl-gst-sink")
        for key, flow_id in sender_flow_ids.items():
            flow_path = (args.flow_json_dir or DEFAULT_MXL_FLOW_JSON_DIR) / f"{flow_id}.json"
            flow_path.unlink(missing_ok=True)
            print(f"  [{_timestamp()}] {key:<60} removed {flow_path}")
        print(f"\n[{_timestamp()}] Stopped.")
