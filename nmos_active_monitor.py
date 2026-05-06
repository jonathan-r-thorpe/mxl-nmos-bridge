#!/usr/bin/env python3

import argparse
import json
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
MXL_DOMAIN = Path.home() / "mxl_domain"
MXL_DOMAIN_DEF_NAME = "domain_def.json"
# Flow descriptor JSON for mxl-gst-testsrc (not part of the MXL domain tree).
DEFAULT_MXL_FLOW_JSON_DIR = Path(tempfile.gettempdir()) / "nmos-active-monitor-mxl-flows"
MXL_GST_DIR = Path.home() / "projects/mxl/build/Linux-GCC-Debug/tools/mxl-gst"
TEST_PATTERNS = [
#    "smpte", "snow", "black", "white", "red", "green", "blue",
#    "checkers-1", "checkers-2", "checkers-4", "checkers-8",
#    "circular", "blink", "smpte75", "zone-plate", "gamut",
#    "chroma-zone-plate", "ball", "smpte100",
#    "bar", "pinwheel", "spokes", "gradient", "colors",
    "colors", "red",
]


def _transport_params_first(data: dict) -> dict:
    tp = data.get("transport_params") or []
    return tp[0] if tp else {}


def _mxl_flow_id_from_tp(tp0: dict) -> str | None:
    """IS-05 MXL transport uses ``mxl_flow_id`` (legacy: ``flow_id``)."""
    v = tp0.get("mxl_flow_id") or tp0.get("flow_id")
    return str(v).strip() if v else None


def _normalize_mxl_domain_id(s: str) -> str:
    return str(s).strip().casefold()


def _read_domain_def_id(domain_root: Path) -> str | None:
    """Return the ``id`` from ``domain_def.json``, or None if missing or unreadable."""
    path = domain_root / MXL_DOMAIN_DEF_NAME
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None
    vid = raw.get("id") if isinstance(raw, dict) else None
    return str(vid).strip() if vid else None


def _validate_mxl_domain(
    domain_root: Path, mxl_domain_id: str | None
) -> tuple[bool, str]:
    """
    Ensure ``mxl_domain_id`` matches the ``id`` in pre-configured ``domain_def.json``
    at ``domain_root``. Returns (True, "") if valid, else (False, reason).
    """
    if mxl_domain_id is None or not str(mxl_domain_id).strip():
        return False, "transport_params missing mxl_domain_id"
    req = _normalize_mxl_domain_id(str(mxl_domain_id))
    disk_id = _read_domain_def_id(domain_root)
    def_path = domain_root / MXL_DOMAIN_DEF_NAME
    if disk_id is None:
        return (
            False,
            f"cannot read domain id from {def_path} (missing, invalid JSON, or no id field)",
        )
    if _normalize_mxl_domain_id(disk_id) != req:
        return (
            False,
            f"mxl_domain_id {mxl_domain_id!r} does not match domain_def.json id {disk_id!r} "
            "(different MXL domain; out of scope)",
        )
    return True, ""


def _pattern_for_sender(sender_id: str) -> str:
    if sender_id not in sender_patterns:
        sender_patterns[sender_id] = TEST_PATTERNS[len(sender_patterns) % len(TEST_PATTERNS)]
    return sender_patterns[sender_id]


def _launch_sink(
    key: str,
    flow_id: str,
    activation_time: str,
    node_url: str,
    mxl_domain_id: str | None,
    mxl_domain: Path,
    mxl_gst_dir: Path,
) -> bool:
    ok, err = _validate_mxl_domain(mxl_domain, mxl_domain_id)
    if not ok:
        print(
            f"  [{_timestamp()}] {key:<60} ERROR: MXL receiver domain validation failed: {err}"
        )
        return False
    receiver_data = fetch_json(f"{node_url}/{key}")
    format = receiver_data.get("format") if receiver_data else None
    flow_flag = "-a" if format == "urn:x-nmos:format:audio" else "-v"
    proc = subprocess.Popen(
        [str(mxl_gst_dir / "mxl-gst-sink"),
         "-d", str(mxl_domain), flow_flag, flow_id],
    )
    sink_processes[key] = proc
    receiver_activation_times[key] = activation_time
    print(f"  [{_timestamp()}] {key:<60} launched mxl-gst-sink {flow_flag} (pid {proc.pid})")
    return True


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


def monitor(
    base_url: str,
    poll_interval: float = 1.0,
    rediscover_every: int = 30,
    flow_json_dir: Path | None = None,
    mxl_domain: Path = MXL_DOMAIN,
    mxl_gst_dir: Path = MXL_GST_DIR,
) -> None:
    flow_dir = flow_json_dir or DEFAULT_MXL_FLOW_JSON_DIR
    flow_dir.mkdir(parents=True, exist_ok=True)
    active_state: dict[str, bool | None] = {}
    node_url = base_url.replace("/x-nmos/connection/v1.2", "/x-nmos/node/v1.3")
    polls_since_discovery = rediscover_every  # force initial discovery

    print(f"[{_timestamp()}] Monitoring NMOS IS-05 MXL active endpoints at {base_url}")
    print(f"  Poll interval: {poll_interval}s | Re-discovery every {rediscover_every} polls")
    print(f"  MXL domain: {mxl_domain} | mxl-gst: {mxl_gst_dir}")
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
                tp0 = _transport_params_first(data)
                flow_id = _mxl_flow_id_from_tp(tp0)
                print(f"  [{_timestamp()}] {key:<60} mxl_flow_id: {flow_id}")

                if key.startswith("senders/") and flow_id:
                    ok_domain, err_domain = _validate_mxl_domain(
                        mxl_domain, tp0.get("mxl_domain_id")
                    )
                    if not ok_domain:
                        print(
                            f"  [{_timestamp()}] {key:<60} "
                            f"ERROR: MXL sender domain validation failed: {err_domain}"
                        )
                    else:
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
                            flow_path = flow_dir / f"{flow_id}.json"
                            flow_path.write_text(json.dumps(flow_data, indent=2))
                            print(f"  [{_timestamp()}] {key:<60} wrote {flow_path}")
                            flow_flag = "-a" if flow_data.get("format") == "urn:x-nmos:format:audio" else "-v"
                            sender_id = key.removeprefix("senders/")
                            pattern = _pattern_for_sender(sender_id)
                            proc = subprocess.Popen(
                                [str(mxl_gst_dir / "mxl-gst-testsrc"),
                                 "-d", str(mxl_domain), flow_flag, str(flow_path),
                                 "-p", pattern],
                            )
                            source_processes[key] = proc
                            print(f"  [{_timestamp()}] {key:<60} launched mxl-gst-testsrc -p {pattern} (pid {proc.pid})")

                if key.startswith("receivers/") and flow_id:
                    activation_time = data.get("activation", {}).get("activation_time")
                    _launch_sink(
                        key,
                        flow_id,
                        activation_time,
                        node_url,
                        tp0.get("mxl_domain_id"),
                        mxl_domain,
                        mxl_gst_dir,
                    )

            if not master_enable and prev:
                if key.startswith("senders/") and key in sender_flow_ids:
                    if key in source_processes:
                        source_processes[key].terminate()
                        print(f"  [{_timestamp()}] {key:<60} terminated mxl-gst-testsrc")
                        del source_processes[key]
                    flow_path = flow_dir / f"{sender_flow_ids[key]}.json"
                    flow_path.unlink(missing_ok=True)
                    print(f"  [{_timestamp()}] {key:<60} removed {flow_path}")
                    del sender_flow_ids[key]

                if key.startswith("receivers/"):
                    _terminate_sink(key)

            if master_enable and prev and key.startswith("receivers/"):
                activation_time = data.get("activation", {}).get("activation_time")
                prev_activation_time = receiver_activation_times.get(key)
                if activation_time and activation_time != prev_activation_time:
                    tp0 = _transport_params_first(data)
                    flow_id = _mxl_flow_id_from_tp(tp0)
                    print(f"  [{_timestamp()}] {key:<60} re-activated: {prev_activation_time} -> {activation_time}")
                    _terminate_sink(key)
                    if flow_id:
                        _launch_sink(
                            key,
                            flow_id,
                            activation_time,
                            node_url,
                            tp0.get("mxl_domain_id"),
                            mxl_domain,
                            mxl_gst_dir,
                        )

            active_state[key] = master_enable

        polls_since_discovery += 1
        time.sleep(poll_interval)


CONFIG_KEYS = frozenset(
    {
        "base_url",
        "interval",
        "rediscover",
        "flow_json_dir",
        "mxl_domain",
        "mxl_gst_dir",
    }
)


def load_config_file(path: Path) -> dict[str, object]:
    """
    Load JSON object from a local path. Unknown keys are rejected.
    Returns validated fields present in the file (subset of CONFIG_KEYS).
    """
    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SystemExit(f"Cannot read config file {path}: {exc}") from exc
    try:
        raw = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise SystemExit(
            f"Config root must be a JSON object, got {type(raw).__name__}"
        )
    unknown = set(raw) - CONFIG_KEYS
    if unknown:
        raise SystemExit(
            "Unknown config keys (strict): " + ", ".join(sorted(unknown))
        )

    out: dict[str, object] = {}
    if "base_url" in raw:
        v = raw["base_url"]
        if not isinstance(v, str) or not v.strip():
            raise SystemExit("Config key base_url must be a non-empty string")
        out["base_url"] = v.strip()
    if "interval" in raw:
        v = raw["interval"]
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            raise SystemExit("Config key interval must be a number")
        out["interval"] = float(v)
    if "rediscover" in raw:
        v = raw["rediscover"]
        if isinstance(v, bool) or not isinstance(v, int):
            raise SystemExit("Config key rediscover must be an integer")
        out["rediscover"] = int(v)
    if "flow_json_dir" in raw:
        v = raw["flow_json_dir"]
        if v is None:
            out["flow_json_dir"] = None
        elif isinstance(v, str):
            out["flow_json_dir"] = Path(v).expanduser()
        else:
            raise SystemExit(
                "Config key flow_json_dir must be a string path or null"
            )
    if "mxl_domain" in raw:
        v = raw["mxl_domain"]
        if not isinstance(v, str) or not v.strip():
            raise SystemExit("Config key mxl_domain must be a non-empty string")
        out["mxl_domain"] = Path(v).expanduser()
    if "mxl_gst_dir" in raw:
        v = raw["mxl_gst_dir"]
        if not isinstance(v, str) or not v.strip():
            raise SystemExit("Config key mxl_gst_dir must be a non-empty string")
        out["mxl_gst_dir"] = Path(v).expanduser()
    return out


def _build_arg_parser(defaults: dict[str, object]) -> argparse.ArgumentParser:
    d = defaults
    parser = argparse.ArgumentParser(
        description=(
            "Poll NMOS IS-05 active endpoints for state changes. "
            "Optional --config PATH loads defaults from a local JSON file; "
            "CLI flags override the file."
        ),
        epilog=(
            "Strict JSON object keys (snake_case): "
            + ", ".join(sorted(CONFIG_KEYS))
            + ". Use --config PATH (local file) on the command line; it may appear "
            "before or after other flags."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--base-url",
        dest="base_url",
        default=d["base_url"],
        help=f"Base Connection API URL (default: {d['base_url']!r})",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=d["interval"],
        help=f"Polling interval in seconds (default: {d['interval']})",
    )
    parser.add_argument(
        "--rediscover",
        type=int,
        default=d["rediscover"],
        help=f"Re-discover resources every N polls (default: {d['rediscover']})",
    )
    parser.add_argument(
        "--flow-json-dir",
        type=Path,
        default=d["flow_json_dir"],
        help=(
            "Directory for temporary flow JSON files passed to mxl-gst-testsrc "
            f"(default: {d['flow_json_dir']!r})"
        ),
    )
    parser.add_argument(
        "--mxl-domain",
        dest="mxl_domain",
        type=Path,
        default=d["mxl_domain"],
        help=(
            "MXL domain root (domain_def.json); passed to mxl-gst -d "
            f"(default: {d['mxl_domain']!r})"
        ),
    )
    parser.add_argument(
        "--mxl-gst-dir",
        dest="mxl_gst_dir",
        type=Path,
        default=d["mxl_gst_dir"],
        help=(
            "Directory containing mxl-gst-testsrc and mxl-gst-sink "
            f"(default: {d['mxl_gst_dir']!r})"
        ),
    )
    return parser


if __name__ == "__main__":
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--config", type=Path, default=None)
    pre_args, argv_rest = pre.parse_known_args()
    file_defaults: dict[str, object] = {}
    if pre_args.config is not None:
        file_defaults = load_config_file(pre_args.config.expanduser())

    merged_defaults: dict[str, object] = {
        "base_url": file_defaults.get("base_url", BASE_URL),
        "interval": file_defaults.get("interval", 1.0),
        "rediscover": file_defaults.get("rediscover", 30),
        "flow_json_dir": file_defaults.get("flow_json_dir"),
        "mxl_domain": file_defaults.get("mxl_domain", MXL_DOMAIN),
        "mxl_gst_dir": file_defaults.get("mxl_gst_dir", MXL_GST_DIR),
    }
    parser = _build_arg_parser(merged_defaults)
    args = parser.parse_args(argv_rest)

    try:
        monitor(
            base_url=args.base_url.rstrip("/"),
            poll_interval=args.interval,
            rediscover_every=args.rediscover,
            flow_json_dir=args.flow_json_dir,
            mxl_domain=args.mxl_domain.expanduser(),
            mxl_gst_dir=args.mxl_gst_dir.expanduser(),
        )
    except KeyboardInterrupt:
        for key, proc in source_processes.items():
            proc.terminate()
            print(f"  [{_timestamp()}] {key:<60} terminated mxl-gst-testsrc")
        for key, proc in sink_processes.items():
            proc.terminate()
            print(f"  [{_timestamp()}] {key:<60} terminated mxl-gst-sink")
        for key, flow_id in sender_flow_ids.items():
            flow_path = (args.flow_json_dir or DEFAULT_MXL_FLOW_JSON_DIR) / f"{flow_id}.json"
            flow_path.unlink(missing_ok=True)
            print(f"  [{_timestamp()}] {key:<60} removed {flow_path}")
        print(f"\n[{_timestamp()}] Stopped.")
