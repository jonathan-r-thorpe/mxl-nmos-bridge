# MXL NMOS Bridge

Python helper that watches **NMOS IS-05** Connection API **active** endpoints for **MXL transport** Senders and Receivers. When a Sender or Receiver becomes active, it launches the matching **mxl-gst** tool from the DMF MXL SDK (`mxl-gst-testsrc` or `mxl-gst-sink`). When it goes inactive, it stops the process and cleans up temporary flow files for Senders.

Use this when you want local MXL GStreamer processes to track live NMOS connection state without wiring everything by hand.

## Prerequisites

- **Python 3** with [`requests`](https://pypi.org/project/requests/) (see `requirements.txt`).
- An **NMOS Node** reachable over HTTP, exposing at least:
  - IS-05 v1.2 Connection API (v1.2 is the latest draft specification of IS-05).
  - IS-04 v1.3 Node API.
- **MXL domain** on disk (default root: `/dev/shm/mxl`). The MXL domain directory **must** contain a `domain_def.json` file that conforms to [**AMWA BCP-007-03** (*NMOS With MXL*)](https://specs.amwa.tv/bcp-007-03/branches/publish-mxl-domain-mapping/docs/NMOS-With-MXL.html) — including the domain identity and metadata described there (for example `id`, `label`, and optional `description`). This monitor checks that the `id` in `domain_def.json` matches `mxl_domain_id` in IS-05 transport parameters for the endpoints you activate.
- **DMF MXL SDK** — Build the [MXL SDK](https://github.com/dmf-mxl/mxl), including the example **GStreamer** tools (`mxl-gst-testsrc`, `mxl-gst-sink`; typically under `tools/mxl-gst` in your build tree—see that repository’s build and tools documentation). Point `--mxl-gst-dir` at the directory that contains those binaries (default: `~/projects/mxl/build/Linux-GCC-Debug/tools/mxl-gst`):
  - `mxl-gst-testsrc` — started for active **Senders**
  - `mxl-gst-sink` — started for active **Receivers**

Only endpoints whose transport type is `urn:x-nmos:transport:mxl` are discovered and monitored.

## Install

```bash
cd /path/to/nmos-active-monitor
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Run

```bash
python nmos_active_monitor.py
```

The script runs until you stop it (**Ctrl+C**). On exit it terminates any launched `mxl-gst-testsrc` / `mxl-gst-sink` processes and removes Sender flow JSON files it created.

### Command-line options

| Option | Description |
|--------|-------------|
| `--config PATH` | Load defaults from a JSON file (may appear before or after other flags). CLI arguments override the file. |
| `--base-url URL` | Connection API base (default: `http://localhost:7000/x-nmos/connection/v1.2`). Trailing slashes are stripped. |
| `--interval SECONDS` | Poll interval (default: `1.0`). |
| `--rediscover N` | Re-run discovery every *N* polls (default: `30`). |
| `--flow-json-dir DIR` | Where Sender flow descriptor JSON files are written for `mxl-gst-testsrc` (default: a directory under the system temp dir: `nmos-active-monitor-mxl-flows`). |
| `--mxl-domain PATH` | MXL domain root passed to `mxl-gst-* -d` (default: `/dev/shm/mxl`). |
| `--mxl-gst-dir PATH` | Directory containing `mxl-gst-testsrc` and `mxl-gst-sink` (see defaults above). |

Show built-in help:

```bash
python nmos_active_monitor.py --help
```

### Config file

Optional JSON object. Only these keys are accepted (unknown keys cause an error):

| Key | Type | Meaning |
|-----|------|---------|
| `base_url` | string | Same as `--base-url`. |
| `interval` | number | Same as `--interval`. |
| `rediscover` | integer | Same as `--rediscover`. |
| `flow_json_dir` | string or `null` | Same as `--flow-json-dir`; `null` uses the default temp directory. |
| `mxl_domain` | string | Same as `--mxl-domain`. |
| `mxl_gst_dir` | string | Same as `--mxl-gst-dir`. |

Example `monitor.json`:

```json
{
  "base_url": "http://localhost:7000/x-nmos/connection/v1.2",
  "interval": 1.0,
  "rediscover": 30,
  "mxl_domain": "/dev/shm/mxl",
  "mxl_gst_dir": "/home/you/projects/mxl/build/Linux-GCC-Debug/tools/mxl-gst"
}
```

```bash
python nmos_active_monitor.py --config monitor.json --interval 0.5
```

## What it does at runtime

1. **Discovery** — Periodically lists Senders and Receivers, keeps only those with MXL transports.
2. **Polling** — For each discovered ID, fetches `.../single/{senders\|receivers}/{id}/active` and compares `master_enable` to the previous poll.
3. **Senders become active** — Reads `mxl_flow_id` from transport params, validates `mxl_domain_id` against `domain_def.json`, fetches the Flow (and related tags/channel info) from the Node API, writes `<flow_id>.json` under `--flow-json-dir`, then runs `mxl-gst-testsrc` with that file and a rotating test pattern.
4. **Receivers become active** — Validates the MXL domain, then runs `mxl-gst-sink` with the flow id and audio/video flag from the Receiver’s `format`.
5. **Receivers re-activate** — If `activation_time` on an already-active Receiver changes, the sink is restarted with the new activation.
6. **Inactive** — Terminates the corresponding GStreamer process; for Senders, deletes the flow JSON file it wrote.

Logs are printed to the terminal with timestamps.

## Related files in this repo

- `requirements.txt` — Python dependency pin for `requests`.

Adjust paths in `--mxl-domain` and `--mxl-gst-dir` (or the config file) to match your machine and MXL build layout.
