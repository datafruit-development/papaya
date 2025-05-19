#!/usr/bin/env python3
# papaya.py ---------------------------------------------------------------
"""
Papaya – a lightweight Spark failure monitor.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import socket
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv

from analyze_failure import analyze_failure
from discord_utils  import send_discord_message, start_discord_bot
from code_repair    import repair_code

load_dotenv()


# -------------------------------------------------------------------------
# Spark helpers
# -------------------------------------------------------------------------
def _port_open(host: str, port: int, timeout: float = 0.2) -> bool:
    """Return True if the TCP port is accepting connections."""
    try:
        with socket.create_connection((host, port), timeout):
            return True
    except OSError:
        return False


def _spark_get(url: str, timeout: float = 1.0) -> Optional[Any]:
    try:
        resp = requests.get(url, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
    except Exception as exc:  # noqa: BLE001
        print("Spark API request failed: %s", exc)
    return None


def list_applications(spark_ui: str) -> List[Dict[str, Any]]:
    """GET /api/v1/applications"""
    return _spark_get(urljoin(spark_ui, "/api/v1/applications")) or []

# -------------------------------------------------------------------------
# Getting Spark app information
# -------------------------------------------------------------------------
def get_complete_app_state(app_id: str, spark_ui: str) -> Dict[str, Any]:
    """Return a huge dict ± identical to the one produced by your server."""
    state: Dict[str, Any] = {"app_id": app_id, "timestamp": time.time()}

    def push(path: str, key: str) -> None:
        data = _spark_get(urljoin(spark_ui, f"/api/v1/applications/{app_id}{path}"))
        if data is not None:
            state[key] = data

    # --- basic information
    push("", "app_info")
    push("/jobs", "jobs")
    push("/stages", "stages")
    push("/executors", "executors")
    push("/environment", "environment")

    # ------------------------------------------------------------------
    # Enhanced failure detection (same logic as before, condensed)
    # ------------------------------------------------------------------
    state["has_failed_jobs"] = False
    state["failed_jobs_details"] = []
    if "jobs" in state:
        failed = [j for j in state["jobs"] if j.get("status") in ("FAILED", "ERROR")]
        state["has_failed_jobs"] = bool(failed)
        state["failed_jobs_details"] = [
            {
                "job_id": j["jobId"],
                "name": j["name"],
                "status": j["status"],
                "failure_reason": j.get("failureReason"),
            }
            for j in failed
        ]

    state["has_failed_stages"] = False
    state["failed_stages_details"] = []
    if "stages" in state:
        failed = [s for s in state["stages"] if s.get("status") == "FAILED"]
        state["has_failed_stages"] = bool(failed)
        for s in failed:
            stage_id, attempt = s["stageId"], s.get("attemptId", 0)
            detail = _spark_get(
                urljoin(
                    spark_ui,
                    f"/api/v1/applications/{app_id}/stages/{stage_id}/{attempt}",
                )
            ) or {}
            state["failed_stages_details"].append(
                {
                    "stage_id": stage_id,
                    "attempt_id": attempt,
                    "name": s["name"],
                    "status": "FAILED",
                    "failure_reason": detail.get("failureReason")
                    or detail.get("exception", "Unknown"),
                }
            )

    # Task failures (expensive – fetch for *every* stage)
    state["task_failure_count"] = 0
    state["task_failures"] = []
    if "stages" in state:
        for s in state["stages"]:
            stage_id, attempt = s["stageId"], s.get("attemptId", 0)
            tasks = _spark_get(
                urljoin(
                    spark_ui,
                    f"/api/v1/applications/{app_id}/stages/{stage_id}/{attempt}/taskList",
                )
            ) or []
            failed = [
                t
                for t in tasks
                if t.get("status") == "FAILED"
                or t.get("errorMessage")
                or t.get("hasException")
            ]
            state["task_failure_count"] += len(failed)
            state["task_failures"].extend(
                {
                    "stage_id": stage_id,
                    "task_id": t["taskId"],
                    "attempt": t["attempt"],
                    "status": t["status"],
                    "error_message": t.get("errorMessage", ""),
                    "executor_id": t.get("executorId"),
                }
                for t in failed
            )

    # Executor failures
    state["executor_failure_count"] = sum(
        e.get("failedTasks", 0) for e in state.get("executors", [])
    )
    state["has_executor_failures"] = state["executor_failure_count"] > 0

    # Application finished?
    if "app_info" in state and state["app_info"].get("attempts"):
        att = state["app_info"]["attempts"][0]
        state["is_completed"] = att.get("completed", False)
    else:
        state["is_completed"] = False

    # Aggregate
    state["has_failures"] = any(
        (
            state["has_failed_jobs"],
            state["has_failed_stages"],
            state["task_failure_count"] > 0,
            state["has_executor_failures"],
        )
    )

    return state


# -------------------------------------------------------------------------
# State hashing / change-detection
# -------------------------------------------------------------------------
def _hash_state(state: Dict[str, Any]) -> str:
    parts: List[str] = []

    for job in state.get("jobs", []):
        parts.append(f"J{job['jobId']}:{job['status']}")

    for stg in state.get("stages", []):
        parts.append(f"S{stg['stageId']}:{stg['status']}")

    parts.extend(
        [
            f"task_fail:{state.get('task_failure_count', 0)}",
            f"exec_fail:{state.get('executor_failure_count', 0)}",
            f"completed:{state.get('is_completed', False)}",
            f"has_failures:{state.get('has_failures', False)}",
        ]
    )
    return hashlib.md5("|".join(parts).encode()).hexdigest()


# -------------------------------------------------------------------------
# Human-readable log
# -------------------------------------------------------------------------
def _build_log(state: Dict[str, Any]) -> str:
    ts = datetime.fromtimestamp(state["timestamp"]).isoformat()
    logs = [
        "▲ Spark job failure detected",
        f"App ID:   {state['app_id']}",
        f"App Name: {state.get('app_info', {}).get('name', 'Unknown')}",
        f"Time:     {ts}",
    ]

    if state["has_failed_jobs"]:
        logs.append("\nJob Failures:")
        for j in state["failed_jobs_details"]:
            logs.append(
                f"  • Job {j['job_id']} — {j['name']} — {j['status']} — "
                f"{j.get('failure_reason') or 'N/A'}"
            )

    if state["has_failed_stages"]:
        logs.append("\nStage Failures:")
        for s in state["failed_stages_details"]:
            logs.append(
                f"  • Stage {s['stage_id']}.{s['attempt_id']} — {s['name']} "
                f"— {s['status']} — {s['failure_reason']}"
            )

    if state["task_failure_count"]:
        logs.append(f"\nTask Failures ({state['task_failure_count']}):")
        for t in state["task_failures"]:
            first_line = (t["error_message"] or "").split("\n", 1)[0]
            logs.append(
                f"  • Task {t['stage_id']}.{t['task_id']}.{t['attempt']} on "
                f"{t['executor_id']} — {t['status']} — {first_line or 'N/A'}"
            )

    if state["has_executor_failures"]:
        logs.append(f"\nExecutor failures: {state['executor_failure_count']}")

    return "\n".join(logs)


# -------------------------------------------------------------------------
# Main monitoring loop
# -------------------------------------------------------------------------
def monitor_loop(
    spark_ui: str,
    polling: float,
    discord_cid: Optional[str],
    github_repo: Optional[str],
) -> None:
    """Continuous poll → analyse → notify loop."""
    # Pre-flight – make sure the Spark UI is reachable
    host = spark_ui.split("://", 1)[-1].split(":", 1)[0]
    port = int(spark_ui.rsplit(":", 1)[-1].split("/", 1)[0])
    if not _port_open(host, port):
        raise SystemExit(f"Cannot connect to Spark UI at {spark_ui}")

    if discord_cid:
        start_discord_bot()

    tracked: Dict[str, Dict[str, Any]] = {}  # app_id -> last_state

    print("Started monitoring %s (interval %.2fs)", spark_ui, polling)
    while True:
        try:
            # ------------------------------------------------------------------
            for app in list_applications(spark_ui):
                app_id = app["id"]

                # Snapshot + hash
                state = get_complete_app_state(app_id, spark_ui)
                hsh = _hash_state(state)
                old_hsh = tracked.get(app_id, {}).get("hash")

                # Save latest
                tracked[app_id] = {"hash": hsh, "state": state}

                # Only act on *new* failures / changes
                if hsh == old_hsh:
                    continue

                if state["has_failures"]:
                    log_txt = _build_log(state)

                    # ---------------- analyze / notify / repair ----------
                    report = analyze_failure(
                        log_txt,
                        # The three args your original fn expected
                        os.getenv("GITHUB_REPO_OWNER"),
                        os.getenv("GITHUB_REPO_URL"),
                        os.getenv("PG_DB_URL"),
                        state["app_id"],
                    )

                    if discord_cid:
                        send_discord_message(report, discord_cid)

                    if github_repo:
                        pr_url = repair_code(
                            github_repo.split("/")[0], github_repo, report
                        )
                        if pr_url and discord_cid:
                            send_discord_message(
                                f"🛠️  Code repair PR created: {pr_url}", discord_cid
                            )

            # ------------------------------------------------------------------
        except KeyboardInterrupt:
            print("Stopped by user")
            break
        except Exception as exc:  # noqa: BLE001
            print("Monitor loop error: %s", exc)

        time.sleep(polling)


# -------------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="papaya",
        description=(
            "Monitor a Spark application and automatically analyse failures, "
            "post reports to Discord, and open code-repair PRs."
        ),
    )
    p.add_argument(
        "url",
        metavar="SPARK_UI_URL",
        help="The SparkUI base URL (e.g. http://localhost:4040)",
    )
    p.add_argument(
        "--discord-cid",
        metavar="CHANNEL_ID",
        type=str,
        help="Discord channel ID to post failure reports. "
        "Requires DISCORD_TOKEN in the environment.",
    )
    p.add_argument(
        "--github-repo",
        metavar="OWNER/REPO",
        type=str,
        help="GitHub repository slug to open automatic PRs. "
        "Requires GH_APP_TOKEN in the environment.",
    )
    p.add_argument(
        "--poll",
        metavar="SECONDS",
        type=float,
        default=0.5,
        help="Polling interval in seconds (default: 0.5)",
    )
    return p


def main(argv: Optional[List[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.discord_cid and "DISCORD_TOKEN" not in os.environ:
        parser.error("--discord-cid requires DISCORD_TOKEN to be set")
    if args.github_repo and "GH_APP_TOKEN" not in os.environ:
        parser.error("--github-repo requires GH_APP_TOKEN to be set")

    print("Initializing monitor for %s", args.url)
    if args.discord_cid:
        print("→ will notify Discord channel %s", args.discord_cid)
    if args.github_repo:
        print("→ will link to GitHub repo %s", args.github_repo)

    monitor_loop(
        spark_ui=args.url,
        polling=args.poll,
        discord_cid=args.discord_cid,
        github_repo=args.github_repo,
    )


if __name__ == "__main__":
    main()
