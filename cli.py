#!/usr/bin/env python3
import argparse
import os
import sys
from monitor import monitor_loop
from typing import Optional, List

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="papaya",
        description=(
            "Listen to an active Spark job’s UI and automatically debug failures; "
            "optionally post results to Discord and link to GitHub."
        ),
        epilog=(
            "EXAMPLES:\n"
            "  papaya http://localhost:4040 \n"
            "  DISCORD_TOKEN=... papaya http://... --discord-cid 123456789 \n"
            "  GH_APP_TOKEN=... papaya http://... --github-repo myorg/myrepo"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # required positional
    parser.add_argument(
        "url",
        metavar="SPARK_UI_URL",
        help="Spark UI URL of the active job to monitor"
    )

    # optional extras
    parser.add_argument(
        "--discord-cid",
        type=int,
        help="Discord channel ID to send messages to (requires DISCORD_TOKEN env var)"
    )
    parser.add_argument(
        "--github-repo",
        metavar="OWNER/REPO",
        help="GitHub repository of the Spark job (requires GH_APP_TOKEN env var)"
    )
    parser.add_argument(
        "--poll",
        metavar="SECONDS",
        type=float,
        default=0.5,
        help="Polling interval in seconds (default: 0.5)",
    )

    return parser

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
